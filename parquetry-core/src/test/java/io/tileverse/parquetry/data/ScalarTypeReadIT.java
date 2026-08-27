/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalTime;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.RowGroupOutcome;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * End-to-end read, filter, and STATS-prune coverage for decimal and time scalar types, exercised on a project-authored
 * fixture ({@code parquetry/scalar_types.parquet}).
 *
 * <p>The fixture has four columns and four rows:
 *
 * <ul>
 *   <li>{@code dec_small} - FIXED_LEN_BYTE_ARRAY(4), Decimal(9, 2): -3.00, 1.50, 4.00, 5.00
 *   <li>{@code dec_big} - FIXED_LEN_BYTE_ARRAY(11), Decimal(25, 2): same values
 *   <li>{@code t_micros} - INT64, Time(MICROS, not UTC-adjusted): 00:00, 01:00, 02:00, 03:00
 *   <li>{@code fx} - FIXED_LEN_BYTE_ARRAY(4), no logical type: 0x00000001 .. 0x00000004
 * </ul>
 *
 * <p>The decimal tests cover three distinct paths in the vectorized evaluator:
 *
 * <ol>
 *   <li>Fast path (FLBA at most 8 bytes, scale matches, query fits in a signed long) - exercises {@code dec_small}.
 *   <li>Out-of-long-range guard (scale matches and FLBA is small but the query's unscaled value exceeds a signed long's
 *       range) - falls back to the exact BigDecimal comparison with the same {@code dec_small} column.
 *   <li>BigInteger fallback (FLBA wider than 8 bytes) - exercises {@code dec_big}.
 * </ol>
 *
 * <p>The signed-ordering test proves that the comparison uses two's-complement SIGNED order, not unsigned byte order:
 * -3.00 is the smallest value in the file, and an unsigned byte comparison would misplace it at the top.
 */
class ScalarTypeReadIT {

    private static final String FIXTURE = "parquetry/scalar_types.parquet";

    private static final ColumnPath DEC_SMALL = ColumnPath.of("dec_small");
    private static final ColumnPath DEC_BIG = ColumnPath.of("dec_big");
    private static final ColumnPath T_MICROS = ColumnPath.of("t_micros");
    private static final ColumnPath FX = ColumnPath.of("fx");

    @TempDir
    Path tempDir;

    // --- Decimal fast path (dec_small = FLBA(4), scale 2) ---
    // Values: -3.00, 1.50, 4.00, 5.00

    @Test
    void decimalFastPathGtFourPointZeroReturnsOnlyFive() {
        // Only 5.00 > 4.00. The fast long-compare path handles the matching scale, small FLBA width,
        // and query unscaled value that fits in a signed long.
        Path file = TestCorpus.extractFile(FIXTURE, tempDir);
        Predicate gt4 = new Predicate.Gt(DEC_SMALL, new Value.DecimalVal(new BigDecimal("4.00")));
        assertThat(countRows(file, gt4))
                .as("Gt(dec_small, 4.00) must return only the single row with value 5.00")
                .isEqualTo(1L);
    }

    @Test
    void decimalFastPathSignedOrderingLtOnePointFiveReturnsNegativeThree() {
        // -3.00 is the only value below 1.50. An unsigned byte comparison of the two's-complement FLBA
        // representation would place -3.00 at the top (0xFF... > 0x00...), returning 0 rows. One returned
        // row proves the comparison is SIGNED.
        Path file = TestCorpus.extractFile(FIXTURE, tempDir);
        Predicate ltOneHalf = new Predicate.Lt(DEC_SMALL, new Value.DecimalVal(new BigDecimal("1.50")));
        assertThat(countRows(file, ltOneHalf))
                .as("Lt(dec_small, 1.50) must return only -3.00, proving signed ordering")
                .isEqualTo(1L);
    }

    @Test
    void decimalFastPathOutOfLongRangeQueryFallsBackToExactComparison() {
        // 92233720368547758.08 has unscaled value 2^63 = 9223372036854775808, which is exactly the boundary
        // that discriminates the guard. Its bitLength is 64. The guard (bitLength < 64) therefore routes it to
        // the exact BigDecimal fallback and the correct answers hold: every file value is below it (Gt -> 0,
        // Lt -> 4). The boundary is chosen deliberately: unscaledValue().longValue() truncates 2^63 to
        // Long.MIN_VALUE. A fast path with the guard removed would then read every cell as greater than
        // Long.MIN_VALUE and return Gt -> 4, Lt -> 0 - failing both assertions loudly. That makes this test a
        // real regression check on the guard.
        Path file = TestCorpus.extractFile(FIXTURE, tempDir);
        BigDecimal boundary = new BigDecimal("92233720368547758.08");
        Predicate gtBoundary = new Predicate.Gt(DEC_SMALL, new Value.DecimalVal(boundary));
        Predicate ltBoundary = new Predicate.Lt(DEC_SMALL, new Value.DecimalVal(boundary));
        assertThat(countRows(file, gtBoundary))
                .as("Gt(dec_small, 2^63) must return 0: no file value exceeds the out-of-range query")
                .isEqualTo(0L);
        assertThat(countRows(file, ltBoundary))
                .as("Lt(dec_small, 2^63) must return all 4 rows: every file value is below the out-of-range query")
                .isEqualTo(4L);
    }

    @Test
    void decimalFastPathStatsEliminatesRowGroupWhenQueryExceedsMax() {
        // dec_small row-group stats: min = -3.00, max = 5.00. A Gt(100.00) predicate allows
        // the STATS tier to eliminate every row group without fetching column data.
        Path file = TestCorpus.extractFile(FIXTURE, tempDir);
        Predicate above = new Predicate.Gt(DEC_SMALL, new Value.DecimalVal(new BigDecimal("100.00")));
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            ExplainPlan plan = reader.explain(above, Projection.ALL, ReadOptions.DEFAULTS);
            assertThat(plan.rowGroups())
                    .as("STATS tier must eliminate every row group: 100.00 exceeds the column max of 5.00")
                    .allMatch(rowGroup -> rowGroup.outcome() == RowGroupOutcome.ELIMINATED);
            assertThat(countRows(reader, above))
                    .as("no row must be returned after STATS elimination")
                    .isEqualTo(0L);
        }
    }

    // --- Decimal BigInteger fallback path (dec_big = FLBA(11), scale 2) ---
    // Values: -3.00, 1.50, 4.00, 5.00

    @Test
    void decimalFallbackPathLtTwoPointZeroReturnsTwoRows() {
        // FLBA(11) exceeds the 8-byte fast-path threshold, forcing the BigInteger fallback.
        // -3.00 and 1.50 are both below 2.00.
        Path file = TestCorpus.extractFile(FIXTURE, tempDir);
        Predicate lt2 = new Predicate.Lt(DEC_BIG, new Value.DecimalVal(new BigDecimal("2.00")));
        assertThat(countRows(file, lt2))
                .as("Lt(dec_big, 2.00) must return the two rows with values -3.00 and 1.50")
                .isEqualTo(2L);
    }

    // --- Time column (t_micros = INT64, Time(MICROS, not UTC-adjusted)) ---
    // Values: 00:00, 01:00, 02:00, 03:00

    @Test
    void timeEqTwoOClockReturnsExactlyOneRow() {
        // The vectorized evaluator converts the LocalTime query to microseconds since midnight
        // and compares it directly against the INT64 column values.
        Path file = TestCorpus.extractFile(FIXTURE, tempDir);
        Predicate eq2 = new Predicate.Eq(T_MICROS, new Value.TimeVal(LocalTime.of(2, 0)));
        assertThat(countRows(file, eq2))
                .as("Eq(t_micros, 02:00) must return exactly the one row with that time")
                .isEqualTo(1L);
    }

    // --- Fixed-length binary with no logical type (fx = FLBA(4)) ---
    // Values: 0x00000001, 0x00000002, 0x00000003, 0x00000004

    @Test
    void fixedLenBinaryWithNoLogicalTypeComparesViaBinaryVal() {
        // A FLBA column with no logical type routes through the BinaryVal comparison path, not
        // DecimalVal or UuidVal. Matching the third row's pattern must return exactly one row.
        Path file = TestCorpus.extractFile(FIXTURE, tempDir);
        byte[] pattern = {0x00, 0x00, 0x00, 0x03};
        Predicate eq3 = new Predicate.Eq(FX, new Value.BinaryVal(MemorySegment.ofArray(pattern)));
        assertThat(countRows(file, eq3))
                .as("Eq(fx, 0x00000003) must return exactly the third row")
                .isEqualTo(1L);
    }

    // --- Helper methods ---

    private static long countRows(Path file, Predicate predicate) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return countRows(ParquetFileReader.open(source), predicate);
        }
    }

    private static long countRows(ParquetFileReader reader, Predicate predicate) {
        try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.count();
        }
    }
}
