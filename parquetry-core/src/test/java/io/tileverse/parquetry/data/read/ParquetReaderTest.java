/*
 * Copyright (c) 2026 Multivers.io
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
package io.tileverse.parquetry.data.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.ExplainPlan;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.RowGroupOutcome;
import io.tileverse.parquetry.filter.RowGroupPlan;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.testsupport.CountingSegmentPool;

class ParquetReaderTest {

    @Test
    void provenMatchedPredicateReturnsEveryRowAndEqualsUnfilteredRead(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, 4_000);
        try (ByteRangeSource src = TestParquetFiles.openRangeReader(file)) {
            ParquetReader reader = ParquetReader.open(src);

            long all;
            try (Stream<ParquetRecord> s = reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                all = s.count();
            }

            // The "year" column holds values 2020..2024. year >= 2020 is provably true for every
            // row of every row group: statistics prove MATCHED, not merely FULL.
            Predicate provenMatched = Pred.col("year").gtEq(2020);
            assertThat(everyRowGroupMatched(reader, provenMatched))
                    .as("the predicate must prove MATCHED for every row group to exercise the record-eval skip")
                    .isTrue();

            long matched;
            try (Stream<ParquetRecord> s = reader.read(provenMatched, Projection.ALL, ReadOptions.DEFAULTS)) {
                matched = s.count();
            }

            assertThat(matched).isEqualTo(all);
        }
    }

    private static boolean everyRowGroupMatched(ParquetReader reader, Predicate predicate) {
        return everyRowGroupHasOutcome(reader, predicate, RowGroupOutcome.MATCHED);
    }

    private static boolean everyRowGroupHasOutcome(
            ParquetReader reader, Predicate predicate, RowGroupOutcome expected) {
        ExplainPlan plan = reader.explain(predicate, Projection.ALL, ReadOptions.DEFAULTS);
        for (RowGroupPlan rowGroup : plan.rowGroups()) {
            if (rowGroup.outcome() != expected) {
                return false;
            }
        }
        return true;
    }

    // ---- count() ----

    @ParameterizedTest
    @MethodSource("countPredicates")
    void countEqualsFilteredReadCount(Predicate p, @TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, 4_000);
        try (ByteRangeSource src = TestParquetFiles.openRangeReader(file)) {
            ParquetReader reader = ParquetReader.open(src);
            long viaCount = reader.count(p, ReadOptions.DEFAULTS);
            long viaRead;
            try (Stream<ParquetRecord> rows = reader.read(p, Projection.ALL, ReadOptions.DEFAULTS)) {
                viaRead = rows.count();
            }
            assertThat(viaCount).isEqualTo(viaRead);
        }
    }

    // year is INT32 holding 2020..2024; country is "AR"/"BR" (no nulls); value is i * 1.5.
    static List<Predicate> countPredicates() {
        return List.of(
                Predicate.ALWAYS_TRUE, // metadata short-circuit
                Pred.col("year").gtEq(2020), // proven MATCHED for every row group
                Pred.col("year").gt(9_999), // ELIMINATED for every row group
                Pred.col("year").eq(2022), // residual FULL/PARTIAL: some rows decode and match
                Pred.col("value").lt(3_000.0), // residual range comparison
                Pred.col("country").isNotNull()); // no-nulls -> proven MATCHED at stats; still equals read().count()
    }

    // A FIXED_LEN_BYTE_ARRAY column is MemorySegment-backed and never proves MATCHED, because binary statistics may be
    // truncated. An equality on it therefore always lands a residual FULL/PARTIAL row group and exercises the
    // vectorized count's binary comparison arm. INT96 reaches the same arm through the same MemorySegment path.
    @Test
    void countEqualsFilteredReadCountOnFixedLenBinaryColumn(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFixedLenColumnFileMultiRowGroup(tmp, 4_000);
        Predicate p = Pred.col("uid").eq(TestParquetFiles.fixedUid(1_234));
        try (ByteRangeSource src = TestParquetFiles.openRangeReader(file)) {
            ParquetReader reader = ParquetReader.open(src);

            assertThat(everyRowGroupHasOutcome(reader, p, RowGroupOutcome.ELIMINATED))
                    .as("a residual row group must remain, otherwise the vectorized binary arm is never reached")
                    .isFalse();

            long viaCount = reader.count(p, ReadOptions.DEFAULTS);
            long viaRead;
            try (Stream<ParquetRecord> rows = reader.read(p, Projection.ALL, ReadOptions.DEFAULTS)) {
                viaRead = rows.count();
            }
            assertThat(viaCount).isEqualTo(viaRead).isEqualTo(1L);
        }
    }

    @Test
    void matchedAndEliminatedPredicatesLandWhereIntended(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, 4_000);
        try (ByteRangeSource src = TestParquetFiles.openRangeReader(file)) {
            ParquetReader reader = ParquetReader.open(src);

            assertThat(everyRowGroupHasOutcome(reader, Pred.col("year").gtEq(2020), RowGroupOutcome.MATCHED))
                    .as("year >= 2020 must prove MATCHED for every row group")
                    .isTrue();
            assertThat(everyRowGroupHasOutcome(reader, Pred.col("year").gt(9_999), RowGroupOutcome.ELIMINATED))
                    .as("year > 9999 must ELIMINATE every row group")
                    .isTrue();
        }
    }

    @Test
    void matchedPredicateDecodesNothing(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, 4_000);
        CountingSegmentPool pool = new CountingSegmentPool();
        ReadOptions opts = ReadOptions.builder().segmentPool(pool).build();
        try (ByteRangeSource src = TestParquetFiles.openRangeReader(file)) {
            long n = ParquetReader.open(src).count(Pred.col("year").gtEq(2020), opts);
            assertThat(n).isPositive();
            assertThat(pool.totalBorrows())
                    .as("MATCHED groups add their row count from metadata and borrow no buffers")
                    .isZero();
        }
    }
}
