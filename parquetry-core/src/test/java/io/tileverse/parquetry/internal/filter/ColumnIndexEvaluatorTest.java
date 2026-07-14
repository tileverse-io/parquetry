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
package io.tileverse.parquetry.internal.filter;

import static io.tileverse.parquetry.filter.Pred.col;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.format.BoundaryOrder;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

class ColumnIndexEvaluatorTest {

    /**
     * Three pages, 100 rows each (0-99, 100-199, 200-299): - page 0: year in [2010, 2015] - page 1: year in [2018,
     * 2022] - page 2: year in [2025, 2030]
     */
    private static final long ROW_GROUP_ROWS = 300;

    @Test
    void eqInMiddlePageNarrows() {
        FilterPipeline.ColumnPageStatsLookup cols = year3Pages();
        PruningDecision d = ColumnIndexEvaluator.evaluate(col("year").eq(2020), cols, ROW_GROUP_ROWS);
        assertThat(d).isInstanceOf(PruningDecision.NarrowedTo.class);
        RowRanges r = ((PruningDecision.NarrowedTo) d).ranges();
        assertThat(r.ranges()).containsExactly(new Range(100, 199));
    }

    @Test
    void eqOutsideAllPagesEliminates() {
        FilterPipeline.ColumnPageStatsLookup cols = year3Pages();
        PruningDecision d = ColumnIndexEvaluator.evaluate(col("year").eq(1999), cols, ROW_GROUP_ROWS);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void ltKeepsPagesBelow() {
        FilterPipeline.ColumnPageStatsLookup cols = year3Pages();
        PruningDecision d = ColumnIndexEvaluator.evaluate(col("year").lt(2020), cols, ROW_GROUP_ROWS);
        assertThat(d).isInstanceOf(PruningDecision.NarrowedTo.class);
        RowRanges r = ((PruningDecision.NarrowedTo) d).ranges();
        assertThat(r.ranges()).containsExactly(new Range(0, 99), new Range(100, 199));
    }

    @Test
    void gtKeepsPagesAbove() {
        FilterPipeline.ColumnPageStatsLookup cols = year3Pages();
        PruningDecision d = ColumnIndexEvaluator.evaluate(col("year").gt(2023), cols, ROW_GROUP_ROWS);
        assertThat(d).isInstanceOf(PruningDecision.NarrowedTo.class);
        RowRanges r = ((PruningDecision.NarrowedTo) d).ranges();
        assertThat(r.ranges()).containsExactly(new Range(200, 299));
    }

    @Test
    void rangePredicateNarrowsToOnePage() {
        FilterPipeline.ColumnPageStatsLookup cols = year3Pages();
        Predicate p = col("year").gtEq(2018).and(col("year").ltEq(2022));
        PruningDecision d = ColumnIndexEvaluator.evaluate(p, cols, ROW_GROUP_ROWS);
        assertThat(d).isInstanceOf(PruningDecision.NarrowedTo.class);
        RowRanges r = ((PruningDecision.NarrowedTo) d).ranges();
        assertThat(r.totalRows()).isEqualTo(100);
    }

    @Test
    void everyPageMatchesIsPassedAll() {
        FilterPipeline.ColumnPageStatsLookup cols = year3Pages();
        PruningDecision d = ColumnIndexEvaluator.evaluate(col("year").gtEq(2000), cols, ROW_GROUP_ROWS);
        assertThat(d).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void missingIndexYieldsNotApplied() {
        FilterPipeline.ColumnPageStatsLookup cols = empty();
        PruningDecision d = ColumnIndexEvaluator.evaluate(col("year").eq(2020), cols, ROW_GROUP_ROWS);
        assertThat(d).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void bboxYieldsNotApplied() {
        FilterPipeline.ColumnPageStatsLookup cols = year3Pages();
        Predicate p = col("year").intersects(io.tileverse.parquetry.filter.Bbox.of2d(0, 0, 1, 1));
        PruningDecision d = ColumnIndexEvaluator.evaluate(p, cols, ROW_GROUP_ROWS);
        assertThat(d).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void allNullPageDoesNotMatchValueComparison() {
        FilterPipeline.ColumnPageStatsLookup cols = singleColumn(
                "year",
                PrimitiveKind.INT32,
                /* nullPages */ List.of(true, false),
                /* min */ List.of(encodeInt(0), encodeInt(2018)),
                /* max */ List.of(encodeInt(0), encodeInt(2022)),
                /* pageFirstRowIndices */ List.of(0L, 100L));
        PruningDecision d = ColumnIndexEvaluator.evaluate(col("year").eq(2020), cols, 200);
        assertThat(d).isInstanceOf(PruningDecision.NarrowedTo.class);
        RowRanges r = ((PruningDecision.NarrowedTo) d).ranges();
        assertThat(r.ranges()).containsExactly(new Range(100, 199));
    }

    @Test
    void orUnionsRanges() {
        FilterPipeline.ColumnPageStatsLookup cols = year3Pages();
        Predicate p = col("year").lt(2016).or(col("year").gt(2023));
        PruningDecision d = ColumnIndexEvaluator.evaluate(p, cols, ROW_GROUP_ROWS);
        assertThat(d).isInstanceOf(PruningDecision.NarrowedTo.class);
        RowRanges r = ((PruningDecision.NarrowedTo) d).ranges();
        assertThat(r.ranges()).containsExactly(new Range(0, 99), new Range(200, 299));
    }

    @Test
    void andIntersectsRanges() {
        FilterPipeline.ColumnPageStatsLookup cols = year3Pages();
        Predicate p = col("year").gtEq(2010).and(col("year").ltEq(2015));
        PruningDecision d = ColumnIndexEvaluator.evaluate(p, cols, ROW_GROUP_ROWS);
        assertThat(d).isInstanceOf(PruningDecision.NarrowedTo.class);
        RowRanges r = ((PruningDecision.NarrowedTo) d).ranges();
        assertThat(r.ranges()).containsExactly(new Range(0, 99));
    }

    // --- typed-bound tests: verify that INT64 Timestamp and FLBA Decimal page bounds decode to typed Values ---

    @Test
    void int64TimestampEqNarrowsToMiddlePage() {
        // 3 pages of 100 rows each; page boundaries are UTC timestamps stored as epoch-micros INT64
        LocalDateTime t0 = LocalDateTime.of(2020, 1, 1, 0, 0, 0);
        LocalDateTime t1 = LocalDateTime.of(2020, 6, 30, 0, 0, 0);
        LocalDateTime t2 = LocalDateTime.of(2021, 1, 1, 0, 0, 0);
        LocalDateTime t3 = LocalDateTime.of(2021, 6, 30, 0, 0, 0);
        LocalDateTime t4 = LocalDateTime.of(2022, 1, 1, 0, 0, 0);
        LocalDateTime t5 = LocalDateTime.of(2022, 6, 30, 0, 0, 0);
        FilterPipeline.ColumnPageStatsLookup cols = singleColumn(
                "ts",
                PrimitiveKind.INT64,
                List.of(false, false, false),
                List.of(encodeTimestampMicros(t0), encodeTimestampMicros(t2), encodeTimestampMicros(t4)),
                List.of(encodeTimestampMicros(t1), encodeTimestampMicros(t3), encodeTimestampMicros(t5)),
                List.of(0L, 100L, 200L),
                new LogicalType.Timestamp(true, LogicalType.TimeUnit.MICROS));
        // 2021-03-15 is within [t2, t3] (page 1) only.
        LocalDateTime query = LocalDateTime.of(2021, 3, 15, 0, 0, 0);
        Predicate p = new Predicate.Eq(ColumnPath.of("ts"), new Value.TimestampVal(query, true));
        PruningDecision d = ColumnIndexEvaluator.evaluate(p, cols, ROW_GROUP_ROWS);
        assertThat(d).isInstanceOf(PruningDecision.NarrowedTo.class);
        RowRanges r = ((PruningDecision.NarrowedTo) d).ranges();
        assertThat(r.ranges()).containsExactly(new Range(100, 199));
    }

    @Test
    void flbaDecimalEqNarrowsToMiddlePage() {
        // 3 pages of 100 rows; scale=2, page ranges: [-3.00,-1.00], [0.00,2.00], [3.00,5.00]
        FilterPipeline.ColumnPageStatsLookup cols = singleColumn(
                "amount",
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                List.of(false, false, false),
                List.of(encodeSignedFlba(-300), encodeSignedFlba(0), encodeSignedFlba(300)),
                List.of(encodeSignedFlba(-100), encodeSignedFlba(200), encodeSignedFlba(500)),
                List.of(0L, 100L, 200L),
                new LogicalType.Decimal(2, 9));
        // 1.00 (unscaled 100) is within [0.00, 2.00] (page 1) only.
        Predicate p = new Predicate.Eq(ColumnPath.of("amount"), new Value.DecimalVal(BigDecimal.valueOf(100, 2)));
        PruningDecision d = ColumnIndexEvaluator.evaluate(p, cols, ROW_GROUP_ROWS);
        assertThat(d).isInstanceOf(PruningDecision.NarrowedTo.class);
        RowRanges r = ((PruningDecision.NarrowedTo) d).ranges();
        assertThat(r.ranges()).containsExactly(new Range(100, 199));
    }

    // --- helpers ---

    private static FilterPipeline.ColumnPageStatsLookup year3Pages() {
        return singleColumn(
                "year",
                PrimitiveKind.INT32,
                List.of(false, false, false),
                List.of(encodeInt(2010), encodeInt(2018), encodeInt(2025)),
                List.of(encodeInt(2015), encodeInt(2022), encodeInt(2030)),
                List.of(0L, 100L, 200L));
    }

    private static FilterPipeline.ColumnPageStatsLookup singleColumn(
            String name,
            PrimitiveKind kind,
            List<Boolean> nullPages,
            List<MemorySegment> minValues,
            List<MemorySegment> maxValues,
            List<Long> pageFirstRowIndices) {
        return singleColumn(name, kind, nullPages, minValues, maxValues, pageFirstRowIndices, null);
    }

    private static FilterPipeline.ColumnPageStatsLookup singleColumn(
            String name,
            PrimitiveKind kind,
            List<Boolean> nullPages,
            List<MemorySegment> minValues,
            List<MemorySegment> maxValues,
            List<Long> pageFirstRowIndices,
            LogicalType logicalType) {
        ColumnIndex idx = new ColumnIndex(
                nullPages,
                minValues,
                maxValues,
                BoundaryOrder.ASCENDING,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        List<PageLocation> pages = pageFirstRowIndices.stream()
                .map(first -> new PageLocation(0L, 0, first))
                .toList();
        OffsetIndex off = new OffsetIndex(pages, Optional.empty());
        Map<ColumnPath, FilterPipeline.ColumnPageStats> map = new HashMap<>();
        FilterPipeline.ColumnPageStats stats = (logicalType == null)
                ? new FilterPipeline.ColumnPageStats(kind, idx, off)
                : new FilterPipeline.ColumnPageStats(kind, idx, off, Optional.of(logicalType));
        map.put(ColumnPath.of(name), stats);
        return path -> Optional.ofNullable(map.get(path));
    }

    private static FilterPipeline.ColumnPageStatsLookup empty() {
        return path -> Optional.empty();
    }

    private static MemorySegment encodeInt(int v) {
        MemorySegment segment = MemorySegment.ofArray(new byte[4]);
        segment.set(INT32, 0, v);
        return segment.asReadOnly();
    }

    private static MemorySegment encodeLong(long v) {
        MemorySegment segment = MemorySegment.ofArray(new byte[8]);
        segment.set(INT64, 0, v);
        return segment.asReadOnly();
    }

    private static MemorySegment encodeTimestampMicros(LocalDateTime dt) {
        long micros = TemporalValues.toEpochUnit(dt, LogicalType.TimeUnit.MICROS);
        return encodeLong(micros);
    }

    /** Encodes an unscaled decimal integer as a 4-byte signed big-endian two's-complement segment. */
    private static MemorySegment encodeSignedFlba(int unscaled) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) ((unscaled >>> 24) & 0xFF);
        bytes[1] = (byte) ((unscaled >>> 16) & 0xFF);
        bytes[2] = (byte) ((unscaled >>> 8) & 0xFF);
        bytes[3] = (byte) (unscaled & 0xFF);
        return MemorySegment.ofArray(bytes).asReadOnly();
    }
}
