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
package io.tileverse.parquetry.internal.filter;

import static io.tileverse.parquetry.filter.Pred.col;
import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

class StatsEvaluatorTest {

    private static final long ROW_COUNT = 1000;

    @Test
    void eqValueAboveMaxIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").eq(2030), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void eqValueBelowMinIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").eq(2000), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void eqValueWithinRangeIsInconclusive() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").eq(2015), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void eqSingleDistinctValueWithoutNullsPassesAll() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2020, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").eq(2020), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void notEqValueOutsideRangePassesAll() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").notEq(2030), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void notEqSingleDistinctMatchingValueIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2020, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").notEq(2020), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void ltAboveMaxPassesAll() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").lt(2030), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void ltAtMinIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").lt(2010), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void ltEqAtMinPassesAll() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2010, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").ltEq(2010), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void gtAtMaxIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").gt(2020), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void gtEqAboveMaxIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").gtEq(2030), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void inWithAllValuesOutsideRangeIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").inInts(2030, 2040), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void inWithOneValueInsideRangeIsInconclusive() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").inInts(2015, 2030), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void isNullWithNullCountZeroIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").isNull(), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void isNullWithAllNullsPassesAll() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2020, ROW_COUNT));
        PruningDecision d = StatsEvaluator.evaluate(col("year").isNull(), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void isNotNullWithAllNullsIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2020, ROW_COUNT));
        PruningDecision d = StatsEvaluator.evaluate(col("year").isNotNull(), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void missingStatsYieldsNotApplied() {
        FilterPipeline.ColumnStatsLookup cols = empty();
        PruningDecision d = StatsEvaluator.evaluate(col("year").eq(2020), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void missingMinMaxYieldsNotApplied() {
        Statistics noBounds =
                Statistics.builder().nullCount(OptionalLong.of(0L)).build();
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, noBounds);
        PruningDecision d = StatsEvaluator.evaluate(col("year").eq(2020), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void andEliminatesIfAnyChildEliminates() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2020, 0));
        Predicate p = col("year").eq(2030).and(col("year").eq(2015));
        PruningDecision d = StatsEvaluator.evaluate(p, cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void orEliminatesOnlyIfAllChildrenEliminate() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2020, 0));
        Predicate p = col("year").eq(2030).or(col("year").eq(2040));
        PruningDecision d = StatsEvaluator.evaluate(p, cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void orWithOnePassingChildPassesAll() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2020, 2020, 0));
        Predicate p = col("year").eq(2020).or(col("year").eq(2030));
        PruningDecision d = StatsEvaluator.evaluate(p, cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void alwaysTrueIsPassedAll() {
        assertThat(StatsEvaluator.evaluate(Predicate.ALWAYS_TRUE, empty(), ROW_COUNT))
                .isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void alwaysFalseIsEliminated() {
        assertThat(StatsEvaluator.evaluate(Predicate.ALWAYS_FALSE, empty(), ROW_COUNT))
                .isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void bboxIntersectsIsNotApplied() {
        FilterPipeline.ColumnStatsLookup cols = empty();
        Predicate p = col("geom").intersects(io.tileverse.parquetry.filter.Bbox.of2d(0, 0, 1, 1));
        assertThat(StatsEvaluator.evaluate(p, cols, ROW_COUNT)).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void doubleColumnLtPrunes() {
        FilterPipeline.ColumnStatsLookup cols = single("price", PrimitiveKind.DOUBLE, doubleStats(1.0, 5.0, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("price").lt(0.5), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void stringColumnEqInRange() {
        FilterPipeline.ColumnStatsLookup cols =
                single("name", PrimitiveKind.BYTE_ARRAY, binaryStats("alpha", "omega", 0));
        PruningDecision d = StatsEvaluator.evaluate(col("name").eq("mango"), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void stringColumnEqBelowRangeIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols =
                single("name", PrimitiveKind.BYTE_ARRAY, binaryStats("delta", "omega", 0));
        PruningDecision d = StatsEvaluator.evaluate(col("name").eq("alpha"), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void gtNotProvenAllMatchWhenColumnHasNulls() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2020, 3));
        PruningDecision d = StatsEvaluator.evaluate(col("year").gt(2000), cols, ROW_COUNT);
        // the guard routes this to NotApplied, not PassedAll
        assertThat(d).isNotInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void gtProvenAllMatchWhenNoNulls() {
        FilterPipeline.ColumnStatsLookup cols = single("year", PrimitiveKind.INT32, intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").gt(2000), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void gtProvenAllMatchForLongColumnWithoutNulls() {
        FilterPipeline.ColumnStatsLookup cols = single("epoch", PrimitiveKind.INT64, longStats(2010L, 2020L, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("epoch").gt(2000L), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void doubleComparisonNeverProvenAllMatch() {
        FilterPipeline.ColumnStatsLookup cols = single("price", PrimitiveKind.DOUBLE, doubleStats(10.0, 20.0, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("price").gt(5.0), cols, ROW_COUNT);
        // the guard routes this to NotApplied, not PassedAll
        assertThat(d).isNotInstanceOf(PruningDecision.PassedAll.class);
    }

    private static FilterPipeline.ColumnStatsLookup single(String name, PrimitiveKind kind, Statistics stats) {
        Map<ColumnPath, FilterPipeline.ColumnStats> map = new HashMap<>();
        map.put(ColumnPath.of(name), new FilterPipeline.ColumnStats(kind, stats));
        return path -> Optional.ofNullable(map.get(path));
    }

    private static FilterPipeline.ColumnStatsLookup empty() {
        return path -> Optional.empty();
    }

    private static Statistics intStats(int min, int max, long nullCount) {
        return Statistics.builder()
                .nullCount(OptionalLong.of(nullCount))
                .maxValue(encodeInt(max))
                .minValue(encodeInt(min))
                .build();
    }

    private static Statistics longStats(long min, long max, long nullCount) {
        return Statistics.builder()
                .nullCount(OptionalLong.of(nullCount))
                .maxValue(encodeLong(max))
                .minValue(encodeLong(min))
                .build();
    }

    private static Statistics doubleStats(double min, double max, long nullCount) {
        return Statistics.builder()
                .nullCount(OptionalLong.of(nullCount))
                .maxValue(encodeDouble(max))
                .minValue(encodeDouble(min))
                .build();
    }

    private static Statistics binaryStats(String min, String max, long nullCount) {
        return Statistics.builder()
                .nullCount(OptionalLong.of(nullCount))
                .maxValue(MemorySegment.ofArray(max.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                .minValue(MemorySegment.ofArray(min.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                .build();
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

    private static MemorySegment encodeDouble(double v) {
        MemorySegment segment = MemorySegment.ofArray(new byte[8]);
        segment.set(DOUBLE, 0, v);
        return segment.asReadOnly();
    }
}
