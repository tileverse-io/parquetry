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
import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

class StatsEvaluatorTest {

    private static final long ROW_COUNT = 1000;

    @Test
    void eqValueAboveMaxIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").eq(2030), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void eqValueBelowMinIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").eq(2000), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void eqValueWithinRangeIsInconclusive() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").eq(2015), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void eqSingleDistinctValueWithoutNullsPassesAll() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2020, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").eq(2020), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void notEqValueOutsideRangePassesAll() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").notEq(2030), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void notEqSingleDistinctMatchingValueIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2020, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").notEq(2020), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void ltAboveMaxPassesAll() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").lt(2030), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void ltAtMinIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").lt(2010), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void ltEqAtMinPassesAll() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2010, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").ltEq(2010), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void gtAtMaxIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").gt(2020), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void gtEqAboveMaxIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").gtEq(2030), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void inWithAllValuesOutsideRangeIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").inInts(2030, 2040), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void inWithOneValueInsideRangeIsInconclusive() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").inInts(2015, 2030), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void isNullWithNullCountZeroIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").isNull(), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void isNullWithAllNullsPassesAll() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, ROW_COUNT));
        PruningDecision d = StatsEvaluator.evaluate(col("year").isNull(), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void isNotNullWithAllNullsIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, ROW_COUNT));
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
        FilterPipeline.ColumnStats noBounds = new FilterPipeline.ColumnStats(
                PrimitiveKind.INT32,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(0L),
                Optional.empty(),
                NaNCells.POSSIBLE);
        FilterPipeline.ColumnStatsLookup cols = single("year", noBounds);
        PruningDecision d = StatsEvaluator.evaluate(col("year").eq(2020), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void halfFloatBoundsLeaveAPresentValueUnpruned() {
        // Ordered by value, -2.0 (0xC000) is the min and 1.0 (0x3C00) the max; as unsigned bytes -2.0 sorts above 1.0.
        MemorySegment minusTwo =
                MemorySegment.ofArray(new byte[] {0x00, (byte) 0xC0}).asReadOnly();
        MemorySegment one = MemorySegment.ofArray(new byte[] {0x00, 0x3C}).asReadOnly();
        FilterPipeline.ColumnStatsLookup cols = single(
                "h", annotatedStats(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, minusTwo, one, new LogicalType.Float16Type()));
        PruningDecision d = StatsEvaluator.evaluate(col("h").eq(minusTwo), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void unsignedFullWidthIntegerBoundsLeaveAPresentValueUnpruned() {
        // Unsigned, 3_000_000_000 is the max; its bits read as a signed int are negative.
        int threeBillionBits = (int) 3_000_000_000L;
        FilterPipeline.ColumnStatsLookup cols = single(
                "u",
                annotatedStats(
                        PrimitiveKind.INT32,
                        encodeInt(1),
                        encodeInt(threeBillionBits),
                        new LogicalType.IntType((byte) 32, false)));
        PruningDecision d = StatsEvaluator.evaluate(col("u").gt(0), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void andEliminatesIfAnyChildEliminates() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 0));
        Predicate p = col("year").eq(2030).and(col("year").eq(2015));
        PruningDecision d = StatsEvaluator.evaluate(p, cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void orEliminatesOnlyIfAllChildrenEliminate() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 0));
        Predicate p = col("year").eq(2030).or(col("year").eq(2040));
        PruningDecision d = StatsEvaluator.evaluate(p, cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void orWithOnePassingChildPassesAll() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2020, 2020, 0));
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
        FilterPipeline.ColumnStatsLookup cols = single("price", doubleStats(1.0, 5.0, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("price").lt(0.5), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void nanBoundIsIgnored() {
        FilterPipeline.ColumnStatsLookup cols = single("price", doubleStats(Double.NaN, 5.0, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("price").gt(10.0), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void summaryLeavesANaNBoundOut() {
        StatsEvaluator.ColumnSummary summary = StatsEvaluator.summarize(doubleStats(1.0, Double.NaN, 0));
        assertThat(summary.min()).contains(new Value.DoubleVal(1.0));
        assertThat(summary.max()).isEmpty();
    }

    @Test
    void nanLiteralEqualityIsNotEliminatedByTheBounds() {
        FilterPipeline.ColumnStatsLookup cols = single("price", doubleStats(1.0, 5.0, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("price").eq(Double.NaN), cols, ROW_COUNT);
        assertThat(d).isNotInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void inWithANaNLiteralIsNotEliminatedByTheBounds() {
        FilterPipeline.ColumnStatsLookup cols = single("price", doubleStats(1.0, 5.0, 0));
        Predicate p = new Predicate.In(
                ColumnPath.of("price"), List.of(new Value.DoubleVal(100.0), new Value.DoubleVal(Double.NaN)));
        assertThat(StatsEvaluator.evaluate(p, cols, ROW_COUNT)).isNotInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void notEqOnASingleValueFloatColumnIsNotEliminated() {
        // NaN cells lie outside [2.0, 2.0] and differ from 2.0
        FilterPipeline.ColumnStatsLookup cols = single("price", doubleStats(2.0, 2.0, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("price").notEq(2.0), cols, ROW_COUNT);
        assertThat(d).isNotInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void orderedComparisonAgainstANaNLiteralIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("price", doubleStats(1.0, 5.0, 0));
        assertThat(StatsEvaluator.evaluate(col("price").lt(Double.NaN), cols, ROW_COUNT))
                .isInstanceOf(PruningDecision.Eliminated.class);
        assertThat(StatsEvaluator.evaluate(col("price").gtEq(Double.NaN), cols, ROW_COUNT))
                .isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void eqNaNOnANaNFreeColumnIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("price", nanFreeStats(1.0, 5.0, 0));

        PruningDecision d = StatsEvaluator.evaluate(col("price").eq(Double.NaN), cols, ROW_COUNT);

        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void inOfOnlyNaNOnANaNFreeColumnIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("price", nanFreeStats(1.0, 5.0, 0));
        Predicate p = new Predicate.In(ColumnPath.of("price"), List.of(new Value.DoubleVal(Double.NaN)));

        assertThat(StatsEvaluator.evaluate(p, cols, ROW_COUNT)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void inWithANaNOnANaNFreeColumnFollowsItsNumbers() {
        FilterPipeline.ColumnStatsLookup cols = single("price", nanFreeStats(1.0, 5.0, 0));
        Predicate withinBounds = new Predicate.In(
                ColumnPath.of("price"), List.of(new Value.DoubleVal(Double.NaN), new Value.DoubleVal(3.0)));
        Predicate outsideBounds = new Predicate.In(
                ColumnPath.of("price"), List.of(new Value.DoubleVal(Double.NaN), new Value.DoubleVal(9.0)));

        assertThat(StatsEvaluator.evaluate(withinBounds, cols, ROW_COUNT))
                .isInstanceOf(PruningDecision.Inconclusive.class);
        assertThat(StatsEvaluator.evaluate(outsideBounds, cols, ROW_COUNT))
                .isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void notEqNaNOnANaNFreeColumnPassesAllWithoutNulls() {
        Predicate p = col("price").notEq(Double.NaN);

        assertThat(StatsEvaluator.evaluate(p, single("price", nanFreeStats(1.0, 5.0, 0)), ROW_COUNT))
                .isInstanceOf(PruningDecision.PassedAll.class);
        assertThat(StatsEvaluator.evaluate(p, single("price", nanFreeStats(1.0, 5.0, 2)), ROW_COUNT))
                .isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void comparisonIsProvenAllMatchOnANaNFreeColumnWithoutNulls() {
        Predicate p = col("price").gt(0.5);

        assertThat(StatsEvaluator.evaluate(p, single("price", nanFreeStats(1.0, 5.0, 0)), ROW_COUNT))
                .isInstanceOf(PruningDecision.PassedAll.class);
        assertThat(StatsEvaluator.evaluate(p, single("price", nanFreeStats(1.0, 5.0, 1)), ROW_COUNT))
                .isNotInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void notEqOnASingleValueNaNFreeColumnIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("price", nanFreeStats(2.0, 2.0, 0));

        PruningDecision d = StatsEvaluator.evaluate(col("price").notEq(2.0), cols, ROW_COUNT);

        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void eqOnASingleValueNaNFreeColumnPassesAll() {
        FilterPipeline.ColumnStatsLookup oneValue = single("price", nanFreeStats(2.0, 2.0, 0));
        FilterPipeline.ColumnStatsLookup bothZeros = single("price", nanFreeStats(-0.0, 0.0, 0));

        assertThat(StatsEvaluator.evaluate(col("price").eq(2.0), oneValue, ROW_COUNT))
                .isInstanceOf(PruningDecision.PassedAll.class);
        assertThat(StatsEvaluator.evaluate(col("price").eq(0.0), bothZeros, ROW_COUNT))
                .isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void comparisonWithANumberOnAColumnOfOnlyNaNIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("price", onlyNaNStats(0));
        Predicate in =
                new Predicate.In(ColumnPath.of("price"), List.of(new Value.DoubleVal(1.0), new Value.DoubleVal(2.0)));

        assertThat(StatsEvaluator.evaluate(col("price").gt(1.0), cols, ROW_COUNT))
                .isInstanceOf(PruningDecision.Eliminated.class);
        assertThat(StatsEvaluator.evaluate(col("price").ltEq(1.0), cols, ROW_COUNT))
                .isInstanceOf(PruningDecision.Eliminated.class);
        assertThat(StatsEvaluator.evaluate(col("price").eq(1.0), cols, ROW_COUNT))
                .isInstanceOf(PruningDecision.Eliminated.class);
        assertThat(StatsEvaluator.evaluate(in, cols, ROW_COUNT)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void eqNaNOnAColumnOfOnlyNaNPassesAllWithoutNulls() {
        Predicate p = col("price").eq(Double.NaN);

        assertThat(StatsEvaluator.evaluate(p, single("price", onlyNaNStats(0)), ROW_COUNT))
                .isInstanceOf(PruningDecision.PassedAll.class);
        assertThat(StatsEvaluator.evaluate(p, single("price", onlyNaNStats(3)), ROW_COUNT))
                .isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void inWithANaNOnAColumnOfOnlyNaNPassesAllWithoutNulls() {
        FilterPipeline.ColumnStatsLookup cols = single("price", onlyNaNStats(0));
        Predicate p = new Predicate.In(
                ColumnPath.of("price"), List.of(new Value.DoubleVal(7.0), new Value.DoubleVal(Double.NaN)));

        assertThat(StatsEvaluator.evaluate(p, cols, ROW_COUNT)).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void notEqOnAColumnOfOnlyNaNFollowsItsLiteral() {
        FilterPipeline.ColumnStatsLookup cols = single("price", onlyNaNStats(0));

        assertThat(StatsEvaluator.evaluate(col("price").notEq(1.0), cols, ROW_COUNT))
                .as("NaN cells differ from a number")
                .isInstanceOf(PruningDecision.PassedAll.class);
        assertThat(StatsEvaluator.evaluate(col("price").notEq(Double.NaN), cols, ROW_COUNT))
                .as("no cell differs from NaN")
                .isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void summaryTellsWhatIsKnownOfTheNaNCells() {
        assertThat(StatsEvaluator.summarize(doubleStats(1.0, 5.0, 0)).nans()).isEqualTo(NaNCells.POSSIBLE);
        assertThat(StatsEvaluator.summarize(nanFreeStats(1.0, 5.0, 0)).nans()).isEqualTo(NaNCells.ABSENT);
        assertThat(StatsEvaluator.summarize(doubleStats(Double.NaN, Double.NaN, 0))
                        .nans())
                .as("NaN bounds without a NaN count")
                .isEqualTo(NaNCells.POSSIBLE);

        StatsEvaluator.ColumnSummary onlyNaN = StatsEvaluator.summarize(onlyNaNStats(0));
        assertThat(onlyNaN.nans()).isEqualTo(NaNCells.ALL);
        assertThat(onlyNaN.min()).isEmpty();
        assertThat(onlyNaN.max()).isEmpty();
    }

    @Test
    void aNumberBoundContradictsACountOfOnlyNaN() {
        StatsEvaluator.ColumnSummary summary =
                StatsEvaluator.summarize(floatingStats(Double.NaN, 5.0, 0, NaNCells.ALL));

        assertThat(summary.nans()).isEqualTo(NaNCells.POSSIBLE);
        assertThat(summary.min()).isEmpty();
        assertThat(summary.max()).contains(new Value.DoubleVal(5.0));
    }

    @Test
    void aNaNBoundContradictsANaNCountOfZero() {
        StatsEvaluator.ColumnSummary summary =
                StatsEvaluator.summarize(floatingStats(1.0, Double.NaN, 0, NaNCells.ABSENT));

        assertThat(summary.nans()).isEqualTo(NaNCells.POSSIBLE);
    }

    @Test
    void nanBoundsWithoutACountOfOnlyNaNDecideNothing() {
        // A writer taking the min and the max over the NaN cells too bounds a chunk holding numbers by two NaNs.
        FilterPipeline.ColumnStatsLookup cols = single("price", doubleStats(Double.NaN, Double.NaN, 0));

        assertThat(StatsEvaluator.evaluate(col("price").eq(1.0), cols, ROW_COUNT))
                .isNotInstanceOf(PruningDecision.Eliminated.class);
        assertThat(StatsEvaluator.evaluate(col("price").notEq(1.0), cols, ROW_COUNT))
                .isNotInstanceOf(PruningDecision.PassedAll.class);
        assertThat(StatsEvaluator.evaluate(col("price").eq(Double.NaN), cols, ROW_COUNT))
                .isNotInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void columnOfOnlyNaNBoundedByNaNIsDecidedFromItsCounts() {
        // A writer following IEEE 754 total order bounds a chunk holding no number by its NaN cells.
        FilterPipeline.ColumnStatsLookup cols = single("price", floatingStats(Double.NaN, Double.NaN, 0, NaNCells.ALL));

        assertThat(StatsEvaluator.evaluate(col("price").gt(1.0), cols, ROW_COUNT))
                .isInstanceOf(PruningDecision.Eliminated.class);
        assertThat(StatsEvaluator.evaluate(col("price").eq(Double.NaN), cols, ROW_COUNT))
                .isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void countedNaNCellsDecideNothingForAHalfFloatColumn() {
        // The cells of a FLOAT16 column compare as bytes, and a byte literal matches a NaN cell with the same bits.
        FilterPipeline.ColumnStats onlyNaNHalves = new FilterPipeline.ColumnStats(
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(0L),
                Optional.of(new LogicalType.Float16Type()),
                NaNCells.ALL);
        FilterPipeline.ColumnStatsLookup cols = single("half", onlyNaNHalves);
        Value nanBits = new Value.BinaryVal(MemorySegment.ofArray(new byte[] {0x00, 0x7E}));

        assertThat(StatsEvaluator.summarize(onlyNaNHalves).nans()).isEqualTo(NaNCells.POSSIBLE);
        assertThat(StatsEvaluator.evaluate(new Predicate.Eq(ColumnPath.of("half"), nanBits), cols, ROW_COUNT))
                .isNotInstanceOf(PruningDecision.Eliminated.class);
        assertThat(StatsEvaluator.evaluate(new Predicate.NotEq(ColumnPath.of("half"), nanBits), cols, ROW_COUNT))
                .isNotInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void inWithoutValuesIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("price", doubleStats(1.0, 5.0, 0));
        Predicate empty = new Predicate.In(ColumnPath.of("price"), List.of());

        assertThat(StatsEvaluator.evaluate(empty, cols, ROW_COUNT)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void zeroBoundsCompareEqualToEitherZero() {
        FilterPipeline.ColumnStatsLookup fromPositiveZero = single("price", doubleStats(0.0, 5.0, 0));
        FilterPipeline.ColumnStatsLookup toNegativeZero = single("price", doubleStats(-5.0, -0.0, 0));
        assertThat(StatsEvaluator.evaluate(col("price").eq(-0.0), fromPositiveZero, ROW_COUNT))
                .isNotInstanceOf(PruningDecision.Eliminated.class);
        assertThat(StatsEvaluator.evaluate(col("price").gtEq(0.0), toNegativeZero, ROW_COUNT))
                .isNotInstanceOf(PruningDecision.Eliminated.class);
        assertThat(StatsEvaluator.evaluate(col("price").gt(0.0), toNegativeZero, ROW_COUNT))
                .isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void notInWithValuesOutsideTheBoundsPassesAllWithoutNullCells() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 0));
        Predicate notIn =
                PredicateNormalizer.normalize(col("year").inInts(2030, 2040).negate());

        assertThat(StatsEvaluator.evaluate(notIn, cols, ROW_COUNT)).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void notInWithValuesOutsideTheBoundsIsNotDecidedOverNullCells() {
        // A null cell is in no list and outside none: the rows holding one do not match the negation.
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 3));
        Predicate notIn =
                PredicateNormalizer.normalize(col("year").inInts(2030, 2040).negate());

        PruningDecision d = StatsEvaluator.evaluate(notIn, cols, ROW_COUNT);

        assertThat(d)
                .isNotInstanceOf(PruningDecision.PassedAll.class)
                .isNotInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void notInWithAValueInsideTheBoundsIsNotDecided() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 0));
        Predicate notIn = PredicateNormalizer.normalize(col("year").inInts(2015).negate());

        PruningDecision d = StatsEvaluator.evaluate(notIn, cols, ROW_COUNT);

        assertThat(d)
                .isNotInstanceOf(PruningDecision.PassedAll.class)
                .isNotInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void notInOfTheSingleValueOfAColumnIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2015, 2015, 0));
        Predicate notIn =
                PredicateNormalizer.normalize(col("year").inInts(2015, 2040).negate());

        assertThat(StatsEvaluator.evaluate(notIn, cols, ROW_COUNT)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void notInWithoutValuesPassesAll() {
        FilterPipeline.ColumnStatsLookup cols = single("price", doubleStats(1.0, 5.0, 3));
        Predicate inNothing = new Predicate.In(ColumnPath.of("price"), List.of());
        Predicate notInNothing = PredicateNormalizer.normalize(inNothing.negate());

        PruningDecision d = StatsEvaluator.evaluate(notInNothing, cols, ROW_COUNT);

        assertThat(d).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void stringColumnEqInRange() {
        FilterPipeline.ColumnStatsLookup cols = single("name", binaryStats("alpha", "omega", 0));
        PruningDecision d = StatsEvaluator.evaluate(col("name").eq("mango"), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void stringColumnEqBelowRangeIsEliminated() {
        FilterPipeline.ColumnStatsLookup cols = single("name", binaryStats("delta", "omega", 0));
        PruningDecision d = StatsEvaluator.evaluate(col("name").eq("alpha"), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void gtNotProvenAllMatchWhenColumnHasNulls() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 3));
        PruningDecision d = StatsEvaluator.evaluate(col("year").gt(2000), cols, ROW_COUNT);
        // the guard routes this to NotApplied, not PassedAll
        assertThat(d).isNotInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void gtProvenAllMatchWhenNoNulls() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2010, 2020, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("year").gt(2000), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void gtProvenAllMatchForLongColumnWithoutNulls() {
        FilterPipeline.ColumnStatsLookup cols = single("epoch", longStats(2010L, 2020L, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("epoch").gt(2000L), cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.PassedAll.class);
    }

    @Test
    void doubleComparisonNeverProvenAllMatch() {
        FilterPipeline.ColumnStatsLookup cols = single("price", doubleStats(10.0, 20.0, 0));
        PruningDecision d = StatsEvaluator.evaluate(col("price").gt(5.0), cols, ROW_COUNT);
        // the guard routes this to NotApplied, not PassedAll
        assertThat(d).isNotInstanceOf(PruningDecision.PassedAll.class);
    }

    // --- typed-bound tests: verify that INT64 Timestamp and FLBA Decimal bounds decode to typed Values ---

    @Test
    void int64TimestampEqAtMinIsInconclusive() {
        LocalDateTime t0 = LocalDateTime.of(2020, 1, 1, 0, 0, 0);
        LocalDateTime t1 = LocalDateTime.of(2025, 1, 1, 0, 0, 0);
        FilterPipeline.ColumnStatsLookup cols = singleTimestamp("ts", t0, t1);
        // t0 is exactly the minimum; the value is within range and the row group cannot be ruled out.
        Predicate p = new Predicate.Eq(ColumnPath.of("ts"), new Value.TimestampVal(t0, true));
        PruningDecision d = StatsEvaluator.evaluate(p, cols, ROW_COUNT);
        assertThat(d).isNotInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void int64TimestampEqBelowMinIsEliminated() {
        LocalDateTime t0 = LocalDateTime.of(2020, 1, 1, 0, 0, 0);
        LocalDateTime t1 = LocalDateTime.of(2025, 1, 1, 0, 0, 0);
        FilterPipeline.ColumnStatsLookup cols = singleTimestamp("ts", t0, t1);
        // One day before the minimum; no column value can match Eq at this timestamp.
        Predicate p = new Predicate.Eq(ColumnPath.of("ts"), new Value.TimestampVal(t0.minusDays(1), true));
        PruningDecision d = StatsEvaluator.evaluate(p, cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("comparisonsOfATimestampColumnWithAnInteger")
    void integerLiteralOnATimestampColumnIsLeftToTheScan(String name, Predicate predicate) {
        // The bounds decode to timestamps, while the scan compares the literal with the stored integers.
        LocalDateTime onlyInstant = LocalDateTime.of(2020, 1, 1, 0, 0, 0);
        FilterPipeline.ColumnStatsLookup cols = singleTimestamp("ts", onlyInstant, onlyInstant);

        PruningDecision d = StatsEvaluator.evaluate(predicate, cols, ROW_COUNT);

        assertThat(d).isInstanceOf(PruningDecision.NotApplied.class);
    }

    static Stream<Arguments> comparisonsOfATimestampColumnWithAnInteger() {
        ColumnPath ts = ColumnPath.of("ts");
        Value five = new Value.LongVal(5L);
        return Stream.of(
                Arguments.of("Eq", new Predicate.Eq(ts, five)),
                Arguments.of("NotEq", new Predicate.NotEq(ts, five)),
                Arguments.of("Lt", new Predicate.Lt(ts, five)),
                Arguments.of("LtEq", new Predicate.LtEq(ts, five)),
                Arguments.of("Gt", new Predicate.Gt(ts, five)),
                Arguments.of("GtEq", new Predicate.GtEq(ts, five)),
                Arguments.of("In", new Predicate.In(ts, List.of(five))));
    }

    @Test
    void bytesLiteralOnAFixedLengthDecimalColumnIsLeftToTheScan() {
        // The bounds decode to decimals, while the scan compares the literal with the stored bytes.
        FilterPipeline.ColumnStatsLookup cols = singleDecimal("amount", 100, 100, 2);
        Value bytes = new Value.BinaryVal(MemorySegment.ofArray(new byte[] {0, 0, 0, 100}));
        Predicate differs = new Predicate.NotEq(ColumnPath.of("amount"), bytes);

        PruningDecision d = StatsEvaluator.evaluate(differs, cols, ROW_COUNT);

        assertThat(d).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void flbaDecimalEqWithinRangeIsInconclusive() {
        // column range [-3.00, 5.00] at scale 2; unscaled [-300, 500]
        FilterPipeline.ColumnStatsLookup cols = singleDecimal("amount", -300, 500, 2);
        // 1.00 is within [-3.00, 5.00]; the row group cannot be ruled out.
        Predicate p = new Predicate.Eq(ColumnPath.of("amount"), new Value.DecimalVal(BigDecimal.valueOf(100, 2)));
        PruningDecision d = StatsEvaluator.evaluate(p, cols, ROW_COUNT);
        assertThat(d).isNotInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void flbaDecimalLtBelowMinIsEliminated() {
        // column range [-3.00, 5.00] at scale 2; unscaled [-300, 500]
        FilterPipeline.ColumnStatsLookup cols = singleDecimal("amount", -300, 500, 2);
        // Lt(-9.00): every column value is >= -3.00 > -9.00 - Lt(-9.00) matches no row.
        Predicate p = new Predicate.Lt(ColumnPath.of("amount"), new Value.DecimalVal(BigDecimal.valueOf(-900, 2)));
        PruningDecision d = StatsEvaluator.evaluate(p, cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void flbaDecimalGtWithinRangeIsNotEliminated() {
        // column range [-3.00, 5.00] at scale 2; unscaled [-300, 500]
        FilterPipeline.ColumnStatsLookup cols = singleDecimal("amount", -300, 500, 2);
        // Gt(4.00): 4.00 < 5.00, rows above 4.00 may exist; the row group must not be eliminated. This
        // discriminates the typed decimal path: an unsigned byte compare would read the operand as equal
        // to max and wrongly eliminate.
        Predicate p = new Predicate.Gt(ColumnPath.of("amount"), new Value.DecimalVal(BigDecimal.valueOf(400, 2)));
        PruningDecision d = StatsEvaluator.evaluate(p, cols, ROW_COUNT);
        assertThat(d).isNotInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void invertedDecimalBoundsAreIgnored() {
        // The bounds left by a writer ordering a binary decimal by its unsigned bytes: a chunk spanning
        // [-49.95, 49.91] gets 5.50 as its minimum and -1.11 as its maximum.
        FilterPipeline.ColumnStatsLookup cols = singleDecimal("amount", 550, -111, 2);
        Predicate p = new Predicate.Gt(ColumnPath.of("amount"), new Value.DecimalVal(BigDecimal.valueOf(1000, 2)));

        PruningDecision d = StatsEvaluator.evaluate(p, cols, ROW_COUNT);

        assertThat(d).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void invertedIntegerBoundsAreIgnored() {
        FilterPipeline.ColumnStatsLookup cols = single("year", intStats(2020, 2010, 0));

        PruningDecision d = StatsEvaluator.evaluate(col("year").eq(2015), cols, ROW_COUNT);

        assertThat(d).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void summaryLeavesInvertedBoundsOut() {
        StatsEvaluator.ColumnSummary summary = StatsEvaluator.summarize(intStats(2020, 2010, 3));

        assertThat(summary.min()).isEmpty();
        assertThat(summary.max()).isEmpty();
        assertThat(summary.nullCount()).hasValue(3L);
    }

    @Test
    void summaryKeepsEqualBounds() {
        StatsEvaluator.ColumnSummary summary = StatsEvaluator.summarize(intStats(2020, 2020, 0));

        assertThat(summary.min()).contains(new Value.IntVal(2020));
        assertThat(summary.max()).contains(new Value.IntVal(2020));
    }

    @Test
    void int32DecimalLtBelowMinIsEliminated() {
        // column range [-3.00, 5.00] at scale 2, stored as the unscaled integers [-300, 500]
        FilterPipeline.ColumnStatsLookup cols = singleInt32Decimal("amount", -300, 500, 2);
        // Lt(-9.00): every column value is >= -3.00 > -9.00 - Lt(-9.00) matches no row.
        Predicate p = new Predicate.Lt(ColumnPath.of("amount"), new Value.DecimalVal(BigDecimal.valueOf(-900, 2)));
        PruningDecision d = StatsEvaluator.evaluate(p, cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void int32DecimalEqWithinRangeIsInconclusive() {
        // column range [-3.00, 5.00] at scale 2, stored as the unscaled integers [-300, 500]
        FilterPipeline.ColumnStatsLookup cols = singleInt32Decimal("amount", -300, 500, 2);
        // 1.00 is within [-3.00, 5.00]; the row group cannot be ruled out. Bounds decoded as plain integers
        // would read the range as [-300, 500] and wrongly eliminate a query of 1.00.
        Predicate p = new Predicate.Eq(ColumnPath.of("amount"), new Value.DecimalVal(BigDecimal.valueOf(100, 2)));
        PruningDecision d = StatsEvaluator.evaluate(p, cols, ROW_COUNT);
        assertThat(d).isNotInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void int64DecimalGtAboveMaxIsEliminated() {
        // column range [-3.00, 5.00] at scale 2, stored as the unscaled integers [-300, 500]
        FilterPipeline.ColumnStatsLookup cols = singleInt64Decimal("amount", -300L, 500L, 2);
        // Gt(9.00): every column value is <= 5.00 < 9.00 - Gt(9.00) matches no row.
        Predicate p = new Predicate.Gt(ColumnPath.of("amount"), new Value.DecimalVal(BigDecimal.valueOf(900, 2)));
        PruningDecision d = StatsEvaluator.evaluate(p, cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void int32DecimalEqAboveMaxIsEliminated() {
        // column range [-3.00, 5.00] at scale 2, stored as the unscaled integers [-300, 500]
        FilterPipeline.ColumnStatsLookup cols = singleInt32Decimal("amount", -300, 500, 2);
        // Eq(9.00): 9.00 exceeds the column max of 5.00 and matches no row. This discriminates the typed
        // decimal path: bounds left as plain integers have no comparison against a decimal query and would
        // decline to prune.
        Predicate p = new Predicate.Eq(ColumnPath.of("amount"), new Value.DecimalVal(BigDecimal.valueOf(900, 2)));
        PruningDecision d = StatsEvaluator.evaluate(p, cols, ROW_COUNT);
        assertThat(d).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void int32DecimalGtWithinRangeIsNotEliminated() {
        // column range [-3.00, 5.00] at scale 2, stored as the unscaled integers [-300, 500]
        FilterPipeline.ColumnStatsLookup cols = singleInt32Decimal("amount", -300, 500, 2);
        // Gt(4.00): 4.00 < 5.00, rows above 4.00 may exist; the row group must not be eliminated. This
        // discriminates the typed decimal path: bounds left as plain integers read the query as equal to max
        // and would wrongly eliminate.
        Predicate p = new Predicate.Gt(ColumnPath.of("amount"), new Value.DecimalVal(BigDecimal.valueOf(400, 2)));
        PruningDecision d = StatsEvaluator.evaluate(p, cols, ROW_COUNT);
        assertThat(d).isNotInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void int64DecimalLtWithinRangeIsNotEliminated() {
        // column range [-3.00, 5.00] at scale 2, stored as the unscaled integers [-300, 500]
        FilterPipeline.ColumnStatsLookup cols = singleInt64Decimal("amount", -300L, 500L, 2);
        // Lt(4.00): -3.00 < 4.00, rows below 4.00 exist; the row group must not be eliminated. Bounds left as
        // plain integers read the query as equal to min and would wrongly eliminate.
        Predicate p = new Predicate.Lt(ColumnPath.of("amount"), new Value.DecimalVal(BigDecimal.valueOf(400, 2)));
        PruningDecision d = StatsEvaluator.evaluate(p, cols, ROW_COUNT);
        assertThat(d).isNotInstanceOf(PruningDecision.Eliminated.class);
    }

    // --- helpers ---

    private static FilterPipeline.ColumnStatsLookup singleTimestamp(String name, LocalDateTime min, LocalDateTime max) {
        long minMicros = TemporalValues.toEpochUnit(min, LogicalType.TimeUnit.MICROS);
        long maxMicros = TemporalValues.toEpochUnit(max, LogicalType.TimeUnit.MICROS);
        LogicalType logicalType = new LogicalType.Timestamp(true, LogicalType.TimeUnit.MICROS);
        FilterPipeline.ColumnStats stats =
                annotatedStats(PrimitiveKind.INT64, encodeLong(minMicros), encodeLong(maxMicros), logicalType);
        return single(name, stats);
    }

    private static FilterPipeline.ColumnStatsLookup singleDecimal(
            String name, int unscaledMin, int unscaledMax, int scale) {
        LogicalType logicalType = new LogicalType.Decimal(scale, 9);
        FilterPipeline.ColumnStats stats = annotatedStats(
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                encodeSignedFlba(unscaledMin),
                encodeSignedFlba(unscaledMax),
                logicalType);
        return single(name, stats);
    }

    private static FilterPipeline.ColumnStatsLookup singleInt32Decimal(
            String name, int unscaledMin, int unscaledMax, int scale) {
        LogicalType logicalType = new LogicalType.Decimal(scale, 9);
        FilterPipeline.ColumnStats stats =
                annotatedStats(PrimitiveKind.INT32, encodeInt(unscaledMin), encodeInt(unscaledMax), logicalType);
        return single(name, stats);
    }

    private static FilterPipeline.ColumnStatsLookup singleInt64Decimal(
            String name, long unscaledMin, long unscaledMax, int scale) {
        LogicalType logicalType = new LogicalType.Decimal(scale, 18);
        FilterPipeline.ColumnStats stats =
                annotatedStats(PrimitiveKind.INT64, encodeLong(unscaledMin), encodeLong(unscaledMax), logicalType);
        return single(name, stats);
    }

    private static FilterPipeline.ColumnStatsLookup single(String name, FilterPipeline.ColumnStats stats) {
        Map<ColumnPath, FilterPipeline.ColumnStats> map = new HashMap<>();
        map.put(ColumnPath.of(name), stats);
        return path -> Optional.ofNullable(map.get(path));
    }

    private static FilterPipeline.ColumnStatsLookup empty() {
        return path -> Optional.empty();
    }

    private static FilterPipeline.ColumnStats intStats(int min, int max, long nullCount) {
        return plainStats(PrimitiveKind.INT32, encodeInt(min), encodeInt(max), nullCount);
    }

    private static FilterPipeline.ColumnStats longStats(long min, long max, long nullCount) {
        return plainStats(PrimitiveKind.INT64, encodeLong(min), encodeLong(max), nullCount);
    }

    private static FilterPipeline.ColumnStats doubleStats(double min, double max, long nullCount) {
        return plainStats(PrimitiveKind.DOUBLE, encodeDouble(min), encodeDouble(max), nullCount);
    }

    /** Bounds of a DOUBLE column with a recorded NaN count of zero. */
    private static FilterPipeline.ColumnStats nanFreeStats(double min, double max, long nullCount) {
        return floatingStats(min, max, nullCount, NaNCells.ABSENT);
    }

    /** A DOUBLE column with NaN and null counts adding up to its values, and no bounds. */
    private static FilterPipeline.ColumnStats onlyNaNStats(long nullCount) {
        return new FilterPipeline.ColumnStats(
                PrimitiveKind.DOUBLE,
                Optional.empty(),
                Optional.empty(),
                OptionalLong.of(nullCount),
                Optional.empty(),
                NaNCells.ALL);
    }

    private static FilterPipeline.ColumnStats floatingStats(double min, double max, long nullCount, NaNCells nans) {
        return new FilterPipeline.ColumnStats(
                PrimitiveKind.DOUBLE,
                Optional.of(encodeDouble(min)),
                Optional.of(encodeDouble(max)),
                OptionalLong.of(nullCount),
                Optional.empty(),
                nans);
    }

    private static FilterPipeline.ColumnStats binaryStats(String min, String max, long nullCount) {
        return plainStats(PrimitiveKind.BYTE_ARRAY, encodeUtf8(min), encodeUtf8(max), nullCount);
    }

    /** Bounds on a column with no logical type annotation. */
    private static FilterPipeline.ColumnStats plainStats(
            PrimitiveKind kind, MemorySegment min, MemorySegment max, long nullCount) {
        return new FilterPipeline.ColumnStats(
                kind,
                Optional.of(min),
                Optional.of(max),
                OptionalLong.of(nullCount),
                Optional.empty(),
                NaNCells.POSSIBLE);
    }

    /** Bounds on a null-free column annotated with {@code logicalType}, which drives typed decoding. */
    private static FilterPipeline.ColumnStats annotatedStats(
            PrimitiveKind kind, MemorySegment min, MemorySegment max, LogicalType logicalType) {
        return new FilterPipeline.ColumnStats(
                kind,
                Optional.of(min),
                Optional.of(max),
                OptionalLong.of(0L),
                Optional.of(logicalType),
                NaNCells.POSSIBLE);
    }

    private static MemorySegment encodeUtf8(String v) {
        return MemorySegment.ofArray(v.getBytes(StandardCharsets.UTF_8)).asReadOnly();
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
