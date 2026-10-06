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
package io.tileverse.parquetry.internal.write;

import static io.tileverse.parquetry.internal.write.WriteFixtures.DOUBLE_LEAF;
import static io.tileverse.parquetry.internal.write.WriteFixtures.DOUBLE_NEGATIVE_NAN;
import static io.tileverse.parquetry.internal.write.WriteFixtures.FLOAT_LEAF;
import static io.tileverse.parquetry.internal.write.WriteFixtures.FLOAT_NEGATIVE_NAN;
import static io.tileverse.parquetry.internal.write.WriteFixtures.HALF_FLOAT_LEAF;
import static io.tileverse.parquetry.internal.write.WriteFixtures.HALF_NAN;
import static io.tileverse.parquetry.internal.write.WriteFixtures.HALF_NEGATIVE_NAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.tileverse.parquetry.data.WriteOptions.FloatColumnOrder;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.write.page.PageStatistics;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * The statistics of FLOAT, DOUBLE and FLOAT16 columns follow the rules of the format for the type-defined order: the
 * bounds are the smallest and largest numbers, a zero bound is recorded as {@code -0.0} for a minimum and as
 * {@code +0.0} for a maximum, the NaN cells are counted and left out of the bounds, and a window holding nothing but
 * NaN has no bounds. The rules shared with IEEE 754 total order are checked in both orders.
 */
class StatisticsAccumulatorFloatingPointTest {

    private static final int FLOAT_NEGATIVE_ZERO = Float.floatToRawIntBits(-0.0f);
    private static final int FLOAT_POSITIVE_ZERO = Float.floatToRawIntBits(0.0f);
    private static final long DOUBLE_NEGATIVE_ZERO = Double.doubleToRawLongBits(-0.0);
    private static final long DOUBLE_POSITIVE_ZERO = Double.doubleToRawLongBits(0.0);
    private static final short HALF_NEGATIVE_ZERO = Float.floatToFloat16(-0.0f);
    private static final short HALF_POSITIVE_ZERO = Float.floatToFloat16(0.0f);

    @ParameterizedTest
    @EnumSource(FloatColumnOrder.class)
    void floatBoundsCoverTheNumbersAndTheNaNsAreCounted(FloatColumnOrder order) {
        StatisticsAccumulator acc = WriteFixtures.accumulator(FLOAT_LEAF, order);
        acc.updateFloat(1.5f);
        acc.updateFloat(Float.NaN);
        acc.updateFloat(-2.25f);
        acc.updateFloat(Float.intBitsToFloat(FLOAT_NEGATIVE_NAN));
        acc.updateFloat(3.0f);
        acc.updateNull();

        Statistics stats = acc.finishChunk();

        assertThat(WriteFixtures.floatBits(stats.minValue())).isEqualTo(Float.floatToRawIntBits(-2.25f));
        assertThat(WriteFixtures.floatBits(stats.maxValue())).isEqualTo(Float.floatToRawIntBits(3.0f));
        assertThat(stats.nanCount()).hasValue(2L);
        assertThat(stats.nullCount()).hasValue(1L);
    }

    @ParameterizedTest
    @EnumSource(FloatColumnOrder.class)
    void floatColumnWithoutNaNsRecordsANaNCountOfZero(FloatColumnOrder order) {
        StatisticsAccumulator acc = WriteFixtures.accumulator(FLOAT_LEAF, order);
        acc.updateFloat(1.5f);

        assertThat(acc.finishChunk().nanCount()).hasValue(0L);
        assertThat(acc.finishPage().nanCount()).hasValue(0L);
    }

    @ParameterizedTest
    @EnumSource(FloatColumnOrder.class)
    void emptyFloatChunkRecordsANaNCountOfZeroAndNoBounds(FloatColumnOrder order) {
        Statistics stats = WriteFixtures.accumulator(FLOAT_LEAF, order).finishChunk();

        assertThat(stats.nanCount()).hasValue(0L);
        assertThat(stats.minValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).isEqualTo(MemorySegment.NULL);
    }

    @ParameterizedTest
    @EnumSource(FloatColumnOrder.class)
    void floatColumnOfBothZerosIsBoundedByNegativeZeroAndPositiveZero(FloatColumnOrder order) {
        StatisticsAccumulator acc = WriteFixtures.accumulator(FLOAT_LEAF, order);
        acc.updateFloat(0.0f);
        acc.updateFloat(-0.0f);

        Statistics stats = acc.finishChunk();

        assertThat(WriteFixtures.floatBits(stats.minValue())).isEqualTo(FLOAT_NEGATIVE_ZERO);
        assertThat(WriteFixtures.floatBits(stats.maxValue())).isEqualTo(FLOAT_POSITIVE_ZERO);
    }

    @Test
    void halfFloatAnnotationOnAnotherWidthRecordsNoNaNCount() {
        StatisticsAccumulator acc =
                WriteFixtures.accumulator(WriteFixtures.fixedLeaf(4, new LogicalType.Float16Type()));
        acc.updateBinary(MemorySegment.ofArray(new byte[] {0x00, 0x7E, 0x00, 0x7E}));

        assertThat(acc.finishChunk().nanCount()).isEmpty();
    }

    @Test
    void integerColumnRecordsNoNaNCount() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(WriteFixtures.leaf(PrimitiveKind.INT32, null));
        acc.updateInt(7);

        assertThat(acc.finishChunk().nanCount()).isEmpty();
        assertThat(acc.finishPage().nanCount()).isEmpty();
    }

    @Test
    void floatChunkOfOnlyNaNsHasNoBounds() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(FLOAT_LEAF);
        acc.updateFloat(Float.NaN);
        acc.updateFloat(Float.intBitsToFloat(FLOAT_NEGATIVE_NAN));

        Statistics stats = acc.finishChunk();

        assertThat(stats.minValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.min()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.max()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.nanCount()).hasValue(2L);
    }

    @Test
    void floatZeroMinimumIsRecordedAsNegativeZero() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(FLOAT_LEAF);
        acc.updateFloat(0.0f);
        acc.updateFloat(1.0f);

        Statistics stats = acc.finishChunk();

        assertThat(WriteFixtures.floatBits(stats.minValue())).isEqualTo(FLOAT_NEGATIVE_ZERO);
        assertThat(WriteFixtures.floatBits(stats.maxValue())).isEqualTo(Float.floatToRawIntBits(1.0f));
    }

    @Test
    void floatZeroMaximumIsRecordedAsPositiveZero() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(FLOAT_LEAF);
        acc.updateFloat(-0.0f);
        acc.updateFloat(-1.0f);

        Statistics stats = acc.finishChunk();

        assertThat(WriteFixtures.floatBits(stats.minValue())).isEqualTo(Float.floatToRawIntBits(-1.0f));
        assertThat(WriteFixtures.floatBits(stats.maxValue())).isEqualTo(FLOAT_POSITIVE_ZERO);
    }

    @Test
    void floatColumnOfOneZeroIsBoundedByBothZeros() {
        StatisticsAccumulator positiveZero = WriteFixtures.accumulator(FLOAT_LEAF);
        positiveZero.updateFloat(0.0f);
        StatisticsAccumulator negativeZero = WriteFixtures.accumulator(FLOAT_LEAF);
        negativeZero.updateFloat(-0.0f);

        for (Statistics stats : List.of(positiveZero.finishChunk(), negativeZero.finishChunk())) {
            assertThat(WriteFixtures.floatBits(stats.minValue())).isEqualTo(FLOAT_NEGATIVE_ZERO);
            assertThat(WriteFixtures.floatBits(stats.maxValue())).isEqualTo(FLOAT_POSITIVE_ZERO);
        }
    }

    @Test
    void zeroBoundWrittenOverACellOfTheOtherSignIsNotFlaggedExact() {
        StatisticsAccumulator positiveZeroMinimum = WriteFixtures.accumulator(FLOAT_LEAF);
        positiveZeroMinimum.updateFloat(0.0f);
        positiveZeroMinimum.updateFloat(1.0f);
        StatisticsAccumulator negativeZeroMaximum = WriteFixtures.accumulator(FLOAT_LEAF);
        negativeZeroMaximum.updateFloat(-0.0f);
        negativeZeroMaximum.updateFloat(-1.0f);

        Statistics rewrittenMin = positiveZeroMinimum.finishChunk();
        Statistics rewrittenMax = negativeZeroMaximum.finishChunk();

        assertThat(rewrittenMin.isMinValueExact())
                .as("-0.0 written over +0.0 cells")
                .isFalse();
        assertThat(rewrittenMin.isMaxValueExact()).isTrue();
        assertThat(rewrittenMax.isMaxValueExact())
                .as("+0.0 written over -0.0 cells")
                .isFalse();
        assertThat(rewrittenMax.isMinValueExact()).isTrue();
    }

    @Test
    void zeroBoundHeldByACellIsFlaggedExact() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(DOUBLE_LEAF);
        acc.updateDouble(-0.0);
        acc.updateDouble(0.0);

        Statistics stats = acc.finishChunk();

        assertThat(stats.isMinValueExact()).isTrue();
        assertThat(stats.isMaxValueExact()).isTrue();
    }

    @Test
    void pageStatisticsTellWhetherAZeroBoundIsHeldByACell() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(HALF_FLOAT_LEAF);
        acc.updateBinary(WriteFixtures.halfFloat(0.0f));
        acc.updateBinary(WriteFixtures.halfFloat(2.0f));

        PageStatistics page = acc.finishPage();

        assertThat(page.minExact()).as("-0.0 written over a +0.0 cell").isFalse();
        assertThat(page.maxExact()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(FloatColumnOrder.class)
    void floatBoundsMirrorIntoTheDeprecatedFields(FloatColumnOrder order) {
        StatisticsAccumulator floats = WriteFixtures.accumulator(FLOAT_LEAF, order);
        floats.updateFloat(0.0f);
        floats.updateFloat(-2.0f);
        StatisticsAccumulator doubles = WriteFixtures.accumulator(DOUBLE_LEAF, order);
        doubles.updateDouble(1.0);
        doubles.updateDouble(-2.0);

        Statistics floatStats = floats.finishChunk();
        Statistics doubleStats = doubles.finishChunk();

        assertThat(WriteFixtures.floatBits(floatStats.min())).isEqualTo(Float.floatToRawIntBits(-2.0f));
        assertThat(WriteFixtures.floatBits(floatStats.max())).isEqualTo(FLOAT_POSITIVE_ZERO);
        assertThat(WriteFixtures.doubleBits(doubleStats.min())).isEqualTo(Double.doubleToRawLongBits(-2.0));
        assertThat(WriteFixtures.doubleBits(doubleStats.max())).isEqualTo(Double.doubleToRawLongBits(1.0));
    }

    @ParameterizedTest
    @EnumSource(FloatColumnOrder.class)
    void halfFloatBoundsStayOutOfTheDeprecatedFields(FloatColumnOrder order) {
        StatisticsAccumulator halves = WriteFixtures.accumulator(HALF_FLOAT_LEAF, order);
        halves.updateBinary(WriteFixtures.halfFloat(1.0f));

        Statistics stats = halves.finishChunk();

        assertThat(stats.min()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.max()).isEqualTo(MemorySegment.NULL);
    }

    @ParameterizedTest
    @EnumSource(FloatColumnOrder.class)
    void doubleBoundsCoverTheNumbersAndTheNaNsAreCounted(FloatColumnOrder order) {
        StatisticsAccumulator acc = WriteFixtures.accumulator(DOUBLE_LEAF, order);
        acc.updateDouble(Double.NaN);
        acc.updateDouble(2.5);
        acc.updateDouble(Double.longBitsToDouble(DOUBLE_NEGATIVE_NAN));
        acc.updateDouble(Double.NEGATIVE_INFINITY);

        Statistics stats = acc.finishChunk();

        assertThat(WriteFixtures.doubleBits(stats.minValue()))
                .isEqualTo(Double.doubleToRawLongBits(Double.NEGATIVE_INFINITY));
        assertThat(WriteFixtures.doubleBits(stats.maxValue())).isEqualTo(Double.doubleToRawLongBits(2.5));
        assertThat(stats.nanCount()).hasValue(2L);
    }

    @Test
    void doubleZeroBoundsAreRecordedAsNegativeAndPositiveZero() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(DOUBLE_LEAF);
        acc.updateDouble(0.0);

        Statistics stats = acc.finishChunk();

        assertThat(WriteFixtures.doubleBits(stats.minValue())).isEqualTo(DOUBLE_NEGATIVE_ZERO);
        assertThat(WriteFixtures.doubleBits(stats.maxValue())).isEqualTo(DOUBLE_POSITIVE_ZERO);
    }

    @Test
    void doubleChunkOfOnlyNaNsHasNoBounds() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(DOUBLE_LEAF);
        acc.updateDouble(Double.NaN);
        acc.updateDouble(Double.longBitsToDouble(DOUBLE_NEGATIVE_NAN));

        Statistics stats = acc.finishChunk();

        assertThat(stats.minValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.nanCount()).hasValue(2L);
    }

    @ParameterizedTest
    @EnumSource(FloatColumnOrder.class)
    void halfFloatBoundsCoverTheNumbersAndTheNaNsAreCounted(FloatColumnOrder order) {
        StatisticsAccumulator acc = WriteFixtures.accumulator(HALF_FLOAT_LEAF, order);
        acc.updateBinary(WriteFixtures.halfFloat(0.5f));
        acc.updateBinary(WriteFixtures.halfFloatCell(HALF_NAN));
        acc.updateBinary(WriteFixtures.halfFloat(-2.0f));
        acc.updateBinary(WriteFixtures.halfFloatCell(HALF_NEGATIVE_NAN));

        Statistics stats = acc.finishChunk();

        assertThat(WriteFixtures.halfFloatBits(stats.minValue())).isEqualTo(Float.floatToFloat16(-2.0f));
        assertThat(WriteFixtures.halfFloatBits(stats.maxValue())).isEqualTo(Float.floatToFloat16(0.5f));
        assertThat(stats.nanCount()).hasValue(2L);
    }

    @Test
    void halfFloatZeroBoundsAreRecordedAsNegativeAndPositiveZero() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(HALF_FLOAT_LEAF);
        acc.updateBinary(WriteFixtures.halfFloat(0.0f));

        Statistics stats = acc.finishChunk();

        assertThat(WriteFixtures.halfFloatBits(stats.minValue())).isEqualTo(HALF_NEGATIVE_ZERO);
        assertThat(WriteFixtures.halfFloatBits(stats.maxValue())).isEqualTo(HALF_POSITIVE_ZERO);
    }

    @Test
    void halfFloatChunkOfOnlyNaNsHasNoBounds() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(HALF_FLOAT_LEAF);
        acc.updateBinary(WriteFixtures.halfFloatCell(HALF_NAN));
        acc.updateBinary(WriteFixtures.halfFloatCell(HALF_NEGATIVE_NAN));

        Statistics stats = acc.finishChunk();

        assertThat(stats.minValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.nanCount()).hasValue(2L);
    }

    @Test
    void pageOfOnlyNaNsHasNoBoundsAndIsNotANullPage() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(FLOAT_LEAF);
        acc.updateFloat(Float.NaN);
        acc.updateNull();

        PageStatistics page = acc.finishPage();

        assertThat(page.isNullPage()).isFalse();
        assertThat(page.min()).isEqualTo(MemorySegment.NULL);
        assertThat(page.max()).isEqualTo(MemorySegment.NULL);
        assertThat(page.nanCount()).hasValue(1L);
        assertThat(page.nullCount()).isEqualTo(1L);
    }

    @Test
    void mergedNumbersBoundTheChunkAndTheNaNCountsAddUp() {
        StatisticsAccumulator chunk = WriteFixtures.accumulator(FLOAT_LEAF);
        StatisticsAccumulator nanPage = WriteFixtures.accumulator(FLOAT_LEAF);
        StatisticsAccumulator numberPage = WriteFixtures.accumulator(FLOAT_LEAF);
        nanPage.updateFloat(Float.NaN);
        nanPage.updateFloat(Float.intBitsToFloat(FLOAT_NEGATIVE_NAN));
        numberPage.updateFloat(4.0f);
        numberPage.updateFloat(-4.0f);
        numberPage.updateFloat(Float.NaN);

        chunk.merge(nanPage);
        chunk.merge(numberPage);
        Statistics stats = chunk.finishChunk();

        assertThat(WriteFixtures.floatBits(stats.minValue())).isEqualTo(Float.floatToRawIntBits(-4.0f));
        assertThat(WriteFixtures.floatBits(stats.maxValue())).isEqualTo(Float.floatToRawIntBits(4.0f));
        assertThat(stats.nanCount()).hasValue(3L);
    }

    @Test
    void mergedPagesOfOnlyNaNsLeaveTheChunkWithoutBounds() {
        StatisticsAccumulator chunk = WriteFixtures.accumulator(FLOAT_LEAF);
        StatisticsAccumulator first = WriteFixtures.accumulator(FLOAT_LEAF);
        StatisticsAccumulator second = WriteFixtures.accumulator(FLOAT_LEAF);
        first.updateFloat(Float.NaN);
        second.updateFloat(Float.intBitsToFloat(FLOAT_NEGATIVE_NAN));

        chunk.merge(first);
        chunk.merge(second);
        Statistics stats = chunk.finishChunk();

        assertThat(stats.minValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.nanCount()).hasValue(2L);
    }

    @Test
    void mergedZerosOfBothSignsAreBoundedByBothZeros() {
        StatisticsAccumulator chunk = WriteFixtures.accumulator(DOUBLE_LEAF);
        StatisticsAccumulator first = WriteFixtures.accumulator(DOUBLE_LEAF);
        StatisticsAccumulator second = WriteFixtures.accumulator(DOUBLE_LEAF);
        first.updateDouble(0.0);
        second.updateDouble(-0.0);

        chunk.merge(first);
        chunk.merge(second);
        Statistics stats = chunk.finishChunk();

        assertThat(WriteFixtures.doubleBits(stats.minValue())).isEqualTo(DOUBLE_NEGATIVE_ZERO);
        assertThat(WriteFixtures.doubleBits(stats.maxValue())).isEqualTo(DOUBLE_POSITIVE_ZERO);
    }

    @Test
    void resetClearsTheNaNCount() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(FLOAT_LEAF);
        acc.updateFloat(Float.NaN);
        acc.reset();

        assertThat(acc.finishChunk().nanCount()).hasValue(0L);
    }
}
