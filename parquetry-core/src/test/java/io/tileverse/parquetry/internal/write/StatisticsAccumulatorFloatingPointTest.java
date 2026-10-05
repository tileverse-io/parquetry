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

import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.write.page.PageStatistics;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The statistics of FLOAT, DOUBLE and FLOAT16 columns follow the rules of the format for the type-defined order: the
 * bounds are the smallest and largest numbers, a zero bound is recorded as {@code -0.0} for a minimum and as
 * {@code +0.0} for a maximum, the NaN cells are counted and left out of the bounds, and a window holding nothing but
 * NaN has no bounds.
 */
class StatisticsAccumulatorFloatingPointTest {

    private static final SchemaNode.Primitive FLOAT = WriteFixtures.leaf(PrimitiveKind.FLOAT, null);
    private static final SchemaNode.Primitive DOUBLE = WriteFixtures.leaf(PrimitiveKind.DOUBLE, null);
    private static final SchemaNode.Primitive HALF =
            WriteFixtures.leaf(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, new LogicalType.Float16Type());

    private static final int FLOAT_NEGATIVE_NAN = 0xFFC00000;
    private static final long DOUBLE_NEGATIVE_NAN = 0xFFF8000000000000L;
    private static final short HALF_NAN = (short) 0x7E00;
    private static final short HALF_NEGATIVE_NAN = (short) 0xFE00;

    private static final int FLOAT_NEGATIVE_ZERO = Float.floatToRawIntBits(-0.0f);
    private static final int FLOAT_POSITIVE_ZERO = Float.floatToRawIntBits(0.0f);
    private static final long DOUBLE_NEGATIVE_ZERO = Double.doubleToRawLongBits(-0.0);
    private static final long DOUBLE_POSITIVE_ZERO = Double.doubleToRawLongBits(0.0);
    private static final short HALF_NEGATIVE_ZERO = Float.floatToFloat16(-0.0f);
    private static final short HALF_POSITIVE_ZERO = Float.floatToFloat16(0.0f);

    @Test
    void floatBoundsCoverTheNumbersAndTheNaNsAreCounted() {
        StatisticsAccumulator acc = StatisticsAccumulator.forColumn(FLOAT);
        acc.updateFloat(1.5f);
        acc.updateFloat(Float.NaN);
        acc.updateFloat(-2.25f);
        acc.updateFloat(Float.intBitsToFloat(FLOAT_NEGATIVE_NAN));
        acc.updateFloat(3.0f);
        acc.updateNull();

        Statistics stats = acc.finishChunk();

        assertThat(floatBits(stats.minValue())).isEqualTo(Float.floatToRawIntBits(-2.25f));
        assertThat(floatBits(stats.maxValue())).isEqualTo(Float.floatToRawIntBits(3.0f));
        assertThat(stats.nanCount()).hasValue(2L);
        assertThat(stats.nullCount()).hasValue(1L);
    }

    @Test
    void floatColumnWithoutNaNsRecordsANaNCountOfZero() {
        StatisticsAccumulator acc = StatisticsAccumulator.forColumn(FLOAT);
        acc.updateFloat(1.5f);

        assertThat(acc.finishChunk().nanCount()).hasValue(0L);
        assertThat(acc.finishPage().nanCount()).hasValue(0L);
    }

    @Test
    void emptyFloatChunkRecordsANaNCountOfZeroAndNoBounds() {
        Statistics stats = StatisticsAccumulator.forColumn(FLOAT).finishChunk();

        assertThat(stats.nanCount()).hasValue(0L);
        assertThat(stats.minValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).isEqualTo(MemorySegment.NULL);
    }

    @Test
    void halfFloatAnnotationOnAnotherWidthRecordsNoNaNCount() {
        StatisticsAccumulator acc =
                StatisticsAccumulator.forColumn(WriteFixtures.fixedLeaf(4, new LogicalType.Float16Type()));
        acc.updateBinary(MemorySegment.ofArray(new byte[] {0x00, 0x7E, 0x00, 0x7E}));

        assertThat(acc.finishChunk().nanCount()).isEmpty();
    }

    @Test
    void integerColumnRecordsNoNaNCount() {
        StatisticsAccumulator acc = StatisticsAccumulator.forColumn(WriteFixtures.leaf(PrimitiveKind.INT32, null));
        acc.updateInt(7);

        assertThat(acc.finishChunk().nanCount()).isEmpty();
        assertThat(acc.finishPage().nanCount()).isEmpty();
    }

    @Test
    void floatChunkOfOnlyNaNsHasNoBounds() {
        StatisticsAccumulator acc = StatisticsAccumulator.forColumn(FLOAT);
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
        StatisticsAccumulator acc = StatisticsAccumulator.forColumn(FLOAT);
        acc.updateFloat(0.0f);
        acc.updateFloat(1.0f);

        Statistics stats = acc.finishChunk();

        assertThat(floatBits(stats.minValue())).isEqualTo(FLOAT_NEGATIVE_ZERO);
        assertThat(floatBits(stats.maxValue())).isEqualTo(Float.floatToRawIntBits(1.0f));
    }

    @Test
    void floatZeroMaximumIsRecordedAsPositiveZero() {
        StatisticsAccumulator acc = StatisticsAccumulator.forColumn(FLOAT);
        acc.updateFloat(-0.0f);
        acc.updateFloat(-1.0f);

        Statistics stats = acc.finishChunk();

        assertThat(floatBits(stats.minValue())).isEqualTo(Float.floatToRawIntBits(-1.0f));
        assertThat(floatBits(stats.maxValue())).isEqualTo(FLOAT_POSITIVE_ZERO);
    }

    @Test
    void floatColumnOfOneZeroIsBoundedByBothZeros() {
        StatisticsAccumulator positiveZero = StatisticsAccumulator.forColumn(FLOAT);
        positiveZero.updateFloat(0.0f);
        StatisticsAccumulator negativeZero = StatisticsAccumulator.forColumn(FLOAT);
        negativeZero.updateFloat(-0.0f);

        for (Statistics stats : List.of(positiveZero.finishChunk(), negativeZero.finishChunk())) {
            assertThat(floatBits(stats.minValue())).isEqualTo(FLOAT_NEGATIVE_ZERO);
            assertThat(floatBits(stats.maxValue())).isEqualTo(FLOAT_POSITIVE_ZERO);
        }
    }

    @Test
    void zeroBoundWrittenOverACellOfTheOtherSignIsNotFlaggedExact() {
        StatisticsAccumulator positiveZeroMinimum = StatisticsAccumulator.forColumn(FLOAT);
        positiveZeroMinimum.updateFloat(0.0f);
        positiveZeroMinimum.updateFloat(1.0f);
        StatisticsAccumulator negativeZeroMaximum = StatisticsAccumulator.forColumn(FLOAT);
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
        StatisticsAccumulator acc = StatisticsAccumulator.forColumn(DOUBLE);
        acc.updateDouble(-0.0);
        acc.updateDouble(0.0);

        Statistics stats = acc.finishChunk();

        assertThat(stats.isMinValueExact()).isTrue();
        assertThat(stats.isMaxValueExact()).isTrue();
    }

    @Test
    void pageStatisticsTellWhetherAZeroBoundIsHeldByACell() {
        StatisticsAccumulator acc = StatisticsAccumulator.forColumn(HALF);
        acc.updateBinary(WriteFixtures.halfFloat(0.0f));
        acc.updateBinary(WriteFixtures.halfFloat(2.0f));

        PageStatistics page = acc.finishPage();

        assertThat(page.minExact()).as("-0.0 written over a +0.0 cell").isFalse();
        assertThat(page.maxExact()).isTrue();
    }

    @Test
    void floatBoundsMirrorIntoTheDeprecatedFields() {
        StatisticsAccumulator floats = StatisticsAccumulator.forColumn(FLOAT);
        floats.updateFloat(0.0f);
        floats.updateFloat(-2.0f);
        StatisticsAccumulator doubles = StatisticsAccumulator.forColumn(DOUBLE);
        doubles.updateDouble(1.0);
        doubles.updateDouble(-2.0);

        Statistics floatStats = floats.finishChunk();
        Statistics doubleStats = doubles.finishChunk();

        assertThat(floatBits(floatStats.min())).isEqualTo(Float.floatToRawIntBits(-2.0f));
        assertThat(floatBits(floatStats.max())).isEqualTo(FLOAT_POSITIVE_ZERO);
        assertThat(doubleBits(doubleStats.min())).isEqualTo(Double.doubleToRawLongBits(-2.0));
        assertThat(doubleBits(doubleStats.max())).isEqualTo(Double.doubleToRawLongBits(1.0));
    }

    @Test
    void halfFloatBoundsStayOutOfTheDeprecatedFields() {
        StatisticsAccumulator halves = StatisticsAccumulator.forColumn(HALF);
        halves.updateBinary(WriteFixtures.halfFloat(1.0f));

        Statistics stats = halves.finishChunk();

        assertThat(stats.min()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.max()).isEqualTo(MemorySegment.NULL);
    }

    @Test
    void doubleBoundsCoverTheNumbersAndTheNaNsAreCounted() {
        StatisticsAccumulator acc = StatisticsAccumulator.forColumn(DOUBLE);
        acc.updateDouble(Double.NaN);
        acc.updateDouble(2.5);
        acc.updateDouble(Double.longBitsToDouble(DOUBLE_NEGATIVE_NAN));
        acc.updateDouble(Double.NEGATIVE_INFINITY);

        Statistics stats = acc.finishChunk();

        assertThat(doubleBits(stats.minValue())).isEqualTo(Double.doubleToRawLongBits(Double.NEGATIVE_INFINITY));
        assertThat(doubleBits(stats.maxValue())).isEqualTo(Double.doubleToRawLongBits(2.5));
        assertThat(stats.nanCount()).hasValue(2L);
    }

    @Test
    void doubleZeroBoundsAreRecordedAsNegativeAndPositiveZero() {
        StatisticsAccumulator acc = StatisticsAccumulator.forColumn(DOUBLE);
        acc.updateDouble(0.0);

        Statistics stats = acc.finishChunk();

        assertThat(doubleBits(stats.minValue())).isEqualTo(DOUBLE_NEGATIVE_ZERO);
        assertThat(doubleBits(stats.maxValue())).isEqualTo(DOUBLE_POSITIVE_ZERO);
    }

    @Test
    void doubleChunkOfOnlyNaNsHasNoBounds() {
        StatisticsAccumulator acc = StatisticsAccumulator.forColumn(DOUBLE);
        acc.updateDouble(Double.NaN);
        acc.updateDouble(Double.longBitsToDouble(DOUBLE_NEGATIVE_NAN));

        Statistics stats = acc.finishChunk();

        assertThat(stats.minValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.nanCount()).hasValue(2L);
    }

    @Test
    void halfFloatBoundsCoverTheNumbersAndTheNaNsAreCounted() {
        StatisticsAccumulator acc = StatisticsAccumulator.forColumn(HALF);
        acc.updateBinary(WriteFixtures.halfFloat(0.5f));
        acc.updateBinary(halfCell(HALF_NAN));
        acc.updateBinary(WriteFixtures.halfFloat(-2.0f));
        acc.updateBinary(halfCell(HALF_NEGATIVE_NAN));

        Statistics stats = acc.finishChunk();

        assertThat(WriteFixtures.halfFloatBits(stats.minValue())).isEqualTo(Float.floatToFloat16(-2.0f));
        assertThat(WriteFixtures.halfFloatBits(stats.maxValue())).isEqualTo(Float.floatToFloat16(0.5f));
        assertThat(stats.nanCount()).hasValue(2L);
    }

    @Test
    void halfFloatZeroBoundsAreRecordedAsNegativeAndPositiveZero() {
        StatisticsAccumulator acc = StatisticsAccumulator.forColumn(HALF);
        acc.updateBinary(WriteFixtures.halfFloat(0.0f));

        Statistics stats = acc.finishChunk();

        assertThat(WriteFixtures.halfFloatBits(stats.minValue())).isEqualTo(HALF_NEGATIVE_ZERO);
        assertThat(WriteFixtures.halfFloatBits(stats.maxValue())).isEqualTo(HALF_POSITIVE_ZERO);
    }

    @Test
    void halfFloatChunkOfOnlyNaNsHasNoBounds() {
        StatisticsAccumulator acc = StatisticsAccumulator.forColumn(HALF);
        acc.updateBinary(halfCell(HALF_NAN));
        acc.updateBinary(halfCell(HALF_NEGATIVE_NAN));

        Statistics stats = acc.finishChunk();

        assertThat(stats.minValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.nanCount()).hasValue(2L);
    }

    @Test
    void pageOfOnlyNaNsHasNoBoundsAndIsNotANullPage() {
        StatisticsAccumulator acc = StatisticsAccumulator.forColumn(FLOAT);
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
        StatisticsAccumulator chunk = StatisticsAccumulator.forColumn(FLOAT);
        StatisticsAccumulator nanPage = StatisticsAccumulator.forColumn(FLOAT);
        StatisticsAccumulator numberPage = StatisticsAccumulator.forColumn(FLOAT);
        nanPage.updateFloat(Float.NaN);
        nanPage.updateFloat(Float.intBitsToFloat(FLOAT_NEGATIVE_NAN));
        numberPage.updateFloat(4.0f);
        numberPage.updateFloat(-4.0f);
        numberPage.updateFloat(Float.NaN);

        chunk.merge(nanPage);
        chunk.merge(numberPage);
        Statistics stats = chunk.finishChunk();

        assertThat(floatBits(stats.minValue())).isEqualTo(Float.floatToRawIntBits(-4.0f));
        assertThat(floatBits(stats.maxValue())).isEqualTo(Float.floatToRawIntBits(4.0f));
        assertThat(stats.nanCount()).hasValue(3L);
    }

    @Test
    void mergedPagesOfOnlyNaNsLeaveTheChunkWithoutBounds() {
        StatisticsAccumulator chunk = StatisticsAccumulator.forColumn(FLOAT);
        StatisticsAccumulator first = StatisticsAccumulator.forColumn(FLOAT);
        StatisticsAccumulator second = StatisticsAccumulator.forColumn(FLOAT);
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
        StatisticsAccumulator chunk = StatisticsAccumulator.forColumn(DOUBLE);
        StatisticsAccumulator first = StatisticsAccumulator.forColumn(DOUBLE);
        StatisticsAccumulator second = StatisticsAccumulator.forColumn(DOUBLE);
        first.updateDouble(0.0);
        second.updateDouble(-0.0);

        chunk.merge(first);
        chunk.merge(second);
        Statistics stats = chunk.finishChunk();

        assertThat(doubleBits(stats.minValue())).isEqualTo(DOUBLE_NEGATIVE_ZERO);
        assertThat(doubleBits(stats.maxValue())).isEqualTo(DOUBLE_POSITIVE_ZERO);
    }

    @Test
    void resetClearsTheNaNCount() {
        StatisticsAccumulator acc = StatisticsAccumulator.forColumn(FLOAT);
        acc.updateFloat(Float.NaN);
        acc.reset();

        assertThat(acc.finishChunk().nanCount()).hasValue(0L);
    }

    private static int floatBits(MemorySegment bound) {
        return bound.get(INT32, 0);
    }

    private static long doubleBits(MemorySegment bound) {
        return bound.get(INT64, 0);
    }

    /** The cell of a FLOAT16 column holding the half float with the given bits. */
    private static MemorySegment halfCell(short bits) {
        return MemorySegment.ofArray(WriteFixtures.halfFloatBytes(bits));
    }
}
