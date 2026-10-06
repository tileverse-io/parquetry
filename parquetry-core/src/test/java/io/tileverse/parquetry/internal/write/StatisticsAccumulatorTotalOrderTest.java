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
import static io.tileverse.parquetry.internal.write.WriteFixtures.DOUBLE_NAN;
import static io.tileverse.parquetry.internal.write.WriteFixtures.DOUBLE_NAN_WITH_PAYLOAD;
import static io.tileverse.parquetry.internal.write.WriteFixtures.DOUBLE_NEGATIVE_NAN;
import static io.tileverse.parquetry.internal.write.WriteFixtures.FLOAT_LEAF;
import static io.tileverse.parquetry.internal.write.WriteFixtures.FLOAT_NAN;
import static io.tileverse.parquetry.internal.write.WriteFixtures.FLOAT_NAN_WITH_PAYLOAD;
import static io.tileverse.parquetry.internal.write.WriteFixtures.FLOAT_NEGATIVE_NAN;
import static io.tileverse.parquetry.internal.write.WriteFixtures.FLOAT_NEGATIVE_NAN_WITH_PAYLOAD;
import static io.tileverse.parquetry.internal.write.WriteFixtures.HALF_FLOAT_LEAF;
import static io.tileverse.parquetry.internal.write.WriteFixtures.HALF_NAN;
import static io.tileverse.parquetry.internal.write.WriteFixtures.HALF_NAN_WITH_PAYLOAD;
import static io.tileverse.parquetry.internal.write.WriteFixtures.HALF_NEGATIVE_NAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.WriteOptions.FloatColumnOrder;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.write.page.PageStatistics;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The statistics of FLOAT, DOUBLE and FLOAT16 columns of a file declaring IEEE 754 total order follow the rules of the
 * format for that order: the bounds are the smallest and largest numbers with {@code -0.0} before {@code +0.0}, the NaN
 * cells are counted, and a window holding nothing but NaN is bounded by its smallest and largest NaN, sign and payload
 * bits included. {@link StatisticsAccumulatorFloatingPointTest} checks the rules shared with the type-defined order.
 */
class StatisticsAccumulatorTotalOrderTest {

    @Test
    void floatChunkOfOnlyNaNsIsBoundedByItsSmallestAndLargestNaN() {
        StatisticsAccumulator acc = totalOrder(FLOAT_LEAF);
        acc.updateFloat(Float.intBitsToFloat(FLOAT_NAN));
        acc.updateFloat(Float.intBitsToFloat(FLOAT_NEGATIVE_NAN));
        acc.updateFloat(Float.intBitsToFloat(FLOAT_NAN_WITH_PAYLOAD));
        acc.updateFloat(Float.intBitsToFloat(FLOAT_NEGATIVE_NAN_WITH_PAYLOAD));

        Statistics stats = acc.finishChunk();

        assertThat(WriteFixtures.floatBits(stats.minValue())).as("min").isEqualTo(FLOAT_NEGATIVE_NAN_WITH_PAYLOAD);
        assertThat(WriteFixtures.floatBits(stats.maxValue())).as("max").isEqualTo(FLOAT_NAN_WITH_PAYLOAD);
        assertThat(stats.nanCount()).hasValue(4L);
    }

    @Test
    void floatZeroBoundsKeepTheirSign() {
        StatisticsAccumulator positiveZero = totalOrder(FLOAT_LEAF);
        positiveZero.updateFloat(0.0f);
        positiveZero.updateFloat(1.0f);
        StatisticsAccumulator negativeZero = totalOrder(FLOAT_LEAF);
        negativeZero.updateFloat(-0.0f);
        negativeZero.updateFloat(-1.0f);

        Statistics positiveZeroStats = positiveZero.finishChunk();
        Statistics negativeZeroStats = negativeZero.finishChunk();

        assertThat(WriteFixtures.floatBits(positiveZeroStats.minValue())).isEqualTo(Float.floatToRawIntBits(0.0f));
        assertThat(WriteFixtures.floatBits(negativeZeroStats.maxValue())).isEqualTo(Float.floatToRawIntBits(-0.0f));
    }

    @Test
    void boundsAreFlaggedExact() {
        StatisticsAccumulator zeros = totalOrder(FLOAT_LEAF);
        zeros.updateFloat(0.0f);
        StatisticsAccumulator nans = totalOrder(FLOAT_LEAF);
        nans.updateFloat(Float.NaN);

        Statistics zeroStats = zeros.finishChunk();
        PageStatistics nanPage = nans.finishPage();

        assertThat(zeroStats.isMinValueExact())
                .as("a zero minimum keeps the sign of its cell")
                .isTrue();
        assertThat(zeroStats.isMaxValueExact()).isTrue();
        assertThat(nanPage.minExact()).as("a NaN bound is a cell of the page").isTrue();
        assertThat(nanPage.maxExact()).isTrue();
    }

    @Test
    void doubleChunkOfOnlyNaNsIsBoundedByItsSmallestAndLargestNaN() {
        StatisticsAccumulator acc = totalOrder(DOUBLE_LEAF);
        acc.updateDouble(Double.longBitsToDouble(DOUBLE_NAN_WITH_PAYLOAD));
        acc.updateDouble(Double.longBitsToDouble(DOUBLE_NEGATIVE_NAN));
        acc.updateDouble(Double.longBitsToDouble(DOUBLE_NAN));

        Statistics stats = acc.finishChunk();

        assertThat(WriteFixtures.doubleBits(stats.minValue())).as("min").isEqualTo(DOUBLE_NEGATIVE_NAN);
        assertThat(WriteFixtures.doubleBits(stats.maxValue())).as("max").isEqualTo(DOUBLE_NAN_WITH_PAYLOAD);
        assertThat(stats.nanCount()).hasValue(3L);
    }

    @Test
    void halfFloatChunkOfOnlyNaNsIsBoundedByItsSmallestAndLargestNaN() {
        StatisticsAccumulator acc = totalOrder(HALF_FLOAT_LEAF);
        acc.updateBinary(WriteFixtures.halfFloatCell(HALF_NAN));
        acc.updateBinary(WriteFixtures.halfFloatCell(HALF_NEGATIVE_NAN));
        acc.updateBinary(WriteFixtures.halfFloatCell(HALF_NAN_WITH_PAYLOAD));

        Statistics stats = acc.finishChunk();

        assertThat(WriteFixtures.halfFloatBits(stats.minValue())).as("min").isEqualTo(HALF_NEGATIVE_NAN);
        assertThat(WriteFixtures.halfFloatBits(stats.maxValue())).as("max").isEqualTo(HALF_NAN_WITH_PAYLOAD);
        assertThat(stats.nanCount()).hasValue(3L);
    }

    @Test
    void pageOfOnlyNaNsIsBoundedAndIsNotANullPage() {
        StatisticsAccumulator acc = totalOrder(FLOAT_LEAF);
        acc.updateFloat(Float.intBitsToFloat(FLOAT_NAN));
        acc.updateNull();

        PageStatistics page = acc.finishPage();

        assertThat(page.isNullPage()).isFalse();
        assertThat(WriteFixtures.floatBits(page.min())).isEqualTo(FLOAT_NAN);
        assertThat(WriteFixtures.floatBits(page.max())).isEqualTo(FLOAT_NAN);
        assertThat(page.nanCount()).hasValue(1L);
        assertThat(page.nullCount()).isEqualTo(1L);
    }

    @Test
    void mergedNumbersTakeTheBoundsOverMergedNaNs() {
        StatisticsAccumulator chunk = totalOrder(FLOAT_LEAF);
        StatisticsAccumulator nanPage = totalOrder(FLOAT_LEAF);
        StatisticsAccumulator numberPage = totalOrder(FLOAT_LEAF);
        nanPage.updateFloat(Float.intBitsToFloat(FLOAT_NAN_WITH_PAYLOAD));
        nanPage.updateFloat(Float.intBitsToFloat(FLOAT_NEGATIVE_NAN));
        numberPage.updateFloat(4.0f);
        numberPage.updateFloat(-4.0f);

        chunk.merge(nanPage);
        chunk.merge(numberPage);
        Statistics stats = chunk.finishChunk();

        assertThat(WriteFixtures.floatBits(stats.minValue())).isEqualTo(Float.floatToRawIntBits(-4.0f));
        assertThat(WriteFixtures.floatBits(stats.maxValue())).isEqualTo(Float.floatToRawIntBits(4.0f));
        assertThat(stats.nanCount()).hasValue(2L);
    }

    @Test
    void mergedPagesOfOnlyNaNsKeepTheSmallestAndLargestNaN() {
        StatisticsAccumulator chunk = totalOrder(FLOAT_LEAF);
        StatisticsAccumulator pageWithTheSmallest = totalOrder(FLOAT_LEAF);
        StatisticsAccumulator pageWithTheLargest = totalOrder(FLOAT_LEAF);
        pageWithTheSmallest.updateFloat(Float.intBitsToFloat(FLOAT_NEGATIVE_NAN));
        pageWithTheSmallest.updateFloat(Float.intBitsToFloat(FLOAT_NAN));
        pageWithTheLargest.updateFloat(Float.intBitsToFloat(FLOAT_NAN_WITH_PAYLOAD));

        chunk.merge(pageWithTheSmallest);
        chunk.merge(pageWithTheLargest);
        Statistics stats = chunk.finishChunk();

        assertThat(WriteFixtures.floatBits(stats.minValue())).as("min").isEqualTo(FLOAT_NEGATIVE_NAN);
        assertThat(WriteFixtures.floatBits(stats.maxValue())).as("max").isEqualTo(FLOAT_NAN_WITH_PAYLOAD);
        assertThat(stats.nanCount()).hasValue(3L);
    }

    @Test
    void resetClearsTheNaNCountAndTheNaNBounds() {
        StatisticsAccumulator acc = totalOrder(FLOAT_LEAF);
        acc.updateFloat(Float.NaN);
        acc.reset();

        Statistics stats = acc.finishChunk();

        assertThat(stats.nanCount()).hasValue(0L);
        assertThat(stats.minValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).isEqualTo(MemorySegment.NULL);
    }

    @Test
    void floatColumnAnnotatedUnknownKeepsTheTypeDefinedRules() {
        SchemaNode.Primitive unknown = WriteFixtures.leaf(PrimitiveKind.FLOAT, new LogicalType.UnknownType());
        StatisticsAccumulator acc = totalOrder(unknown);
        acc.updateFloat(Float.NaN);
        acc.updateFloat(Float.NaN);

        Statistics stats = acc.finishChunk();

        assertThat(stats.minValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.nanCount()).hasValue(2L);
    }

    private static StatisticsAccumulator totalOrder(SchemaNode.Primitive leaf) {
        return WriteFixtures.accumulator(leaf, FloatColumnOrder.IEEE_754_TOTAL_ORDER);
    }
}
