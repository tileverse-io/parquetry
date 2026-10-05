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
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.math.BigInteger;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * The min and max of a column follow the order defined by the format for the column's type: unsigned for the unsigned
 * integer annotations, the signed value of a binary decimal, the represented value of a half float. A reader pruning by
 * bounds in another order drops rows held by the chunk.
 */
class StatisticsAccumulatorOrderTest {

    private static final LogicalType UINT_32 = new LogicalType.IntType((byte) 32, false);
    private static final LogicalType UINT_64 = new LogicalType.IntType((byte) 64, false);
    private static final LogicalType UINT_8 = new LogicalType.IntType((byte) 8, false);
    private static final LogicalType DECIMAL = new LogicalType.Decimal(2, 20);
    private static final LogicalType FLOAT16 = new LogicalType.Float16Type();
    private static final int FIXED_DECIMAL_BYTES = 9;

    @Test
    void unsigned32BitBoundsFollowUnsignedOrder() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(PrimitiveKind.INT32, UINT_32);
        acc.updateInt(5);
        acc.updateInt(unsignedInt(4_000_000_000L));
        acc.updateInt(1);
        acc.updateInt(unsignedInt(2_147_483_648L));

        Statistics stats = acc.finishChunk();

        assertThat(Integer.toUnsignedLong(stats.minValue().get(INT32, 0)))
                .as("min")
                .isEqualTo(1L);
        assertThat(Integer.toUnsignedLong(stats.maxValue().get(INT32, 0)))
                .as("max")
                .isEqualTo(4_000_000_000L);
    }

    @Test
    void unsigned64BitBoundsFollowUnsignedOrder() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(PrimitiveKind.INT64, UINT_64);
        long aboveSignedRange = Long.parseUnsignedLong("18000000000000000000");
        acc.updateLong(7L);
        acc.updateLong(aboveSignedRange);
        acc.updateLong(3L);
        acc.updateLong(Long.MIN_VALUE);

        Statistics stats = acc.finishChunk();

        assertThat(stats.minValue().get(INT64, 0)).as("min").isEqualTo(3L);
        assertThat(stats.maxValue().get(INT64, 0)).as("max").isEqualTo(aboveSignedRange);
    }

    @Test
    void narrowUnsignedBoundsFollowUnsignedOrder() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(PrimitiveKind.INT32, UINT_8);
        acc.updateInt(200);
        acc.updateInt(0);
        acc.updateInt(255);

        Statistics stats = acc.finishChunk();

        assertThat(stats.minValue().get(INT32, 0)).as("min").isZero();
        assertThat(stats.maxValue().get(INT32, 0)).as("max").isEqualTo(255);
    }

    @Test
    void mergedUnsignedBoundsFollowUnsignedOrder() {
        StatisticsAccumulator chunk = WriteFixtures.accumulator(PrimitiveKind.INT32, UINT_32);
        StatisticsAccumulator page = WriteFixtures.accumulator(PrimitiveKind.INT32, UINT_32);
        chunk.updateInt(9);
        chunk.updateInt(10);
        page.updateInt(unsignedInt(3_000_000_000L));
        page.updateInt(2);

        chunk.merge(page);
        Statistics stats = chunk.finishChunk();

        assertThat(Integer.toUnsignedLong(stats.minValue().get(INT32, 0)))
                .as("min")
                .isEqualTo(2L);
        assertThat(Integer.toUnsignedLong(stats.maxValue().get(INT32, 0)))
                .as("max")
                .isEqualTo(3_000_000_000L);
    }

    @Test
    void fixedLengthDecimalBoundsFollowTheSignedValue() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, DECIMAL);
        acc.updateBinary(fixedDecimal(300));
        acc.updateBinary(fixedDecimal(-499_508));
        acc.updateBinary(fixedDecimal(499_107));
        acc.updateBinary(fixedDecimal(-1));

        Statistics stats = acc.finishChunk();

        assertThat(unscaled(stats.minValue())).as("min").isEqualTo(-499_508L);
        assertThat(unscaled(stats.maxValue())).as("max").isEqualTo(499_107L);
    }

    @Test
    void variableLengthDecimalBoundsFollowTheSignedValue() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(PrimitiveKind.BYTE_ARRAY, DECIMAL);
        acc.updateBinary(minimalDecimal(127));
        acc.updateBinary(minimalDecimal(128));
        acc.updateBinary(minimalDecimal(-128));
        acc.updateBinary(minimalDecimal(-129));
        acc.updateBinary(minimalDecimal(-1));

        Statistics stats = acc.finishChunk();

        assertThat(unscaled(stats.minValue())).as("min").isEqualTo(-129L);
        assertThat(unscaled(stats.maxValue())).as("max").isEqualTo(128L);
    }

    @Test
    void variableLengthDecimalsOfEqualValueAndUnequalLengthKeepTheFirstBound() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(PrimitiveKind.BYTE_ARRAY, DECIMAL);
        MemorySegment minimal = asSegment(new byte[] {(byte) 0xFF});
        MemorySegment padded = asSegment(new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        acc.updateBinary(minimal);
        acc.updateBinary(padded);

        Statistics stats = acc.finishChunk();

        assertThat(stats.minValue().toArray(JAVA_BYTE)).as("min").isEqualTo(minimal.toArray(JAVA_BYTE));
        assertThat(stats.maxValue().toArray(JAVA_BYTE)).as("max").isEqualTo(minimal.toArray(JAVA_BYTE));
    }

    @Test
    void mergedDecimalBoundsFollowTheSignedValue() {
        StatisticsAccumulator chunk = WriteFixtures.accumulator(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, DECIMAL);
        StatisticsAccumulator page = WriteFixtures.accumulator(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, DECIMAL);
        chunk.updateBinary(fixedDecimal(5));
        chunk.updateBinary(fixedDecimal(3));
        page.updateBinary(fixedDecimal(-7));
        page.updateBinary(fixedDecimal(4));

        chunk.merge(page);
        Statistics stats = chunk.finishChunk();

        assertThat(unscaled(stats.minValue())).as("min").isEqualTo(-7L);
        assertThat(unscaled(stats.maxValue())).as("max").isEqualTo(5L);
    }

    @Test
    void halfFloatBoundsFollowTheRepresentedValue() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, FLOAT16);
        acc.updateBinary(WriteFixtures.halfFloat(0.5f));
        acc.updateBinary(WriteFixtures.halfFloat(-2.0f));
        acc.updateBinary(WriteFixtures.halfFloat(3.0f));
        acc.updateBinary(WriteFixtures.halfFloat(-95.5f));
        acc.updateBinary(WriteFixtures.halfFloat(12.0f));

        Statistics stats = acc.finishChunk();

        assertThat(halfFloatValue(stats.minValue())).as("min").isEqualTo(-95.5f);
        assertThat(halfFloatValue(stats.maxValue())).as("max").isEqualTo(12.0f);
    }

    @Test
    void mergedHalfFloatBoundsFollowTheRepresentedValue() {
        StatisticsAccumulator chunk = WriteFixtures.accumulator(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, FLOAT16);
        StatisticsAccumulator page = WriteFixtures.accumulator(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, FLOAT16);
        chunk.updateBinary(WriteFixtures.halfFloat(1.0f));
        page.updateBinary(WriteFixtures.halfFloat(-1.0f));
        page.updateBinary(WriteFixtures.halfFloat(0.25f));

        chunk.merge(page);
        Statistics stats = chunk.finishChunk();

        assertThat(halfFloatValue(stats.minValue())).as("min").isEqualTo(-1.0f);
        assertThat(halfFloatValue(stats.maxValue())).as("max").isEqualTo(1.0f);
    }

    @Test
    void timestampOnFixedLengthBytesHasNoBounds() {
        LogicalType timestamp = new LogicalType.Timestamp(true, LogicalType.TimeUnit.MILLIS);
        StatisticsAccumulator acc = WriteFixtures.accumulator(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, timestamp);
        acc.updateBinary(asSegment(new byte[12]));
        acc.updateNull();

        Statistics stats = acc.finishChunk();

        assertThat(stats.minValue()).as("minValue").isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).as("maxValue").isEqualTo(MemorySegment.NULL);
        assertThat(stats.nullCount()).as("nullCount").hasValue(1L);
    }

    @Test
    void annotationUndefinedForThePhysicalTypeHasNoBounds() {
        StatisticsAccumulator acc = WriteFixtures.accumulator(PrimitiveKind.INT32, new LogicalType.UuidType());
        acc.updateInt(7);
        acc.updateInt(3);

        Statistics stats = acc.finishChunk();

        assertThat(stats.minValue()).as("minValue").isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).as("maxValue").isEqualTo(MemorySegment.NULL);
        assertThat(stats.nullCount()).as("nullCount").hasValue(0L);
    }

    @Test
    void mergeRejectsAccumulatorsInDifferentOrders() {
        StatisticsAccumulator signed = WriteFixtures.accumulator(PrimitiveKind.INT32, null);
        StatisticsAccumulator unsigned = WriteFixtures.accumulator(PrimitiveKind.INT32, UINT_32);

        assertThatThrownBy(() -> signed.merge(unsigned))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("different orders");
    }

    private static int unsignedInt(long value) {
        return (int) value;
    }

    /** A decimal's unscaled value as {@code length} big-endian two's complement bytes. */
    private static MemorySegment fixedDecimal(long unscaled) {
        return asSegment(WriteFixtures.signedBytes(BigInteger.valueOf(unscaled), FIXED_DECIMAL_BYTES));
    }

    /** A decimal's unscaled value in the fewest big-endian two's complement bytes. */
    private static MemorySegment minimalDecimal(long unscaled) {
        return asSegment(BigInteger.valueOf(unscaled).toByteArray());
    }

    private static long unscaled(MemorySegment bound) {
        return new BigInteger(bound.toArray(JAVA_BYTE)).longValueExact();
    }

    private static float halfFloatValue(MemorySegment bound) {
        return Float.float16ToFloat(WriteFixtures.halfFloatBits(bound));
    }

    private static MemorySegment asSegment(byte[] bytes) {
        return MemorySegment.ofArray(bytes).asReadOnly();
    }
}
