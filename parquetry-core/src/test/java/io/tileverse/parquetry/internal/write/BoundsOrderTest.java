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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.WriteOptions.FloatColumnOrder;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.LogicalType.TimeUnit;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

class BoundsOrderTest {

    private static final LogicalType DECIMAL = new LogicalType.Decimal(2, 9);
    private static final LogicalType FLOAT16 = new LogicalType.Float16Type();

    static Stream<Arguments> definedOrders() {
        return Stream.of(
                Arguments.of(PrimitiveKind.BOOLEAN, null, BoundsOrder.BOOLEAN),
                Arguments.of(PrimitiveKind.INT32, null, BoundsOrder.SIGNED_INT32),
                Arguments.of(PrimitiveKind.INT64, null, BoundsOrder.SIGNED_INT64),
                Arguments.of(PrimitiveKind.FLOAT, null, BoundsOrder.FLOAT),
                Arguments.of(PrimitiveKind.DOUBLE, null, BoundsOrder.DOUBLE),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, null, BoundsOrder.UNSIGNED_BYTES),
                Arguments.of(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, null, BoundsOrder.UNSIGNED_BYTES),
                Arguments.of(PrimitiveKind.INT32, integer(8, true), BoundsOrder.SIGNED_INT32),
                Arguments.of(PrimitiveKind.INT32, integer(8, false), BoundsOrder.UNSIGNED_INT32),
                Arguments.of(PrimitiveKind.INT32, integer(16, false), BoundsOrder.UNSIGNED_INT32),
                Arguments.of(PrimitiveKind.INT32, integer(32, false), BoundsOrder.UNSIGNED_INT32),
                Arguments.of(PrimitiveKind.INT64, integer(64, true), BoundsOrder.SIGNED_INT64),
                Arguments.of(PrimitiveKind.INT64, integer(64, false), BoundsOrder.UNSIGNED_INT64),
                Arguments.of(PrimitiveKind.INT32, DECIMAL, BoundsOrder.SIGNED_INT32),
                Arguments.of(PrimitiveKind.INT64, DECIMAL, BoundsOrder.SIGNED_INT64),
                Arguments.of(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, DECIMAL, BoundsOrder.SIGNED_BYTES),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, DECIMAL, BoundsOrder.SIGNED_BYTES),
                Arguments.of(PrimitiveKind.INT32, new LogicalType.DateType(), BoundsOrder.SIGNED_INT32),
                Arguments.of(
                        PrimitiveKind.INT32, new LogicalType.Time(true, TimeUnit.MILLIS), BoundsOrder.SIGNED_INT32),
                Arguments.of(
                        PrimitiveKind.INT64, new LogicalType.Time(true, TimeUnit.MICROS), BoundsOrder.SIGNED_INT64),
                Arguments.of(
                        PrimitiveKind.INT64, new LogicalType.Timestamp(true, TimeUnit.NANOS), BoundsOrder.SIGNED_INT64),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, new LogicalType.StringType(), BoundsOrder.UNSIGNED_BYTES),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, new LogicalType.EnumType(), BoundsOrder.UNSIGNED_BYTES),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, new LogicalType.JsonType(), BoundsOrder.UNSIGNED_BYTES),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, new LogicalType.BsonType(), BoundsOrder.UNSIGNED_BYTES),
                Arguments.of(
                        PrimitiveKind.FIXED_LEN_BYTE_ARRAY, new LogicalType.UuidType(), BoundsOrder.UNSIGNED_BYTES),
                Arguments.of(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, FLOAT16, BoundsOrder.HALF_FLOAT),
                Arguments.of(PrimitiveKind.INT32, new LogicalType.UnknownType(), BoundsOrder.SIGNED_INT32),
                Arguments.of(PrimitiveKind.FLOAT, new LogicalType.UnknownType(), BoundsOrder.FLOAT));
    }

    @ParameterizedTest(name = "{0} annotated {1} orders as {2}")
    @MethodSource("definedOrders")
    void theOrderFollowsThePhysicalAndTheLogicalType(
            PrimitiveKind kind, LogicalType logicalType, BoundsOrder expected) {
        assertThat(BoundsOrder.of(WriteFixtures.leaf(kind, logicalType))).isEqualTo(expected);
    }

    static Stream<Arguments> undefinedOrders() {
        return Stream.of(
                Arguments.of(PrimitiveKind.INT96, null),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, new LogicalType.Geometry(Optional.empty())),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, new LogicalType.Geography(Optional.empty(), Optional.empty())),
                Arguments.of(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, new LogicalType.Timestamp(true, TimeUnit.MILLIS)),
                Arguments.of(PrimitiveKind.INT32, new LogicalType.UuidType()),
                Arguments.of(PrimitiveKind.INT64, new LogicalType.StringType()),
                Arguments.of(PrimitiveKind.INT64, new LogicalType.DateType()),
                Arguments.of(PrimitiveKind.DOUBLE, DECIMAL),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, integer(32, false)),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, FLOAT16),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, new LogicalType.ListType()),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, new LogicalType.MapType()),
                Arguments.of(PrimitiveKind.BYTE_ARRAY, new LogicalType.Variant()));
    }

    @ParameterizedTest(name = "{0} annotated {1} has no order")
    @MethodSource("undefinedOrders")
    void aTypeWithoutADefinedOrderGetsNoBounds(PrimitiveKind kind, LogicalType logicalType) {
        BoundsOrder order = BoundsOrder.of(WriteFixtures.leaf(kind, logicalType));

        assertThat(order).isEqualTo(BoundsOrder.UNDEFINED);
        assertThat(order.isDefined()).isFalse();
    }

    @Test
    void aHalfFloatAnnotationOnAnotherWidthHasNoOrder() {
        assertThat(BoundsOrder.of(WriteFixtures.fixedLeaf(4, FLOAT16))).isEqualTo(BoundsOrder.UNDEFINED);
        assertThat(BoundsOrder.of(WriteFixtures.fixedLeaf(2, FLOAT16))).isEqualTo(BoundsOrder.HALF_FLOAT);
    }

    static Stream<Arguments> floatingPointColumns() {
        return Stream.of(
                Arguments.of(WriteFixtures.leaf(PrimitiveKind.FLOAT, null), BoundsOrder.FLOAT_TOTAL_ORDER),
                Arguments.of(WriteFixtures.leaf(PrimitiveKind.DOUBLE, null), BoundsOrder.DOUBLE_TOTAL_ORDER),
                Arguments.of(
                        WriteFixtures.leaf(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, FLOAT16),
                        BoundsOrder.HALF_FLOAT_TOTAL_ORDER));
    }

    @ParameterizedTest(name = "{0} orders as {1}")
    @MethodSource("floatingPointColumns")
    void aFloatingPointColumnTakesTotalOrderInAFileDeclaringIt(SchemaNode.Primitive leaf, BoundsOrder expected) {
        assertThat(BoundsOrder.of(leaf, FloatColumnOrder.IEEE_754_TOTAL_ORDER)).isEqualTo(expected);
    }

    @ParameterizedTest(name = "{0} does not order as {1}")
    @MethodSource("floatingPointColumns")
    void aFloatingPointColumnKeepsItsOrderInAFileDeclaringTheTypeDefinedOrder(
            SchemaNode.Primitive leaf, BoundsOrder totalOrder) {
        BoundsOrder order = BoundsOrder.of(leaf, FloatColumnOrder.TYPE_DEFINED);

        assertThat(order).isEqualTo(BoundsOrder.of(leaf)).isNotEqualTo(totalOrder);
    }

    static Stream<Arguments> columnsOutsideTotalOrder() {
        return Stream.of(
                Arguments.of(WriteFixtures.leaf(PrimitiveKind.INT32, null)),
                Arguments.of(WriteFixtures.leaf(PrimitiveKind.BYTE_ARRAY, new LogicalType.StringType())),
                Arguments.of(WriteFixtures.leaf(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, DECIMAL)),
                Arguments.of(WriteFixtures.leaf(PrimitiveKind.FLOAT, new LogicalType.UnknownType())),
                Arguments.of(WriteFixtures.leaf(PrimitiveKind.DOUBLE, new LogicalType.UnknownType())),
                Arguments.of(WriteFixtures.fixedLeaf(4, FLOAT16)),
                Arguments.of(WriteFixtures.leaf(PrimitiveKind.DOUBLE, DECIMAL)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("columnsOutsideTotalOrder")
    void totalOrderLeavesTheOtherColumnsInTheirTypeDefinedOrder(SchemaNode.Primitive leaf) {
        BoundsOrder order = BoundsOrder.of(leaf, FloatColumnOrder.IEEE_754_TOTAL_ORDER);

        assertThat(order).isEqualTo(BoundsOrder.of(leaf));
        assertThat(order.isTotalOrder()).isFalse();
    }

    @Test
    void floatingPointOrdersCountNaNCellsInEitherOrder() {
        List<BoundsOrder> floatingPoint = List.of(
                BoundsOrder.FLOAT,
                BoundsOrder.DOUBLE,
                BoundsOrder.HALF_FLOAT,
                BoundsOrder.FLOAT_TOTAL_ORDER,
                BoundsOrder.DOUBLE_TOTAL_ORDER,
                BoundsOrder.HALF_FLOAT_TOTAL_ORDER);

        for (BoundsOrder order : BoundsOrder.values()) {
            assertThat(order.isFloatingPoint())
                    .as("%s counts NaN cells", order)
                    .isEqualTo(floatingPoint.contains(order));
        }
    }

    @Test
    void onlyTheTotalOrderConstantsReportTotalOrder() {
        List<BoundsOrder> totalOrder = List.of(
                BoundsOrder.FLOAT_TOTAL_ORDER, BoundsOrder.DOUBLE_TOTAL_ORDER, BoundsOrder.HALF_FLOAT_TOTAL_ORDER);

        for (BoundsOrder order : BoundsOrder.values()) {
            assertThat(order.isTotalOrder()).as("%s is a total order", order).isEqualTo(totalOrder.contains(order));
        }
    }

    @Test
    void totalOrderPutsNaNCellsBeyondTheInfinitiesBySignAndPayload() {
        MemorySegment negativeNaN = int32(0xFFC00000);
        MemorySegment nan = int32(0x7FC00000);
        MemorySegment nanWithPayload = int32(0x7FC00001);
        MemorySegment halfPositiveInfinity = bytes(0x00, 0x7C);
        MemorySegment halfNaN = bytes(0x00, 0x7E);

        assertThat(BoundsOrder.FLOAT_TOTAL_ORDER.compare(negativeNaN, float32(Float.NEGATIVE_INFINITY)))
                .as("a negative NaN before negative infinity")
                .isNegative();
        assertThat(BoundsOrder.FLOAT_TOTAL_ORDER.compare(float32(Float.POSITIVE_INFINITY), nan))
                .as("positive infinity before a positive NaN")
                .isNegative();
        assertThat(BoundsOrder.FLOAT_TOTAL_ORDER.compare(nan, nanWithPayload))
                .as("a positive NaN before one with a larger payload")
                .isNegative();
        assertThat(BoundsOrder.HALF_FLOAT_TOTAL_ORDER.compare(halfPositiveInfinity, halfNaN))
                .as("half-float positive infinity before a positive NaN")
                .isNegative();
    }

    @Test
    void totalOrderPutsNegativeZeroBeforePositiveZero() {
        assertThat(BoundsOrder.DOUBLE_TOTAL_ORDER.compare(float64(-0.0), float64(0.0)))
                .isNegative();
    }

    @Test
    void unsignedIntegersCompareAboveTheSignedRange() {
        MemorySegment small = int32(7);
        MemorySegment aboveSignedRange = int32((int) 3_000_000_000L);

        assertThat(BoundsOrder.UNSIGNED_INT32.compare(small, aboveSignedRange)).isNegative();
        assertThat(BoundsOrder.SIGNED_INT32.compare(small, aboveSignedRange)).isPositive();
        assertThat(BoundsOrder.UNSIGNED_INT64.compare(int64(7L), int64(Long.MIN_VALUE)))
                .isNegative();
        assertThat(BoundsOrder.SIGNED_INT64.compare(int64(7L), int64(Long.MIN_VALUE)))
                .isPositive();
    }

    @Test
    void signedBytesCompareAsTheNumbersTheyHold() {
        MemorySegment minusOne = bytes(0xFF, 0xFF);
        MemorySegment five = bytes(0x00, 0x05);

        assertThat(BoundsOrder.SIGNED_BYTES.compare(minusOne, five)).isNegative();
        assertThat(BoundsOrder.UNSIGNED_BYTES.compare(minusOne, five)).isPositive();
    }

    @Test
    void halfFloatsCompareByValue() {
        MemorySegment minusTwo = bytes(0x00, 0xC0);
        MemorySegment half = bytes(0x00, 0x38);

        assertThat(BoundsOrder.HALF_FLOAT.compare(minusTwo, half)).isNegative();
        assertThat(BoundsOrder.UNSIGNED_BYTES.compare(minusTwo, half)).isPositive();
    }

    @Test
    void floatingPointBoundsCompareByTheNumbersTheyHold() {
        assertThat(BoundsOrder.FLOAT.compare(float32(-2.0f), float32(-1.0f))).isNegative();
        assertThat(BoundsOrder.FLOAT.compare(float32(Float.NEGATIVE_INFINITY), float32(-Float.MAX_VALUE)))
                .isNegative();
        assertThat(BoundsOrder.FLOAT.compare(float32(-0.0f), float32(0.0f)))
                .as("a zero minimum before a zero maximum")
                .isNegative();
        assertThat(BoundsOrder.DOUBLE.compare(float64(1.5), float64(1.5))).isZero();
        assertThat(BoundsOrder.DOUBLE.compare(float64(Double.POSITIVE_INFINITY), float64(Double.MAX_VALUE)))
                .isPositive();
    }

    @Test
    void booleanBoundsPutFalseBeforeTrue() {
        assertThat(BoundsOrder.BOOLEAN.compare(bytes(0), bytes(1))).isNegative();
        assertThat(BoundsOrder.BOOLEAN.compare(bytes(1), bytes(1))).isZero();
    }

    @Test
    void anUndefinedOrderComparesNothing() {
        MemorySegment bound = bytes(0x01);

        assertThatThrownBy(() -> BoundsOrder.UNDEFINED.compare(bound, bound)).isInstanceOf(IllegalStateException.class);
    }

    private static LogicalType integer(int bitWidth, boolean signed) {
        return new LogicalType.IntType((byte) bitWidth, signed);
    }

    private static MemorySegment int32(int value) {
        MemorySegment segment = MemorySegment.ofArray(new byte[Integer.BYTES]);
        segment.set(INT32, 0, value);
        return segment;
    }

    private static MemorySegment float32(float value) {
        return int32(Float.floatToRawIntBits(value));
    }

    private static MemorySegment float64(double value) {
        return int64(Double.doubleToRawLongBits(value));
    }

    private static MemorySegment int64(long value) {
        MemorySegment segment = MemorySegment.ofArray(new byte[Long.BYTES]);
        segment.set(INT64, 0, value);
        return segment;
    }

    private static MemorySegment bytes(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return MemorySegment.ofArray(bytes);
    }
}
