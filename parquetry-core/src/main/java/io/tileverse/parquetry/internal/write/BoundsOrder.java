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

import java.lang.foreign.MemorySegment;
import java.util.Optional;

import io.tileverse.parquetry.data.WriteOptions.FloatColumnOrder;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The order of a column's min and max statistics and of the page bounds of its column index, as defined by the format
 * for the column's physical and logical type and for the column order declared in the footer. A reader prunes by those
 * bounds in that order, and bounds computed in another order make it drop rows held by the column.
 */
enum BoundsOrder {

    /** {@code false} before {@code true}. */
    BOOLEAN,

    /** Signed 32-bit integers: plain INT32 and its signed integer, date, time and decimal annotations. */
    SIGNED_INT32,

    /** INT32 cells read as unsigned integers. */
    UNSIGNED_INT32,

    /** Signed 64-bit integers: plain INT64 and its signed integer, time, timestamp and decimal annotations. */
    SIGNED_INT64,

    /** INT64 cells read as unsigned integers. */
    UNSIGNED_INT64,

    /** FLOAT cells by their value, {@code -0.0} before {@code +0.0}; NaN cells are left out of the bounds. */
    FLOAT,

    /** DOUBLE cells by their value, {@code -0.0} before {@code +0.0}; NaN cells are left out of the bounds. */
    DOUBLE,

    /**
     * FLOAT16 cells by the value of the half float in their two little-endian bytes, {@code -0.0} before {@code +0.0};
     * NaN cells are left out of the bounds.
     */
    HALF_FLOAT,

    /**
     * FLOAT cells in IEEE 754 total order: the negative NaNs, the numbers with {@code -0.0} before {@code +0.0}, the
     * positive NaNs. The bounds are numbers, and NaN cells bound a chunk or page holding no number.
     */
    FLOAT_TOTAL_ORDER,

    /** DOUBLE cells in IEEE 754 total order; see {@link #FLOAT_TOTAL_ORDER}. */
    DOUBLE_TOTAL_ORDER,

    /**
     * The half floats in the two little-endian bytes of FLOAT16 cells, in IEEE 754 total order; see
     * {@link #FLOAT_TOTAL_ORDER}.
     */
    HALF_FLOAT_TOTAL_ORDER,

    /** Bytes compared unsigned, left to right, a prefix before its extensions: strings, UUIDs, plain binary. */
    UNSIGNED_BYTES,

    /** The signed number held by the big-endian two's complement bytes of a binary decimal. */
    SIGNED_BYTES,

    /**
     * No order: the format defines none for the type, or the logical type is not defined for the physical one. The
     * column gets no min, no max and no page bounds.
     */
    UNDEFINED;

    /**
     * The order of the statistics of {@code leaf} in a file declaring {@code floatOrder} for its floating-point
     * columns. IEEE 754 total order applies to the FLOAT and DOUBLE columns without annotation and to the FLOAT16
     * columns. The other columns keep their type-defined order. A FLOAT or DOUBLE column annotated UNKNOWN keeps it
     * too, because the parquet-java reader rejects a footer declaring total order for such a column.
     */
    static BoundsOrder of(SchemaNode.Primitive leaf, FloatColumnOrder floatOrder) {
        BoundsOrder typeDefined = of(leaf);
        if (floatOrder == FloatColumnOrder.TYPE_DEFINED || isAnnotatedUnknown(leaf)) {
            return typeDefined;
        }
        return typeDefined.inTotalOrder();
    }

    /** The order of the statistics of {@code leaf} in a file declaring the type-defined order for its columns. */
    static BoundsOrder of(SchemaNode.Primitive leaf) {
        Optional<LogicalType> logicalType = leaf.logicalType();
        if (logicalType.isEmpty()) {
            return physicalOrder(leaf.kind());
        }
        return annotatedOrder(leaf, logicalType.orElseThrow());
    }

    private static boolean isAnnotatedUnknown(SchemaNode.Primitive leaf) {
        return leaf.logicalType().orElse(null) instanceof LogicalType.UnknownType;
    }

    private BoundsOrder inTotalOrder() {
        return switch (this) {
            case FLOAT -> FLOAT_TOTAL_ORDER;
            case DOUBLE -> DOUBLE_TOTAL_ORDER;
            case HALF_FLOAT -> HALF_FLOAT_TOTAL_ORDER;
            case BOOLEAN,
                    SIGNED_INT32,
                    UNSIGNED_INT32,
                    SIGNED_INT64,
                    UNSIGNED_INT64,
                    FLOAT_TOTAL_ORDER,
                    DOUBLE_TOTAL_ORDER,
                    HALF_FLOAT_TOTAL_ORDER,
                    UNSIGNED_BYTES,
                    SIGNED_BYTES,
                    UNDEFINED -> this;
        };
    }

    private static BoundsOrder physicalOrder(PrimitiveKind kind) {
        return switch (kind) {
            case BOOLEAN -> BOOLEAN;
            case INT32 -> SIGNED_INT32;
            case INT64 -> SIGNED_INT64;
            case FLOAT -> FLOAT;
            case DOUBLE -> DOUBLE;
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> UNSIGNED_BYTES;
            case INT96 -> UNDEFINED;
        };
    }

    private static BoundsOrder annotatedOrder(SchemaNode.Primitive leaf, LogicalType logicalType) {
        PrimitiveKind kind = leaf.kind();
        return switch (logicalType) {
            case LogicalType.Decimal _ -> decimalOrder(kind);
            case LogicalType.IntType intType -> integerOrder(kind, intType.isSigned());
            case LogicalType.DateType _ -> kind == PrimitiveKind.INT32 ? SIGNED_INT32 : UNDEFINED;
            case LogicalType.Time _ -> integerOrder(kind, true);
            case LogicalType.Timestamp _ -> kind == PrimitiveKind.INT64 ? SIGNED_INT64 : UNDEFINED;
            case LogicalType.StringType _, LogicalType.EnumType _, LogicalType.JsonType _, LogicalType.BsonType _ ->
                kind == PrimitiveKind.BYTE_ARRAY ? UNSIGNED_BYTES : UNDEFINED;
            case LogicalType.UuidType _ -> kind == PrimitiveKind.FIXED_LEN_BYTE_ARRAY ? UNSIGNED_BYTES : UNDEFINED;
            case LogicalType.Float16Type _ -> isHalfFloat(leaf) ? HALF_FLOAT : UNDEFINED;
            case LogicalType.UnknownType _ -> physicalOrder(kind);
            case LogicalType.Geometry _,
                    LogicalType.Geography _,
                    LogicalType.MapType _,
                    LogicalType.ListType _,
                    LogicalType.Variant _ -> UNDEFINED;
        };
    }

    private static BoundsOrder decimalOrder(PrimitiveKind kind) {
        return switch (kind) {
            case INT32 -> SIGNED_INT32;
            case INT64 -> SIGNED_INT64;
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> SIGNED_BYTES;
            case BOOLEAN, FLOAT, DOUBLE, INT96 -> UNDEFINED;
        };
    }

    private static BoundsOrder integerOrder(PrimitiveKind kind, boolean signed) {
        return switch (kind) {
            case INT32 -> signed ? SIGNED_INT32 : UNSIGNED_INT32;
            case INT64 -> signed ? SIGNED_INT64 : UNSIGNED_INT64;
            case BOOLEAN, FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> UNDEFINED;
        };
    }

    private static boolean isHalfFloat(SchemaNode.Primitive leaf) {
        return leaf.kind() == PrimitiveKind.FIXED_LEN_BYTE_ARRAY
                && leaf.typeLength().orElse(0) == HalfFloats.BYTES;
    }

    /** Whether the column gets a min and a max. */
    boolean isDefined() {
        return this != UNDEFINED;
    }

    /** Whether the bounds are floating-point numbers. The statistics of such a column count its NaN cells. */
    boolean isFloatingPoint() {
        return this == FLOAT || this == DOUBLE || this == HALF_FLOAT || isTotalOrder();
    }

    /** Whether the bounds follow IEEE 754 total order, declared in the footer for such a column. */
    boolean isTotalOrder() {
        return this == FLOAT_TOTAL_ORDER || this == DOUBLE_TOTAL_ORDER || this == HALF_FLOAT_TOTAL_ORDER;
    }

    /** Whether the cells are the two little-endian bytes of half floats. */
    boolean holdsHalfFloats() {
        return this == HALF_FLOAT || this == HALF_FLOAT_TOTAL_ORDER;
    }

    /** Whether the bounds are the bytes of binary cells, compared in place. */
    boolean comparesBytes() {
        return this == UNSIGNED_BYTES || this == SIGNED_BYTES;
    }

    /**
     * Compares two PLAIN-encoded bounds of a column in this order: negative when {@code a} orders first, positive when
     * {@code b} does, zero when neither does.
     *
     * @throws IllegalStateException for {@link #UNDEFINED}, an order without bounds to compare
     */
    int compare(MemorySegment a, MemorySegment b) {
        return switch (this) {
            case BOOLEAN -> Boolean.compare(a.get(JAVA_BYTE, 0) != 0, b.get(JAVA_BYTE, 0) != 0);
            case SIGNED_INT32 -> Integer.compare(a.get(INT32, 0), b.get(INT32, 0));
            case UNSIGNED_INT32 -> Integer.compareUnsigned(a.get(INT32, 0), b.get(INT32, 0));
            case SIGNED_INT64 -> Long.compare(a.get(INT64, 0), b.get(INT64, 0));
            case UNSIGNED_INT64 -> Long.compareUnsigned(a.get(INT64, 0), b.get(INT64, 0));
            case FLOAT, FLOAT_TOTAL_ORDER -> Integer.compare(floatKey(a), floatKey(b));
            case DOUBLE, DOUBLE_TOTAL_ORDER -> Long.compare(doubleKey(a), doubleKey(b));
            case HALF_FLOAT, HALF_FLOAT_TOTAL_ORDER -> Integer.compare(HalfFloats.key(a), HalfFloats.key(b));
            case UNSIGNED_BYTES -> UnsignedLexOrder.compare(a, b);
            case SIGNED_BYTES -> SignedBytesOrder.compare(a, b);
            case UNDEFINED -> throw new IllegalStateException("a column without a defined order has no bounds");
        };
    }

    private static int floatKey(MemorySegment bound) {
        return TotalOrder.key(bound.get(INT32, 0));
    }

    private static long doubleKey(MemorySegment bound) {
        return TotalOrder.key(bound.get(INT64, 0));
    }
}
