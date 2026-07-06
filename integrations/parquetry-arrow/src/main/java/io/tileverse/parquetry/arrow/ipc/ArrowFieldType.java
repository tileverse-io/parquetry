/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.arrow.ipc;

import java.util.Optional;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.UnsupportedFeatureException;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The Arrow type chosen for one parquetry leaf column, plus the parameters a schema encoder needs to build the matching
 * flatbuffer {@code Field} (bit width, decimal precision/scale, time unit, fixed-size width, sign, value transform).
 * Unsupported column kinds are rejected here with {@link UnsupportedFeatureException}.
 */
public record ArrowFieldType(
        Kind kind,
        int bitWidth,
        int byteWidth,
        int precision,
        int scale,
        TimeUnit timeUnit,
        boolean utcAdjusted,
        boolean signed,
        ValueTransform valueTransform) {

    /** Which rewrite the body encoder applies when a leaf's Arrow value bytes differ from the Parquet bytes. */
    public enum ValueTransform {
        NONE,
        INT96_TO_TIMESTAMP,
        DECIMAL128
    }

    /** Arrow type family, matching the flatbuffer {@code Type} union discriminant names. */
    public enum Kind {
        BOOL,
        INT,
        FLOATING_POINT,
        UTF8,
        BINARY,
        FIXED_SIZE_BINARY,
        DECIMAL,
        DATE32,
        TIME,
        TIMESTAMP
    }

    /**
     * Mirrors {@code org.apache.arrow.flatbuf.TimeUnit} ordinals. Named with the full precision label to match Arrow
     * IPC spec language exactly and avoid confusion with Parquet's own {@link LogicalType.TimeUnit}.
     */
    public enum TimeUnit {
        SECOND,
        MILLISECOND,
        MICROSECOND,
        NANOSECOND
    }

    /**
     * Resolves the Arrow type for {@code leaf}, applying logical-type annotations where they change the physical
     * mapping. INT96 maps to a timezone-less microsecond timestamp (the body encoder converts the 12-byte values).
     * DECIMAL up to precision 38 maps to Arrow Decimal128 (the body encoder re-encodes the unscaled value as 16-byte
     * little-endian); precision above 38 exceeds Decimal128 and is rejected with {@link UnsupportedFeatureException}.
     */
    static ArrowFieldType of(SchemaNode.Primitive leaf) {
        Optional<LogicalType> logical = leaf.logicalType();
        return switch (leaf.kind()) {
            case BOOLEAN -> simple(Kind.BOOL);
            case INT32 -> int32(logical);
            case INT64 -> int64(logical);
            case FLOAT -> floatingPoint(32);
            case DOUBLE -> floatingPoint(64);
            case BYTE_ARRAY -> byteArray(logical);
            case FIXED_LEN_BYTE_ARRAY -> fixedLenByteArray(leaf, logical);
            case INT96 -> int96Timestamp();
        };
    }

    /**
     * Maps the legacy INT96 timestamp to a timezone-less microsecond timestamp; the body encoder converts the values.
     */
    private static ArrowFieldType int96Timestamp() {
        return new ArrowFieldType(
                Kind.TIMESTAMP, 64, 0, 0, 0, TimeUnit.MICROSECOND, false, true, ValueTransform.INT96_TO_TIMESTAMP);
    }

    private static ArrowFieldType int32(Optional<LogicalType> logical) {
        if (logical.isEmpty()) {
            return signedInt(32);
        }
        LogicalType type = logical.get();
        if (type instanceof LogicalType.DateType) {
            return new ArrowFieldType(Kind.DATE32, 32, 0, 0, 0, null, false, true, ValueTransform.NONE);
        }
        if (type instanceof LogicalType.Time time) {
            return time(time);
        }
        if (type instanceof LogicalType.Decimal decimal) {
            return decimal(decimal);
        }
        if (type instanceof LogicalType.IntType intType) {
            return integer(32, intType.isSigned());
        }
        return signedInt(32);
    }

    private static ArrowFieldType int64(Optional<LogicalType> logical) {
        if (logical.isEmpty()) {
            return signedInt(64);
        }
        LogicalType type = logical.get();
        if (type instanceof LogicalType.Timestamp(boolean isAdjustedToUTC, LogicalType.TimeUnit tsUnit)) {
            return new ArrowFieldType(
                    Kind.TIMESTAMP, 64, 0, 0, 0, unit(tsUnit), isAdjustedToUTC, true, ValueTransform.NONE);
        }
        if (type instanceof LogicalType.Time time) {
            return time(time);
        }
        if (type instanceof LogicalType.Decimal decimal) {
            return decimal(decimal);
        }
        if (type instanceof LogicalType.IntType intType) {
            return integer(64, intType.isSigned());
        }
        return signedInt(64);
    }

    /** Maps BYTE_ARRAY to Decimal128 when annotated DECIMAL, UTF8 for string-like annotations, BINARY otherwise. */
    private static ArrowFieldType byteArray(Optional<LogicalType> logical) {
        if (logical.isPresent() && logical.get() instanceof LogicalType.Decimal decimal) {
            return decimal(decimal);
        }
        boolean isStringLike = logical.isPresent()
                && (logical.get() instanceof LogicalType.StringType
                        || logical.get() instanceof LogicalType.EnumType
                        || logical.get() instanceof LogicalType.JsonType);
        return simple(isStringLike ? Kind.UTF8 : Kind.BINARY);
    }

    private static ArrowFieldType fixedLenByteArray(SchemaNode.Primitive leaf, Optional<LogicalType> logical) {
        if (logical.isPresent() && logical.get() instanceof LogicalType.Decimal decimal) {
            return decimal(decimal);
        }
        if (logical.isPresent() && logical.get() instanceof LogicalType.Float16Type) {
            return floatingPoint(16);
        }
        int width = leaf.typeLength().orElseThrow(() -> unsupported("FIXED_LEN_BYTE_ARRAY without a length", leaf));
        return new ArrowFieldType(Kind.FIXED_SIZE_BINARY, 0, width, 0, 0, null, false, true, ValueTransform.NONE);
    }

    private static final int DECIMAL128_MAX_PRECISION = 38;

    /** Maps a Parquet DECIMAL to Arrow Decimal128; the body re-encodes the unscaled value as 16-byte little-endian. */
    private static ArrowFieldType decimal(LogicalType.Decimal decimal) {
        if (decimal.precision() > DECIMAL128_MAX_PRECISION) {
            throw new UnsupportedFeatureException("Arrow output supports DECIMAL precision up to "
                    + DECIMAL128_MAX_PRECISION + ", got " + decimal.precision());
        }
        return new ArrowFieldType(
                Kind.DECIMAL,
                128,
                0,
                decimal.precision(),
                decimal.scale(),
                null,
                false,
                true,
                ValueTransform.DECIMAL128);
    }

    private static ArrowFieldType signedInt(int bitWidth) {
        return new ArrowFieldType(Kind.INT, bitWidth, 0, 0, 0, null, true, true, ValueTransform.NONE);
    }

    /**
     * Builds an integer type at the column's physical bit width. Parquet stores sub-word ints (8/16-bit) in a 32-bit
     * physical column; the Arrow width therefore follows the physical type, not the logical annotation, and only the
     * sign bit comes from the annotation. The data buffer is reused unchanged.
     */
    private static ArrowFieldType integer(int physicalBitWidth, boolean signed) {
        return new ArrowFieldType(Kind.INT, physicalBitWidth, 0, 0, 0, null, true, signed, ValueTransform.NONE);
    }

    private static ArrowFieldType time(LogicalType.Time time) {
        TimeUnit unit = unit(time.unit());
        // Arrow TIME32 uses 32 bits for milliseconds; all finer granularities require TIME64 (64 bits).
        int bitWidth = unit == TimeUnit.MILLISECOND ? 32 : 64;
        return new ArrowFieldType(
                Kind.TIME, bitWidth, 0, 0, 0, unit, time.isAdjustedToUTC(), true, ValueTransform.NONE);
    }

    private static ArrowFieldType floatingPoint(int bitWidth) {
        return new ArrowFieldType(Kind.FLOATING_POINT, bitWidth, 0, 0, 0, null, false, true, ValueTransform.NONE);
    }

    private static ArrowFieldType simple(Kind kind) {
        return new ArrowFieldType(kind, 0, 0, 0, 0, null, false, true, ValueTransform.NONE);
    }

    /** The Arrow {@code binary} leaf type, used for a Parquet Variant column's metadata and value children. */
    static ArrowFieldType binary() {
        return simple(Kind.BINARY);
    }

    private static TimeUnit unit(LogicalType.TimeUnit parquetUnit) {
        return switch (parquetUnit) {
            case MILLIS -> TimeUnit.MILLISECOND;
            case MICROS -> TimeUnit.MICROSECOND;
            case NANOS -> TimeUnit.NANOSECOND;
        };
    }

    private static UnsupportedFeatureException unsupported(String what, SchemaNode.Primitive leaf) {
        return new UnsupportedFeatureException(
                "Arrow output does not support " + what + " (column " + leaf.name() + ")");
    }
}
