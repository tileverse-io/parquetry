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
package io.tileverse.parquetry.arrow;

import java.util.Optional;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.UnsupportedFeatureException;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The Arrow type chosen for one parquetry leaf column, plus the parameters a schema encoder needs to build the matching
 * flatbuffer {@code Field} (bit width, decimal precision/scale, time unit, fixed-size width). Unsupported column kinds
 * are rejected here with {@link UnsupportedFeatureException}.
 */
record ArrowFieldType(
        Kind kind, int bitWidth, int byteWidth, int precision, int scale, TimeUnit timeUnit, boolean utcAdjusted) {

    /** Arrow type family, matching the flatbuffer {@code Type} union discriminant names. */
    enum Kind {
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
    enum TimeUnit {
        SECOND,
        MILLISECOND,
        MICROSECOND,
        NANOSECOND
    }

    /**
     * Resolves the Arrow type for {@code leaf}, applying logical-type annotations where they change the physical
     * mapping. Throws {@link UnsupportedFeatureException} for INT96, DECIMAL (deferred - correct Arrow Decimal128
     * layout requires logical-type-aware buffer encoding), and FLOAT16.
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
            case INT96 -> throw unsupported("INT96 columns", leaf);
        };
    }

    private static ArrowFieldType int32(Optional<LogicalType> logical) {
        if (logical.isEmpty()) {
            return new ArrowFieldType(Kind.INT, 32, 0, 0, 0, null, true);
        }
        LogicalType type = logical.get();
        if (type instanceof LogicalType.DateType) {
            return new ArrowFieldType(Kind.DATE32, 32, 0, 0, 0, null, false);
        }
        if (type instanceof LogicalType.Time time) {
            return time(time);
        }
        if (type instanceof LogicalType.Decimal) {
            throw new UnsupportedFeatureException("Arrow output does not support DECIMAL columns");
        }
        return new ArrowFieldType(Kind.INT, 32, 0, 0, 0, null, true);
    }

    private static ArrowFieldType int64(Optional<LogicalType> logical) {
        if (logical.isEmpty()) {
            return new ArrowFieldType(Kind.INT, 64, 0, 0, 0, null, true);
        }
        LogicalType type = logical.get();
        if (type instanceof LogicalType.Timestamp(boolean isAdjustedToUTC, LogicalType.TimeUnit tsUnit)) {
            return new ArrowFieldType(Kind.TIMESTAMP, 64, 0, 0, 0, unit(tsUnit), isAdjustedToUTC);
        }
        if (type instanceof LogicalType.Time time) {
            return time(time);
        }
        if (type instanceof LogicalType.Decimal) {
            throw new UnsupportedFeatureException("Arrow output does not support DECIMAL columns");
        }
        return new ArrowFieldType(Kind.INT, 64, 0, 0, 0, null, true);
    }

    /** Maps BYTE_ARRAY to UTF8 when the logical type is a string-like annotation, BINARY otherwise. */
    private static ArrowFieldType byteArray(Optional<LogicalType> logical) {
        boolean isStringLike = logical.isPresent()
                && (logical.get() instanceof LogicalType.StringType
                        || logical.get() instanceof LogicalType.EnumType
                        || logical.get() instanceof LogicalType.JsonType);
        return simple(isStringLike ? Kind.UTF8 : Kind.BINARY);
    }

    private static ArrowFieldType fixedLenByteArray(SchemaNode.Primitive leaf, Optional<LogicalType> logical) {
        if (logical.isPresent() && logical.get() instanceof LogicalType.Decimal) {
            throw new UnsupportedFeatureException("Arrow output does not support DECIMAL columns");
        }
        if (logical.isPresent() && logical.get() instanceof LogicalType.Float16Type) {
            throw unsupported("FLOAT16 columns", leaf);
        }
        int width = leaf.typeLength().orElseThrow(() -> unsupported("FIXED_LEN_BYTE_ARRAY without a length", leaf));
        return new ArrowFieldType(Kind.FIXED_SIZE_BINARY, 0, width, 0, 0, null, false);
    }

    private static ArrowFieldType time(LogicalType.Time time) {
        TimeUnit unit = unit(time.unit());
        // Arrow TIME32 uses 32 bits for milliseconds; all finer granularities require TIME64 (64 bits).
        int bitWidth = unit == TimeUnit.MILLISECOND ? 32 : 64;
        return new ArrowFieldType(Kind.TIME, bitWidth, 0, 0, 0, unit, time.isAdjustedToUTC());
    }

    private static ArrowFieldType floatingPoint(int bitWidth) {
        return new ArrowFieldType(Kind.FLOATING_POINT, bitWidth, 0, 0, 0, null, false);
    }

    private static ArrowFieldType simple(Kind kind) {
        return new ArrowFieldType(kind, 0, 0, 0, 0, null, false);
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
