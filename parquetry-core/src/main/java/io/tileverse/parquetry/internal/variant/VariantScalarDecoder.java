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
package io.tileverse.parquetry.internal.variant;

import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_BINARY;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_BOOLEAN_FALSE;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_BOOLEAN_TRUE;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_DATE;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_DECIMAL16;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_DECIMAL4;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_DECIMAL8;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_DOUBLE;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_FLOAT;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_INT16;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_INT32;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_INT64;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_INT8;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_STRING;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_TIME;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_TIMESTAMP_NTZ_MICROS;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_TIMESTAMP_NTZ_NANOS;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_TIMESTAMP_TZ_MICROS;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_TIMESTAMP_TZ_NANOS;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_UUID;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.UuidConverter;
import io.tileverse.parquetry.variant.ShreddedVariant;
import io.tileverse.parquetry.variant.Variant;
import io.tileverse.parquetry.variant.VariantScalarValues;

/**
 * Decodes a {@link Variant} scalar value into the physical box a shredded {@code typed_value} leaf stores, the inverse
 * of {@link VariantScalarValues#encode}. The decode succeeds only when the Variant value's primitive type matches the
 * typed slot's {@code variantTypeId}; a non-matching value cannot be stored in the typed leaf without loss and the
 * caller must route it to the unshredded {@code value} residual instead.
 *
 * <p>The physical box matches the read path's {@code typed_value} boxing exactly: {@link Boolean} for booleans,
 * {@link Integer} for INT32-backed types, {@link Long} for INT64-backed types, {@link Float}, {@link Double}, and a
 * read-only big-endian {@link MemorySegment} for binary, string, uuid, and decimal16-unscaled leaves.
 */
final class VariantScalarDecoder {

    private static final int DECIMAL16_BYTES = 16;

    private VariantScalarDecoder() {}

    /**
     * The decode outcome: either the physical box for a matching scalar, or a mismatch signal telling the caller to
     * store the whole Variant value in the residual.
     */
    sealed interface Decoded permits Matched, Mismatch {}

    /** The Variant scalar matched the typed slot; {@code physical} is the box the typed leaf stores. */
    record Matched(Object physical) implements Decoded {}

    /** The Variant scalar did not match the typed slot and belongs in the unshredded {@code value} residual. */
    record Mismatch() implements Decoded {}

    private static final Mismatch MISMATCH = new Mismatch();

    /**
     * Decodes {@code value} (a {@link Variant} positioned at a scalar) into the physical box for {@code scalar}'s typed
     * leaf, or signals a mismatch when the value's primitive type is not the typed slot's {@code variantTypeId}.
     */
    static Decoded decode(ShreddedVariant.Scalar scalar, Variant value) {
        int typeId = scalar.variantTypeId();
        return switch (typeId) {
            case TYPE_BOOLEAN_TRUE, TYPE_BOOLEAN_FALSE -> decodeBoolean(value);
            case TYPE_INT8 -> decodeInt8(value);
            case TYPE_INT16 -> decodeInt16(value);
            case TYPE_INT32 -> decodeInt32(value);
            case TYPE_INT64 -> decodeInt64(value);
            case TYPE_FLOAT -> decodeFloat(value);
            case TYPE_DOUBLE -> decodeDouble(value);
            case TYPE_DATE -> decodeDate(value);
            case TYPE_TIME -> decodeTime(value);
            case TYPE_TIMESTAMP_TZ_MICROS -> decodeTimestampMicros(value, Variant.Type.TIMESTAMP_TZ);
            case TYPE_TIMESTAMP_NTZ_MICROS -> decodeTimestampMicros(value, Variant.Type.TIMESTAMP_NTZ);
            case TYPE_TIMESTAMP_TZ_NANOS -> decodeTimestampNanos(value, Variant.Type.TIMESTAMP_NANOS_TZ);
            case TYPE_TIMESTAMP_NTZ_NANOS -> decodeTimestampNanos(value, Variant.Type.TIMESTAMP_NANOS_NTZ);
            case TYPE_DECIMAL4 -> decodeDecimal4(scalar, value);
            case TYPE_DECIMAL8 -> decodeDecimal8(scalar, value);
            case TYPE_DECIMAL16 -> decodeDecimal16(scalar, value);
            case TYPE_BINARY -> decodeBinary(value);
            case TYPE_STRING -> decodeString(value);
            case TYPE_UUID -> decodeUuid(value);
            default -> throw new IllegalStateException("unexpected variant scalar type id " + typeId);
        };
    }

    private static Decoded decodeBoolean(Variant value) {
        if (value.type() != Variant.Type.BOOLEAN) {
            return MISMATCH;
        }
        return new Matched(value.getBoolean());
    }

    private static Decoded decodeInt8(Variant value) {
        if (value.type() != Variant.Type.INT8) {
            return MISMATCH;
        }
        return new Matched((int) value.getByte());
    }

    private static Decoded decodeInt16(Variant value) {
        if (value.type() != Variant.Type.INT16) {
            return MISMATCH;
        }
        return new Matched((int) value.getShort());
    }

    private static Decoded decodeInt32(Variant value) {
        if (value.type() != Variant.Type.INT32) {
            return MISMATCH;
        }
        return new Matched(value.getInt());
    }

    private static Decoded decodeInt64(Variant value) {
        if (value.type() != Variant.Type.INT64) {
            return MISMATCH;
        }
        return new Matched(value.getLong());
    }

    private static Decoded decodeFloat(Variant value) {
        if (value.type() != Variant.Type.FLOAT) {
            return MISMATCH;
        }
        return new Matched(value.getFloat());
    }

    private static Decoded decodeDouble(Variant value) {
        if (value.type() != Variant.Type.DOUBLE) {
            return MISMATCH;
        }
        return new Matched(value.getDouble());
    }

    private static Decoded decodeDate(Variant value) {
        if (value.type() != Variant.Type.DATE) {
            return MISMATCH;
        }
        return new Matched(value.getDateDays());
    }

    private static Decoded decodeTime(Variant value) {
        if (value.type() != Variant.Type.TIME_NTZ) {
            return MISMATCH;
        }
        return new Matched(value.getTimeMicros());
    }

    private static Decoded decodeTimestampMicros(Variant value, Variant.Type expected) {
        if (value.type() != expected) {
            return MISMATCH;
        }
        return new Matched(value.getTimestampMicros());
    }

    private static Decoded decodeTimestampNanos(Variant value, Variant.Type expected) {
        if (value.type() != expected) {
            return MISMATCH;
        }
        return new Matched(value.getTimestampNanos());
    }

    private static Decoded decodeDecimal4(ShreddedVariant.Scalar scalar, Variant value) {
        return decodeDecimalAtScale(scalar, value, Variant.Type.DECIMAL4, unscaled -> (int) unscaled.longValueExact());
    }

    private static Decoded decodeDecimal8(ShreddedVariant.Scalar scalar, Variant value) {
        return decodeDecimalAtScale(scalar, value, Variant.Type.DECIMAL8, BigInteger::longValueExact);
    }

    private static Decoded decodeDecimal16(ShreddedVariant.Scalar scalar, Variant value) {
        return decodeDecimalAtScale(
                scalar, value, Variant.Type.DECIMAL16, VariantScalarDecoder::bigEndianUnscaledSegment);
    }

    /**
     * A Variant decimal stores its own scale; the typed leaf stores only the unscaled value at the schema's fixed
     * scale. A value whose scale differs cannot be represented in the typed leaf without rescaling and is treated as a
     * mismatch, preserving the exact bytes in the residual.
     */
    private static Decoded decodeDecimalAtScale(
            ShreddedVariant.Scalar scalar,
            Variant value,
            Variant.Type expected,
            Function<BigInteger, Object> unscaledToPhysical) {
        if (value.type() != expected) {
            return MISMATCH;
        }
        BigDecimal decimal = value.getBigDecimal();
        if (decimal.scale() != schemaScale(scalar)) {
            return MISMATCH;
        }
        return new Matched(unscaledToPhysical.apply(decimal.unscaledValue()));
    }

    private static int schemaScale(ShreddedVariant.Scalar scalar) {
        LogicalType logicalType = scalar.typedValue().logicalType().orElseThrow(VariantScalarDecoder::missingDecimal);
        if (logicalType instanceof LogicalType.Decimal decimal) {
            return decimal.scale();
        }
        throw missingDecimal();
    }

    private static IllegalStateException missingDecimal() {
        return new IllegalStateException("decimal scalar without a Decimal logical type");
    }

    private static MemorySegment bigEndianUnscaledSegment(BigInteger unscaled) {
        byte[] bigEndian = signExtendedBigEndian(unscaled, DECIMAL16_BYTES);
        return MemorySegment.ofArray(bigEndian).asReadOnly();
    }

    private static byte[] signExtendedBigEndian(BigInteger value, int width) {
        byte[] minimal = value.toByteArray();
        if (minimal.length > width) {
            // Unreachable from a well-formed Variant DECIMAL16; a malformed input would otherwise throw AIOOBE below.
            throw new ParquetWriteException("decimal unscaled value does not fit the " + width + "-byte decimal width");
        }
        byte[] fixed = new byte[width];
        byte signExtension = (value.signum() < 0) ? (byte) 0xFF : (byte) 0x00;
        for (int i = 0; i < width; i++) {
            fixed[i] = signExtension;
        }
        System.arraycopy(minimal, 0, fixed, width - minimal.length, minimal.length);
        return fixed;
    }

    private static Decoded decodeBinary(Variant value) {
        if (value.type() != Variant.Type.BINARY) {
            return MISMATCH;
        }
        return new Matched(value.getBinary());
    }

    private static Decoded decodeString(Variant value) {
        if (value.type() != Variant.Type.STRING) {
            return MISMATCH;
        }
        return new Matched(utf8Segment(value.getString()));
    }

    private static Decoded decodeUuid(Variant value) {
        if (value.type() != Variant.Type.UUID) {
            return MISMATCH;
        }
        return new Matched(UuidConverter.toReadOnlySegment(value.getUuid()));
    }

    private static MemorySegment utf8Segment(String text) {
        byte[] utf8 = text.getBytes(StandardCharsets.UTF_8);
        return MemorySegment.ofArray(utf8).asReadOnly();
    }
}
