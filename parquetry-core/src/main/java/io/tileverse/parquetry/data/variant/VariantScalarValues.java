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
package io.tileverse.parquetry.data.variant;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigInteger;

import io.tileverse.parquetry.format.LogicalType;

/**
 * Builds the canonical Parquet Variant scalar value bytes for a shredded scalar leaf, given the classified
 * {@link ShreddedVariant.Scalar} and the physical value read from its {@code typed_value} column. The bytes match what
 * a conformant Variant writer produces, letting the read path reconstruct a {@link Variant} value buffer from shredded
 * columns.
 *
 * <p>The {@code physical} value is boxed by the read path the way it boxes a {@code typed_value} primitive:
 * {@link Boolean} for booleans, {@link Integer} for INT32-backed types, {@link Long} for INT64-backed types,
 * {@link Float}, {@link Double}, and a read-only {@link MemorySegment} for binary, string, uuid, and decimal16-unscaled
 * leaves.
 */
final class VariantScalarValues {

    private static final int BASIC_TYPE_PRIMITIVE = 0;
    private static final int BASIC_TYPE_SHORT_STRING = 1;

    private static final int TYPE_BOOLEAN_TRUE = 1;
    private static final int TYPE_BOOLEAN_FALSE = 2;
    private static final int TYPE_INT8 = 3;
    private static final int TYPE_INT16 = 4;
    private static final int TYPE_INT32 = 5;
    private static final int TYPE_INT64 = 6;
    private static final int TYPE_DOUBLE = 7;
    private static final int TYPE_DECIMAL4 = 8;
    private static final int TYPE_DECIMAL8 = 9;
    private static final int TYPE_DECIMAL16 = 10;
    private static final int TYPE_DATE = 11;
    private static final int TYPE_TIMESTAMP_TZ_MICROS = 12;
    private static final int TYPE_TIMESTAMP_NTZ_MICROS = 13;
    private static final int TYPE_FLOAT = 14;
    private static final int TYPE_BINARY = 15;
    private static final int TYPE_STRING = 16;
    private static final int TYPE_TIME = 17;
    private static final int TYPE_TIMESTAMP_TZ_NANOS = 18;
    private static final int TYPE_TIMESTAMP_NTZ_NANOS = 19;
    private static final int TYPE_UUID = 20;

    private static final int SHORT_STRING_MAX_LENGTH = 63;
    private static final int DECIMAL16_BYTES = 16;

    private VariantScalarValues() {}

    /** Builds the canonical Variant scalar value bytes for a shredded scalar leaf. */
    static MemorySegment encode(ShreddedVariant.Scalar scalar, Object physical) {
        byte[] bytes = encodeBytes(scalar, physical);
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    private static byte[] encodeBytes(ShreddedVariant.Scalar scalar, Object physical) {
        int typeId = scalar.variantTypeId();
        return switch (typeId) {
            case TYPE_BOOLEAN_TRUE, TYPE_BOOLEAN_FALSE -> bool((Boolean) physical);
            case TYPE_INT8 -> int8((Integer) physical);
            case TYPE_INT16 -> headerPlusLittleEndian(TYPE_INT16, (Integer) physical, Short.BYTES);
            case TYPE_INT32 -> headerPlusLittleEndian(TYPE_INT32, (Integer) physical, Integer.BYTES);
            case TYPE_INT64 -> headerPlusLittleEndian(TYPE_INT64, (Long) physical, Long.BYTES);
            case TYPE_DOUBLE ->
                headerPlusLittleEndian(TYPE_DOUBLE, Double.doubleToRawLongBits((Double) physical), Long.BYTES);
            case TYPE_FLOAT ->
                headerPlusLittleEndian(TYPE_FLOAT, Float.floatToRawIntBits((Float) physical), Float.BYTES);
            case TYPE_DECIMAL4 -> decimalFromInt(scalar, (Integer) physical);
            case TYPE_DECIMAL8 -> decimalFromLong(scalar, (Long) physical);
            case TYPE_DECIMAL16 -> decimalFromSegment(scalar, (MemorySegment) physical);
            case TYPE_DATE -> headerPlusLittleEndian(TYPE_DATE, (Integer) physical, Integer.BYTES);
            case TYPE_TIME -> headerPlusLittleEndian(TYPE_TIME, (Long) physical, Long.BYTES);
            case TYPE_TIMESTAMP_TZ_MICROS,
                    TYPE_TIMESTAMP_NTZ_MICROS,
                    TYPE_TIMESTAMP_TZ_NANOS,
                    TYPE_TIMESTAMP_NTZ_NANOS -> headerPlusLittleEndian(typeId, (Long) physical, Long.BYTES);
            case TYPE_BINARY -> binary((MemorySegment) physical);
            case TYPE_STRING -> string((MemorySegment) physical);
            case TYPE_UUID -> uuid((MemorySegment) physical);
            default -> throw new IllegalStateException("unexpected variant scalar type id " + typeId);
        };
    }

    private static byte[] bool(Boolean value) {
        int typeId = value ? TYPE_BOOLEAN_TRUE : TYPE_BOOLEAN_FALSE;
        return new byte[] {(byte) primitiveHeader(typeId)};
    }

    private static byte[] int8(Integer value) {
        return new byte[] {(byte) primitiveHeader(TYPE_INT8), value.byteValue()};
    }

    private static byte[] headerPlusLittleEndian(int typeId, long value, int width) {
        byte[] out = new byte[1 + width];
        out[0] = (byte) primitiveHeader(typeId);
        writeLittleEndian(out, 1, value, width);
        return out;
    }

    private static byte[] decimalFromInt(ShreddedVariant.Scalar scalar, Integer unscaled) {
        return decimal(TYPE_DECIMAL4, scaleOf(scalar), BigInteger.valueOf(unscaled), Integer.BYTES);
    }

    private static byte[] decimalFromLong(ShreddedVariant.Scalar scalar, Long unscaled) {
        return decimal(TYPE_DECIMAL8, scaleOf(scalar), BigInteger.valueOf(unscaled), Long.BYTES);
    }

    private static byte[] decimalFromSegment(ShreddedVariant.Scalar scalar, MemorySegment bigEndianUnscaled) {
        BigInteger unscaled = new BigInteger(bigEndianUnscaled.toArray(ValueLayout.JAVA_BYTE));
        return decimal(TYPE_DECIMAL16, scaleOf(scalar), unscaled, DECIMAL16_BYTES);
    }

    private static byte[] decimal(int typeId, int scale, BigInteger unscaled, int unscaledBytes) {
        byte[] out = new byte[2 + unscaledBytes];
        out[0] = (byte) primitiveHeader(typeId);
        out[1] = (byte) scale;
        byte[] littleEndian = VariantBytes.twosComplementLittleEndian(unscaled, unscaledBytes);
        System.arraycopy(littleEndian, 0, out, 2, unscaledBytes);
        return out;
    }

    private static int scaleOf(ShreddedVariant.Scalar scalar) {
        LogicalType logicalType = scalar.typedValue().logicalType().orElseThrow(VariantScalarValues::missingDecimal);
        if (logicalType instanceof LogicalType.Decimal decimal) {
            return decimal.scale();
        }
        throw missingDecimal();
    }

    private static IllegalStateException missingDecimal() {
        return new IllegalStateException("decimal scalar without a Decimal logical type");
    }

    private static byte[] binary(MemorySegment segment) {
        byte[] payload = segment.toArray(ValueLayout.JAVA_BYTE);
        byte[] out = new byte[1 + Integer.BYTES + payload.length];
        out[0] = (byte) primitiveHeader(TYPE_BINARY);
        writeLittleEndian(out, 1, payload.length & 0xFFFFFFFFL, Integer.BYTES);
        System.arraycopy(payload, 0, out, 1 + Integer.BYTES, payload.length);
        return out;
    }

    private static byte[] string(MemorySegment segment) {
        byte[] utf8 = segment.toArray(ValueLayout.JAVA_BYTE);
        if (utf8.length <= SHORT_STRING_MAX_LENGTH) {
            return shortString(utf8);
        }
        return longString(utf8);
    }

    private static byte[] shortString(byte[] utf8) {
        byte[] out = new byte[1 + utf8.length];
        out[0] = (byte) VariantBytes.header(BASIC_TYPE_SHORT_STRING, utf8.length);
        System.arraycopy(utf8, 0, out, 1, utf8.length);
        return out;
    }

    private static byte[] longString(byte[] utf8) {
        byte[] out = new byte[1 + Integer.BYTES + utf8.length];
        out[0] = (byte) primitiveHeader(TYPE_STRING);
        writeLittleEndian(out, 1, utf8.length & 0xFFFFFFFFL, Integer.BYTES);
        System.arraycopy(utf8, 0, out, 1 + Integer.BYTES, utf8.length);
        return out;
    }

    private static byte[] uuid(MemorySegment segment) {
        byte[] bigEndian = segment.toArray(ValueLayout.JAVA_BYTE);
        byte[] out = new byte[1 + bigEndian.length];
        out[0] = (byte) primitiveHeader(TYPE_UUID);
        System.arraycopy(bigEndian, 0, out, 1, bigEndian.length);
        return out;
    }

    private static void writeLittleEndian(byte[] out, int offset, long value, int width) {
        for (int i = 0; i < width; i++) {
            out[offset + i] = (byte) ((value >>> (8 * i)) & 0xFF);
        }
    }

    private static int primitiveHeader(int typeId) {
        return VariantBytes.header(BASIC_TYPE_PRIMITIVE, typeId);
    }
}
