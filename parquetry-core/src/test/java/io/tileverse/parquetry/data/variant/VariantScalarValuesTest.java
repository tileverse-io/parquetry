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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.UuidConverter;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class VariantScalarValuesTest {

    @Test
    void booleanTrueMatchesEncoder() {
        byte[] actual = encode(booleanScalar(), Boolean.TRUE);
        byte[] expected = new VariantEncoder().addBoolean(true).encode().value().toArray(JAVA_BYTE);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void booleanFalseMatchesEncoder() {
        byte[] actual = encode(booleanScalar(), Boolean.FALSE);
        byte[] expected =
                new VariantEncoder().addBoolean(false).encode().value().toArray(JAVA_BYTE);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void int8MatchesEncoder() {
        byte[] actual = encode(int8Scalar(), Integer.valueOf(-7));
        byte[] expected =
                new VariantEncoder().addByte((byte) -7).encode().value().toArray(JAVA_BYTE);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void int16MatchesEncoder() {
        byte[] actual = encode(int16Scalar(), Integer.valueOf(-1234));
        byte[] expected =
                new VariantEncoder().addShort((short) -1234).encode().value().toArray(JAVA_BYTE);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void int32MatchesEncoder() {
        byte[] actual = encode(int32Scalar(), Integer.valueOf(123456));
        byte[] expected = new VariantEncoder().addInt(123456).encode().value().toArray(JAVA_BYTE);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void int64MatchesEncoder() {
        byte[] actual = encode(int64Scalar(), Long.valueOf(9876543210L));
        byte[] expected =
                new VariantEncoder().addLong(9876543210L).encode().value().toArray(JAVA_BYTE);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void floatMatchesEncoder() {
        byte[] actual = encode(floatScalar(), Float.valueOf(3.5f));
        byte[] expected = new VariantEncoder().addFloat(3.5f).encode().value().toArray(JAVA_BYTE);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void doubleMatchesEncoder() {
        byte[] actual = encode(doubleScalar(), Double.valueOf(2.718281828));
        byte[] expected =
                new VariantEncoder().addDouble(2.718281828).encode().value().toArray(JAVA_BYTE);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void binaryMatchesEncoder() {
        byte[] payload = {1, 2, 3, 4, 5};
        byte[] actual = encode(binaryScalar(), readOnly(payload));
        byte[] expected = new VariantEncoder()
                .addBinary(readOnly(payload))
                .encode()
                .value()
                .toArray(JAVA_BYTE);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void shortStringMatchesEncoder() {
        String text = "hello";
        byte[] actual = encode(stringScalar(), readOnly(text.getBytes(StandardCharsets.UTF_8)));
        byte[] expected = new VariantEncoder().addString(text).encode().value().toArray(JAVA_BYTE);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void longStringMatchesEncoder() {
        String text = "x".repeat(100);
        byte[] actual = encode(stringScalar(), readOnly(text.getBytes(StandardCharsets.UTF_8)));
        byte[] expected = new VariantEncoder().addString(text).encode().value().toArray(JAVA_BYTE);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void emptyBinaryMatchesEncoder() {
        byte[] empty = new byte[0];
        byte[] actual = encode(binaryScalar(), readOnly(empty));
        byte[] expected =
                new VariantEncoder().addBinary(readOnly(empty)).encode().value().toArray(JAVA_BYTE);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void emptyStringMatchesEncoder() {
        byte[] actual = encode(stringScalar(), readOnly(new byte[0]));
        byte[] expected = new VariantEncoder().addString("").encode().value().toArray(JAVA_BYTE);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void lastShortStringMatchesEncoder() {
        String text = "x".repeat(63);
        byte[] actual = encode(stringScalar(), readOnly(text.getBytes(StandardCharsets.UTF_8)));
        byte[] expected = new VariantEncoder().addString(text).encode().value().toArray(JAVA_BYTE);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void firstLongStringMatchesEncoder() {
        String text = "x".repeat(64);
        byte[] actual = encode(stringScalar(), readOnly(text.getBytes(StandardCharsets.UTF_8)));
        byte[] expected = new VariantEncoder().addString(text).encode().value().toArray(JAVA_BYTE);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void int8TruncatesToLowByte() {
        byte[] actual = encode(int8Scalar(), Integer.valueOf(200));
        byte[] expected = headerPlus(3, new byte[] {(byte) 200});
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void uuidMatchesEncoder() {
        UUID uuid = UUID.fromString("12345678-1234-5678-1234-567812345678");
        byte[] actual = encode(uuidScalar(), UuidConverter.toReadOnlySegment(uuid));
        byte[] expected = new VariantEncoder().addUuid(uuid).encode().value().toArray(JAVA_BYTE);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void dateIsHeaderPlusLittleEndianDays() {
        int days = 19876;
        byte[] actual = encode(dateScalar(), Integer.valueOf(days));
        byte[] expected = headerPlus(11, littleEndian(days & 0xFFFFFFFFL, 4));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void timeIsHeaderPlusLittleEndianMicros() {
        long micros = 45_123_456_789L;
        byte[] actual = encode(timeScalar(), Long.valueOf(micros));
        byte[] expected = headerPlus(17, littleEndian(micros, 8));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void timestampTzMicrosIsHeaderPlusLittleEndianLong() {
        long micros = 1_700_000_000_000_000L;
        byte[] actual = encode(timestampScalar(12), Long.valueOf(micros));
        byte[] expected = headerPlus(12, littleEndian(micros, 8));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void timestampNtzMicrosIsHeaderPlusLittleEndianLong() {
        long micros = 1_700_000_000_000_000L;
        byte[] actual = encode(timestampScalar(13), Long.valueOf(micros));
        byte[] expected = headerPlus(13, littleEndian(micros, 8));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void timestampTzNanosIsHeaderPlusLittleEndianLong() {
        long nanos = 1_700_000_000_123_456_789L;
        byte[] actual = encode(timestampScalar(18), Long.valueOf(nanos));
        byte[] expected = headerPlus(18, littleEndian(nanos, 8));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void timestampNtzNanosIsHeaderPlusLittleEndianLong() {
        long nanos = 1_700_000_000_123_456_789L;
        byte[] actual = encode(timestampScalar(19), Long.valueOf(nanos));
        byte[] expected = headerPlus(19, littleEndian(nanos, 8));
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void decimal4IsHeaderScaleThenFourByteLittleEndianUnscaled() {
        int unscaled = -12345;
        byte[] payload = new byte[5];
        payload[0] = 3;
        System.arraycopy(littleEndian(unscaled & 0xFFFFFFFFL, 4), 0, payload, 1, 4);
        byte[] actual = encode(decimal4Scalar(3), Integer.valueOf(unscaled));
        byte[] expected = headerPlus(8, payload);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void decimal8IsHeaderScaleThenEightByteLittleEndianUnscaled() {
        long unscaled = -987654321098L;
        byte[] payload = new byte[9];
        payload[0] = 6;
        System.arraycopy(littleEndian(unscaled, 8), 0, payload, 1, 8);
        byte[] actual = encode(decimal8Scalar(6), Long.valueOf(unscaled));
        byte[] expected = headerPlus(9, payload);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void decimal16IsHeaderScaleThenSixteenByteLittleEndianUnscaled() {
        BigInteger unscaled = new BigInteger("-1234567890123456789012345");
        byte[] bigEndian = bigEndianFixed16(unscaled);
        byte[] payload = new byte[17];
        payload[0] = 10;
        System.arraycopy(twosComplementLittleEndian(unscaled, 16), 0, payload, 1, 16);
        byte[] actual = encode(decimal16Scalar(10), readOnly(bigEndian));
        byte[] expected = headerPlus(10, payload);
        assertThat(actual).isEqualTo(expected);
    }

    private static byte[] encode(ShreddedVariant.Scalar scalar, Object physical) {
        return VariantScalarValues.encode(scalar, physical).toArray(JAVA_BYTE);
    }

    private static byte[] headerPlus(int typeId, byte[] payload) {
        byte[] out = new byte[1 + payload.length];
        out[0] = (byte) (typeId << 2);
        System.arraycopy(payload, 0, out, 1, payload.length);
        return out;
    }

    private static byte[] littleEndian(long value, int width) {
        byte[] bytes = new byte[width];
        for (int i = 0; i < width; i++) {
            bytes[i] = (byte) ((value >>> (8 * i)) & 0xFF);
        }
        return bytes;
    }

    private static byte[] twosComplementLittleEndian(BigInteger value, int width) {
        byte[] bigEndian = value.toByteArray();
        byte[] littleEndian = new byte[width];
        byte signExtension = (value.signum() < 0) ? (byte) 0xFF : (byte) 0x00;
        for (int i = 0; i < width; i++) {
            int bigEndianIndex = bigEndian.length - 1 - i;
            littleEndian[i] = (bigEndianIndex >= 0) ? bigEndian[bigEndianIndex] : signExtension;
        }
        return littleEndian;
    }

    private static byte[] bigEndianFixed16(BigInteger value) {
        byte[] minimal = value.toByteArray();
        byte[] fixed = new byte[16];
        byte signExtension = (value.signum() < 0) ? (byte) 0xFF : (byte) 0x00;
        for (int i = 0; i < 16; i++) {
            fixed[i] = signExtension;
        }
        System.arraycopy(minimal, 0, fixed, 16 - minimal.length, minimal.length);
        return fixed;
    }

    private static MemorySegment readOnly(byte[] bytes) {
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    private static ShreddedVariant.Scalar booleanScalar() {
        return new ShreddedVariant.Scalar(primitive(PrimitiveKind.BOOLEAN, Optional.empty()), 1);
    }

    private static ShreddedVariant.Scalar int8Scalar() {
        return new ShreddedVariant.Scalar(
                primitive(PrimitiveKind.INT32, Optional.of(new LogicalType.IntType((byte) 8, true))), 3);
    }

    private static ShreddedVariant.Scalar int16Scalar() {
        return new ShreddedVariant.Scalar(
                primitive(PrimitiveKind.INT32, Optional.of(new LogicalType.IntType((byte) 16, true))), 4);
    }

    private static ShreddedVariant.Scalar int32Scalar() {
        return new ShreddedVariant.Scalar(primitive(PrimitiveKind.INT32, Optional.empty()), 5);
    }

    private static ShreddedVariant.Scalar int64Scalar() {
        return new ShreddedVariant.Scalar(primitive(PrimitiveKind.INT64, Optional.empty()), 6);
    }

    private static ShreddedVariant.Scalar floatScalar() {
        return new ShreddedVariant.Scalar(primitive(PrimitiveKind.FLOAT, Optional.empty()), 14);
    }

    private static ShreddedVariant.Scalar doubleScalar() {
        return new ShreddedVariant.Scalar(primitive(PrimitiveKind.DOUBLE, Optional.empty()), 7);
    }

    private static ShreddedVariant.Scalar binaryScalar() {
        return new ShreddedVariant.Scalar(primitive(PrimitiveKind.BYTE_ARRAY, Optional.empty()), 15);
    }

    private static ShreddedVariant.Scalar stringScalar() {
        return new ShreddedVariant.Scalar(
                primitive(PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType())), 16);
    }

    private static ShreddedVariant.Scalar uuidScalar() {
        return new ShreddedVariant.Scalar(fixedLen(16, Optional.of(new LogicalType.UuidType())), 20);
    }

    private static ShreddedVariant.Scalar dateScalar() {
        return new ShreddedVariant.Scalar(primitive(PrimitiveKind.INT32, Optional.of(new LogicalType.DateType())), 11);
    }

    private static ShreddedVariant.Scalar timeScalar() {
        return new ShreddedVariant.Scalar(
                primitive(PrimitiveKind.INT64, Optional.of(new LogicalType.Time(false, LogicalType.TimeUnit.MICROS))),
                17);
    }

    private static ShreddedVariant.Scalar timestampScalar(int typeId) {
        return new ShreddedVariant.Scalar(
                primitive(
                        PrimitiveKind.INT64, Optional.of(new LogicalType.Timestamp(true, LogicalType.TimeUnit.MICROS))),
                typeId);
    }

    private static ShreddedVariant.Scalar decimal4Scalar(int scale) {
        return new ShreddedVariant.Scalar(
                primitive(PrimitiveKind.INT32, Optional.of(new LogicalType.Decimal(scale, 9))), 8);
    }

    private static ShreddedVariant.Scalar decimal8Scalar(int scale) {
        return new ShreddedVariant.Scalar(
                primitive(PrimitiveKind.INT64, Optional.of(new LogicalType.Decimal(scale, 18))), 9);
    }

    private static ShreddedVariant.Scalar decimal16Scalar(int scale) {
        return new ShreddedVariant.Scalar(fixedLen(16, Optional.of(new LogicalType.Decimal(scale, 38))), 10);
    }

    private static SchemaNode.Primitive primitive(PrimitiveKind kind, Optional<LogicalType> logicalType) {
        return new SchemaNode.Primitive("typed_value", Repetition.OPTIONAL, kind, OptionalInt.empty(), logicalType, 1);
    }

    private static SchemaNode.Primitive fixedLen(int length, Optional<LogicalType> logicalType) {
        return new SchemaNode.Primitive(
                "typed_value",
                Repetition.OPTIONAL,
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                OptionalInt.of(length),
                logicalType,
                1);
    }
}
