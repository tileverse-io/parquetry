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
package io.tileverse.parquetry.format.codec;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies {@link CompactProtocolWriter} is a strict inverse of {@link CompactProtocolReader}: every byte sequence the
 * writer emits parses back through the reader to the same logical value.
 */
class CompactProtocolWriterTest {

    @Test
    void writesByteFieldAtSmallDelta() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeStructBegin();
        w.writeByteField((short) 1, (byte) 42);
        w.writeFieldStop();
        w.writeStructEnd();

        byte[] bytes = out.toByteArray();
        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(bytes));
        FieldHeader fh = r.readFieldHeader(0);
        assertThat(fh.fieldId()).isEqualTo((short) 1);
        assertThat(fh.type()).isEqualTo(CompactType.BYTE);
        assertThat((byte) r.readByte()).isEqualTo((byte) 42);
        FieldHeader stop = r.readFieldHeader(fh.fieldId());
        assertThat(stop.isStop()).isTrue();
    }

    @Test
    void writesStandaloneBoolAsSingleByte() throws IOException {
        // Standalone bool (list / set / map element) writes a raw 0x01 / 0x00 byte, paired with readBool().
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeBool(true);
        w.writeBool(false);

        byte[] bytes = out.toByteArray();
        assertThat(bytes).containsExactly((byte) 0x01, (byte) 0x00);

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(bytes));
        assertThat(r.readBool()).isTrue();
        assertThat(r.readBool()).isFalse();
    }

    @Test
    void writesBoolFieldEncodesValueInTypeNibble() throws IOException {
        // BOOLEAN_TRUE / BOOLEAN_FALSE are encoded directly in the field header type nibble; no payload follows.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeStructBegin();
        w.writeBoolField((short) 1, true);
        w.writeBoolField((short) 2, false);
        w.writeFieldStop();
        w.writeStructEnd();

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        FieldHeader f1 = r.readFieldHeader(0);
        assertThat(f1.fieldId()).isEqualTo((short) 1);
        assertThat(f1.type()).isEqualTo(CompactType.BOOLEAN_TRUE);
        FieldHeader f2 = r.readFieldHeader(f1.fieldId());
        assertThat(f2.fieldId()).isEqualTo((short) 2);
        assertThat(f2.type()).isEqualTo(CompactType.BOOLEAN_FALSE);
        FieldHeader stop = r.readFieldHeader(f2.fieldId());
        assertThat(stop.isStop()).isTrue();
    }

    @Test
    void writesFieldIdDeltaFifteenPacksIntoHighNibble() throws IOException {
        // delta=15 is the largest value that fits in the 4-bit nibble; must use packed form, not explicit-id.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeStructBegin();
        w.writeI32Field((short) 1, 7);
        w.writeI32Field((short) 16, 8); // delta=15 from field 1
        w.writeFieldStop();
        w.writeStructEnd();

        // Packed form: each header is 1 byte (delta|type); no explicit-id varint follows.
        assertThat(out.toByteArray()).hasSize(5);

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        FieldHeader f1 = r.readFieldHeader(0);
        assertThat(f1.fieldId()).isEqualTo((short) 1);
        assertThat(r.readI32()).isEqualTo(7);
        FieldHeader f16 = r.readFieldHeader(f1.fieldId());
        assertThat(f16.fieldId()).isEqualTo((short) 16);
        assertThat(r.readI32()).isEqualTo(8);
    }

    @Test
    void writesFieldIdDeltaSixteenUsesExplicitId() throws IOException {
        // delta=16 exceeds the 4-bit nibble; must fall through to the type-byte + zigzag-varint form.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeStructBegin();
        w.writeI32Field((short) 1, 7);
        w.writeI32Field((short) 17, 8); // delta=16 from field 1
        w.writeFieldStop();
        w.writeStructEnd();

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        FieldHeader f1 = r.readFieldHeader(0);
        assertThat(f1.fieldId()).isEqualTo((short) 1);
        assertThat(r.readI32()).isEqualTo(7);
        FieldHeader f17 = r.readFieldHeader(f1.fieldId());
        assertThat(f17.fieldId()).isEqualTo((short) 17);
        assertThat(r.readI32()).isEqualTo(8);
    }

    @Test
    void writesFieldIdsLargerThanDeltaWindowAsZigzag() throws IOException {
        // A jump larger than 15 between consecutive field ids must fall back to the explicit-id encoding.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeStructBegin();
        w.writeI32Field((short) 1, 100);
        w.writeI32Field((short) 1000, 200);
        w.writeFieldStop();
        w.writeStructEnd();

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        FieldHeader f1 = r.readFieldHeader(0);
        assertThat(f1.fieldId()).isEqualTo((short) 1);
        assertThat(r.readI32()).isEqualTo(100);
        FieldHeader f2 = r.readFieldHeader(f1.fieldId());
        assertThat(f2.fieldId()).isEqualTo((short) 1000);
        assertThat(r.readI32()).isEqualTo(200);
    }

    @Test
    void writesNegativeFieldIdAsZigzag() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeStructBegin();
        w.writeI32Field((short) -7, 99);
        w.writeFieldStop();
        w.writeStructEnd();

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        FieldHeader f1 = r.readFieldHeader(0);
        assertThat(f1.fieldId()).isEqualTo((short) -7);
        assertThat(r.readI32()).isEqualTo(99);
    }

    @Test
    void writesNestedStructsResetsFieldIdDeltaPerScope() throws IOException {
        // Each struct scope must track its own lastFieldId; nesting then exiting must restore the parent scope.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeStructBegin();
        w.writeI32Field((short) 1, 10);
        w.writeFieldBegin((short) 2, CompactType.STRUCT);
        w.writeStructBegin();
        w.writeI32Field((short) 1, 20);
        w.writeFieldStop();
        w.writeStructEnd();
        w.writeI32Field((short) 3, 30);
        w.writeFieldStop();
        w.writeStructEnd();

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        FieldHeader outer1 = r.readFieldHeader(0);
        assertThat(outer1.fieldId()).isEqualTo((short) 1);
        assertThat(r.readI32()).isEqualTo(10);
        FieldHeader outer2 = r.readFieldHeader(outer1.fieldId());
        assertThat(outer2.type()).isEqualTo(CompactType.STRUCT);
        FieldHeader inner1 = r.readFieldHeader(0);
        assertThat(inner1.fieldId()).isEqualTo((short) 1);
        assertThat(r.readI32()).isEqualTo(20);
        FieldHeader innerStop = r.readFieldHeader(inner1.fieldId());
        assertThat(innerStop.isStop()).isTrue();
        FieldHeader outer3 = r.readFieldHeader(outer2.fieldId());
        assertThat(outer3.fieldId()).isEqualTo((short) 3);
        assertThat(r.readI32()).isEqualTo(30);
    }

    @Test
    void writesListOfI32() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeStructBegin();
        w.writeListField((short) 1, CompactType.I32, List.of(1, 2, 3), CompactProtocolWriter::writeI32);
        w.writeFieldStop();
        w.writeStructEnd();

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        FieldHeader fh = r.readFieldHeader(0);
        assertThat(fh.type()).isEqualTo(CompactType.LIST);
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        assertThat(lh.size()).isEqualTo(3);
        assertThat(lh.elementType()).isEqualTo(CompactType.I32);
        assertThat(r.readI32()).isEqualTo(1);
        assertThat(r.readI32()).isEqualTo(2);
        assertThat(r.readI32()).isEqualTo(3);
    }

    @Test
    void writesListSizeFourteenUsesShortForm() throws IOException {
        // size=14 is the last value that fits in the high nibble; must NOT use the varint long form.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeListBegin(CompactType.I32, 14);
        w.writeListEnd();

        // Single header byte, no varint follows.
        assertThat(out.toByteArray()).hasSize(1);

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        assertThat(lh.size()).isEqualTo(14);
        assertThat(lh.elementType()).isEqualTo(CompactType.I32);
    }

    @Test
    void writesListSizeFifteenUsesVarintLongForm() throws IOException {
        // size=15 is the first value that requires the 0xF0|type + varint long form.
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeListBegin(CompactType.I32, 15);
        w.writeListEnd();

        // 0xF0|type sentinel + varint(15).
        assertThat(out.toByteArray()).hasSize(2);

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        assertThat(lh.size()).isEqualTo(15);
        assertThat(lh.elementType()).isEqualTo(CompactType.I32);
    }

    @Test
    void writesListWithMoreThanFourteenElementsUsesVarintSize() throws IOException {
        // The size nibble can hold 0..14; 15 or more spills over into a varint long form.
        int count = 20;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeListBegin(CompactType.I32, count);
        for (int i = 0; i < count; i++) {
            w.writeI32(i);
        }
        w.writeListEnd();

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        assertThat(lh.size()).isEqualTo(count);
        assertThat(lh.elementType()).isEqualTo(CompactType.I32);
        for (int i = 0; i < count; i++) {
            assertThat(r.readI32()).isEqualTo(i);
        }
    }

    @Test
    void writesEmptyList() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeListBegin(CompactType.I32, 0);
        w.writeListEnd();

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        CompactProtocolReader.ListHeader lh = r.readListHeader();
        assertThat(lh.size()).isZero();
        assertThat(lh.elementType()).isEqualTo(CompactType.I32);
    }

    @Test
    void writesStringFieldUtf8() throws IOException {
        // UTF-8 round-trip through readString; covers the binary/length-prefix machinery.
        String value = "héllo 世界";
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeStructBegin();
        w.writeStringField((short) 1, value);
        w.writeFieldStop();
        w.writeStructEnd();

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        FieldHeader fh = r.readFieldHeader(0);
        assertThat(fh.type()).isEqualTo(CompactType.BINARY);
        assertThat(r.readString()).isEqualTo(value);
    }

    @Test
    void writesBinaryFieldAsMemorySegment() throws IOException {
        byte[] payload = {1, 2, 3, 4, 5};
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeStructBegin();
        w.writeBinaryField((short) 1, payload);
        w.writeFieldStop();
        w.writeStructEnd();

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        FieldHeader fh = r.readFieldHeader(0);
        assertThat(fh.type()).isEqualTo(CompactType.BINARY);
        MemorySegment seg = r.readBinary();
        assertThat(seg.toArray(JAVA_BYTE)).isEqualTo(payload);
    }

    @ParameterizedTest
    @ValueSource(
            ints = {
                0,
                1,
                -1,
                2,
                -2,
                63,
                -64,
                64,
                -65,
                127,
                -128,
                16_383,
                -16_384,
                Integer.MAX_VALUE - 1,
                Integer.MAX_VALUE,
                Integer.MIN_VALUE + 1,
                Integer.MIN_VALUE
            })
    void roundTripI32(int value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeI32(value);

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        assertThat(r.readI32()).isEqualTo(value);
    }

    @ParameterizedTest
    @ValueSource(
            longs = {
                0L,
                1L,
                -1L,
                63L,
                -64L,
                64L,
                -65L,
                127L,
                -128L,
                16_383L,
                -16_384L,
                Integer.MAX_VALUE,
                Integer.MIN_VALUE,
                (long) Integer.MAX_VALUE + 1L,
                (long) Integer.MIN_VALUE - 1L,
                Long.MAX_VALUE - 1L,
                Long.MAX_VALUE,
                Long.MIN_VALUE + 1L,
                Long.MIN_VALUE
            })
    void roundTripI64(long value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeI64(value);

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        assertThat(r.readI64()).isEqualTo(value);
    }

    @ParameterizedTest
    @ValueSource(
            doubles = {
                0.0,
                -0.0,
                1.0,
                -1.0,
                Math.PI,
                -Math.PI,
                Double.MIN_VALUE,
                Double.MIN_NORMAL,
                Double.MAX_VALUE,
                -Double.MAX_VALUE,
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY,
                Double.NaN
            })
    void roundTripDouble(double value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeDouble(value);

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        double decoded = r.readDouble();
        if (Double.isNaN(value)) {
            assertThat(decoded).isNaN();
        } else {
            assertThat(decoded).isEqualTo(value);
        }
    }

    @ParameterizedTest
    @MethodSource("byteVectors")
    void roundTripByte(byte value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeByte(value);

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        assertThat((byte) r.readByte()).isEqualTo(value);
    }

    static Stream<Byte> byteVectors() {
        return Stream.of((byte) 0, (byte) 1, (byte) -1, (byte) 127, (byte) -128, (byte) 0x55, (byte) 0xAA);
    }

    @ParameterizedTest
    @MethodSource("stringVectors")
    void roundTripString(String value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeString(value);

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        assertThat(r.readString()).isEqualTo(value);
    }

    static Stream<String> stringVectors() {
        return Stream.of(
                "", "a", "hello", "héllo 世界", "line1\nline2\ttab", "x".repeat(127), "y".repeat(128), "z".repeat(4096));
    }

    @ParameterizedTest
    @MethodSource("binaryVectors")
    void roundTripBinary(byte[] value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeBinary(value);

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        MemorySegment seg = r.readBinary();
        assertThat(seg.toArray(JAVA_BYTE)).isEqualTo(value);
    }

    static Stream<byte[]> binaryVectors() {
        byte[] big = new byte[4096];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) (i & 0xff);
        }
        return Stream.of(
                new byte[0],
                new byte[] {0},
                new byte[] {1, 2, 3, 4, 5},
                "binary".getBytes(StandardCharsets.UTF_8),
                big);
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1, -1, 127, -128, 1000, -1000, Short.MAX_VALUE, Short.MIN_VALUE})
    void roundTripI16(short value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CompactProtocolWriter w = new CompactProtocolWriter(out);
        w.writeI16(value);

        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(out.toByteArray()));
        // i16 is zigzag-varint just like i32; reader exposes readI32 for that wire form.
        assertThat((short) r.readI32()).isEqualTo(value);
    }
}
