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
package io.tileverse.parquetry.iceberg;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Value;

class IcebergBoundsTest {

    @Test
    void decodesLong() {
        MemorySegment bytes = segment(longLe(1234567890123L));

        Value value = IcebergBounds.decodeValue("long", bytes);

        assertThat(value).isEqualTo(new Value.LongVal(1234567890123L));
    }

    @Test
    void decodesInt() {
        MemorySegment bytes = segment(intLe(42));

        Value value = IcebergBounds.decodeValue("int", bytes);

        assertThat(value).isEqualTo(new Value.IntVal(42));
    }

    @Test
    void decodesDouble() {
        MemorySegment bytes = segment(doubleLe(-122.5));

        Value value = IcebergBounds.decodeValue("double", bytes);

        assertThat(value).isEqualTo(new Value.DoubleVal(-122.5));
    }

    @Test
    void decodesString() {
        MemorySegment bytes = segment("abc".getBytes(StandardCharsets.UTF_8));

        Value value = IcebergBounds.decodeValue("string", bytes);

        assertThat(value).isEqualTo(new Value.StringVal("abc"));
    }

    @Test
    void decodesDate() {
        MemorySegment bytes = segment(intLe(19000));

        Value value = IcebergBounds.decodeValue("date", bytes);

        assertThat(value).isEqualTo(new Value.DateVal(LocalDate.ofEpochDay(19000)));
    }

    @Test
    void decodesBoolean() {
        MemorySegment bytes = segment(new byte[] {1});

        Value value = IcebergBounds.decodeValue("boolean", bytes);

        assertThat(value).isEqualTo(new Value.BoolVal(true));
    }

    @Test
    void rejectsUnsupportedType() {
        MemorySegment bytes = segment(new byte[16]);

        assertThatThrownBy(() -> IcebergBounds.decodeValue("decimal", bytes))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("decimal");
    }

    @Test
    void rejectsTruncatedLong() {
        MemorySegment bytes = segment(intLe(7));

        assertThatThrownBy(() -> IcebergBounds.decodeValue("long", bytes)).isInstanceOf(IcebergFormatException.class);
    }

    @Test
    void decodesPackedXyPoint() {
        byte[] raw = new byte[16];
        System.arraycopy(doubleLe(-122.5), 0, raw, 0, 8);
        System.arraycopy(doubleLe(37.5), 0, raw, 8, 8);

        double[] point = IcebergBounds.decodePoint(segment(raw));

        assertThat(point).containsExactly(-122.5, 37.5);
    }

    @Test
    void decodesWkbPoint() {
        byte[] raw = new byte[21];
        raw[0] = 0x01;
        raw[1] = 0x01;
        System.arraycopy(doubleLe(-122.5), 0, raw, 5, 8);
        System.arraycopy(doubleLe(37.5), 0, raw, 13, 8);

        double[] point = IcebergBounds.decodePoint(segment(raw));

        assertThat(point).containsExactly(-122.5, 37.5);
    }

    @Test
    void rejectsWrongGeometryLength() {
        MemorySegment bytes = segment(new byte[10]);

        assertThatThrownBy(() -> IcebergBounds.decodePoint(bytes))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("10");
    }

    private static MemorySegment segment(byte[] bytes) {
        return MemorySegment.ofArray(bytes);
    }

    private static byte[] intLe(int value) {
        return ByteBuffer.allocate(4).order(LITTLE_ENDIAN).putInt(value).array();
    }

    private static byte[] longLe(long value) {
        return ByteBuffer.allocate(8).order(LITTLE_ENDIAN).putLong(value).array();
    }

    private static byte[] doubleLe(double value) {
        return ByteBuffer.allocate(8).order(LITTLE_ENDIAN).putDouble(value).array();
    }
}
