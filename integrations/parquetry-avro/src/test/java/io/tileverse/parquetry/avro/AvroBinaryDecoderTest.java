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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

class AvroBinaryDecoderTest {

    private static AvroBinaryDecoder over(int... unsignedBytes) {
        byte[] bytes = new byte[unsignedBytes.length];
        for (int i = 0; i < unsignedBytes.length; i++) {
            bytes[i] = (byte) unsignedBytes[i];
        }
        return new AvroBinaryDecoder(MemorySegment.ofArray(bytes).asReadOnly());
    }

    @Test
    void readsZigZagLongs() {
        assertThat(over(0x00).readLong()).isZero();
        assertThat(over(0x01).readLong()).isEqualTo(-1L);
        assertThat(over(0x02).readLong()).isEqualTo(1L);
        assertThat(over(0x03).readLong()).isEqualTo(-2L);
        assertThat(over(0xD8, 0x04).readLong()).isEqualTo(300L);
    }

    @Test
    void readsBoolean() {
        assertThat(over(0x01).readBoolean()).isTrue();
        assertThat(over(0x00).readBoolean()).isFalse();
    }

    @Test
    void readsLittleEndianDouble() {
        assertThat(over(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F).readDouble())
                .isEqualTo(1.0d);
    }

    @Test
    void readsLittleEndianFloat() {
        assertThat(over(0x00, 0x00, 0x80, 0x3F).readFloat()).isEqualTo(1.0f);
    }

    @Test
    void readsStringWithLengthPrefix() {
        assertThat(over(0x06, 'a', 'b', 'c').readString()).isEqualTo("abc");
    }

    @Test
    void readsBytesAsReadOnlySegmentCopy() {
        MemorySegment bytes = over(0x04, 0xDE, 0xAD).readBytes();
        assertThat(bytes.isReadOnly()).isTrue();
        assertThat(bytes.byteSize()).isEqualTo(2);
        assertThat(bytes.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE)).containsExactly((byte) 0xDE, (byte) 0xAD);
    }

    @Test
    void readsIntWithinRange() {
        assertThat(over(0xD8, 0x04).readInt()).isEqualTo(300);
        assertThat(over(0x03).readInt()).isEqualTo(-2);
    }

    @Test
    void rejectsIntOutOfRange() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        OcfTestWriter.varLong(out, Long.MAX_VALUE);
        AvroBinaryDecoder decoder =
                new AvroBinaryDecoder(MemorySegment.ofArray(out.toByteArray()).asReadOnly());
        assertThatThrownBy(decoder::readInt)
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("int range");
    }

    @Test
    void rejectsVarintRunningPastEnd() {
        AvroBinaryDecoder decoder = over(0x80, 0x80);
        assertThatThrownBy(decoder::readLong).isInstanceOf(AvroFormatException.class);
    }

    @Test
    void rejectsTooLongVarint() {
        AvroBinaryDecoder decoder = over(0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01);
        assertThatThrownBy(decoder::readLong)
                .isInstanceOf(AvroFormatException.class)
                .hasMessageContaining("Varint too long");
    }

    @Test
    void rejectsTruncatedDouble() {
        AvroBinaryDecoder decoder = over(0x00, 0x00, 0x00);
        assertThatThrownBy(decoder::readDouble).isInstanceOf(AvroFormatException.class);
    }

    @Test
    void rejectsTruncatedFloat() {
        AvroBinaryDecoder decoder = over(0x00);
        assertThatThrownBy(decoder::readFloat).isInstanceOf(AvroFormatException.class);
    }

    @Test
    void utf8RoundTrips() {
        byte[] payload = "héllo".getBytes(StandardCharsets.UTF_8);
        byte[] framed = new byte[payload.length + 1];
        framed[0] = (byte) (payload.length << 1);
        System.arraycopy(payload, 0, framed, 1, payload.length);
        AvroBinaryDecoder decoder =
                new AvroBinaryDecoder(MemorySegment.ofArray(framed).asReadOnly());
        assertThat(decoder.readString()).isEqualTo("héllo");
    }
}
