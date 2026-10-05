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
package io.tileverse.parquetry.internal.read.page;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.MalformedFileException;

class RleBooleanDecoderTest {

    @Test
    void decodesRleRunOfTrues() {
        // RLE run length 10 of value 1 (true), bitWidth=1
        // Body: [varint(10<<1=20)=0x14] [1 byte value: 0x01]
        // Total body: 2 bytes
        ByteBuffer page = ByteBuffer.allocate(6).order(LITTLE_ENDIAN);
        page.putInt(2); // length prefix
        page.put((byte) 0x14); // RLE header
        page.put((byte) 0x01); // value
        page.flip();

        PageDecoder<Boolean> decoder = new RleBooleanDecoder();
        decoder.load(MemorySegment.ofBuffer(page), 10);
        for (int i = 0; i < 10; i++) {
            assertThat(decoder.next()).as("value at index " + i).isTrue();
        }
    }

    @Test
    void decodesBitPackedAlternating() {
        // 8 values [F,T,F,T,F,T,F,T] = bits 0,1,0,1,0,1,0,1 packed LSB-first in 1 byte = 0xAA
        // Header: groups=1, low bit set -> (1<<1)|1 = 3 = 0x03; then 1 byte 0xAA
        ByteBuffer page = ByteBuffer.allocate(6).order(LITTLE_ENDIAN);
        page.putInt(2); // length prefix
        page.put((byte) 0x03); // bit-packed header
        page.put((byte) 0xAA); // 8 packed booleans
        page.flip();

        PageDecoder<Boolean> decoder = new RleBooleanDecoder();
        decoder.load(MemorySegment.ofBuffer(page), 8);
        boolean[] expected = {false, true, false, true, false, true, false, true};
        for (int i = 0; i < 8; i++) {
            assertThat(decoder.next()).as("value at index " + i).isEqualTo(expected[i]);
        }
    }

    @Test
    void skipAdvancesPastValues() {
        // RLE run of 20 trues, length prefix 2, header 0x28 (20<<1), value 1
        ByteBuffer page = ByteBuffer.allocate(6).order(LITTLE_ENDIAN);
        page.putInt(2);
        page.put((byte) 0x28);
        page.put((byte) 0x01);
        page.flip();

        PageDecoder<Boolean> decoder = new RleBooleanDecoder();
        decoder.load(MemorySegment.ofBuffer(page), 20);
        decoder.skip(15);
        for (int i = 0; i < 5; i++) {
            assertThat(decoder.next()).isTrue();
        }
    }

    @Test
    void bulkDecodeFromMixedRuns() {
        // Build RLE-bit-packed for booleans (bitWidth=1).
        // Run 1: RLE of 5 trues  -> varint header (5 << 1) | 0 = 10; RLE value byte 0x01.
        // Run 2: bit-packed group of 8 (1 group) -> header (1 << 1) | 1 = 3; packed byte 0b11110000 (alternating).
        ByteBuffer page = ByteBuffer.allocate(8).order(LITTLE_ENDIAN);
        page.putInt(4); // length prefix (4 bytes of RLE payload)
        page.put((byte) 10);
        page.put((byte) 0x01);
        page.put((byte) 3);
        page.put((byte) 0b11110000);
        page.flip();

        PageDecoder<Boolean> decoder = new RleBooleanDecoder();
        decoder.load(MemorySegment.ofBuffer(page), 13);

        boolean[] dst = new boolean[13];
        decoder.decodeBooleans(13, dst, 0);

        assertThat(dst)
                .containsExactly(
                        true, true, true, true, true, // RLE run of 5 trues
                        false, false, false, false, true, true, true, true); // bit-packed LSB-first
    }

    static Stream<Arguments> malformedRlePages() {
        // each vector is a page and the expected failure
        return Stream.of(
                // a page shorter than its 4-byte length prefix
                Arguments.of(new byte[] {1, 0}, "too short for its 4-byte length prefix"),
                // a length prefix announcing 3 payload bytes with 1 present
                Arguments.of(new byte[] {3, 0, 0, 0, 0x06}, "declares a payload of 3 bytes but holds 1"),
                // a negative length prefix
                Arguments.of(
                        new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff},
                        "declares a payload of -1 bytes"));
    }

    @ParameterizedTest
    @MethodSource("malformedRlePages")
    void malformedLengthPrefixIsAFormatError(byte[] page, String failure) {
        RleBooleanDecoder decoder = new RleBooleanDecoder();

        assertThatThrownBy(() -> decoder.load(MemorySegment.ofArray(page), 1))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining(failure);
    }

    @Test
    void payloadShortOfItsValuesIsAFormatErrorNamingTheStream() {
        RleBooleanDecoder decoder = new RleBooleanDecoder();
        // a 2-byte payload holding an RLE run of 3 trues, then a fourth value requested
        decoder.load(MemorySegment.ofArray(new byte[] {2, 0, 0, 0, 0x06, 0x01}), 4);

        assertThatThrownBy(() -> decoder.decodeBooleans(4, new boolean[4], 0))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageStartingWith("RLE boolean values end at byte 2");
    }
}
