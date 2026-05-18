/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.page.plain;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.page.PageDecoder;

class PlainBooleanDecoderTest {

    /**
     * Verify basic LSB-first bit packing within a single byte.
     *
     * <p>Byte 0 = 0b10101010 = 0xAA: bits 0,2,4,6 are 0; bits 1,3,5,7 are 1.
     * Value order: false, true, false, true, false, true, false, true.
     */
    @Test
    void decodesLsbFirstWithinOneByte() {
        // 0xAA = 1010_1010 => bit0=0, bit1=1, bit2=0, bit3=1, ...
        ByteBuffer page = ByteBuffer.wrap(new byte[] {(byte) 0xAA});

        PageDecoder<Boolean> decoder = new PlainBooleanDecoder();
        decoder.load(page, 8);

        assertThat(decoder.next()).isFalse();
        assertThat(decoder.next()).isTrue();
        assertThat(decoder.next()).isFalse();
        assertThat(decoder.next()).isTrue();
        assertThat(decoder.next()).isFalse();
        assertThat(decoder.next()).isTrue();
        assertThat(decoder.next()).isFalse();
        assertThat(decoder.next()).isTrue();
    }

    /**
     * Verify crossing the byte boundary: 16 values across 2 bytes.
     *
     * <p>Byte 0 = 0b01010101 = 0x55: alternating F/T starting at T (bit0=1).
     * Byte 1 = 0b11111111 = 0xFF: all eight values true.
     */
    @Test
    void decodesCrossesByteBounderCorrectly() {
        // Byte 0: 0x55 = 0101_0101 => bit0=1,bit1=0,bit2=1,bit3=0,bit4=1,bit5=0,bit6=1,bit7=0
        // Byte 1: 0xFF = 1111_1111 => all 8 bits = 1
        ByteBuffer page = ByteBuffer.wrap(new byte[] {(byte) 0x55, (byte) 0xFF});

        PageDecoder<Boolean> decoder = new PlainBooleanDecoder();
        decoder.load(page, 16);

        // Byte 0 values (alternating T/F, bit0=1)
        assertThat(decoder.next()).isTrue();
        assertThat(decoder.next()).isFalse();
        assertThat(decoder.next()).isTrue();
        assertThat(decoder.next()).isFalse();
        assertThat(decoder.next()).isTrue();
        assertThat(decoder.next()).isFalse();
        assertThat(decoder.next()).isTrue();
        assertThat(decoder.next()).isFalse();

        // Byte 1 values (all true)
        for (int i = 0; i < 8; i++) {
            assertThat(decoder.next()).isTrue();
        }
    }

    @Test
    void skipAdvancesBitCursor() {
        // 0x0F = 0000_1111: bits 0-3 = 1, bits 4-7 = 0
        ByteBuffer page = ByteBuffer.wrap(new byte[] {(byte) 0x0F});

        PageDecoder<Boolean> decoder = new PlainBooleanDecoder();
        decoder.load(page, 8);
        decoder.skip(4);

        // After skipping 4 (which were all true), we should read the next 4 (all false)
        assertThat(decoder.next()).isFalse();
        assertThat(decoder.next()).isFalse();
        assertThat(decoder.next()).isFalse();
        assertThat(decoder.next()).isFalse();
    }

    @Test
    void allFalseAllZeroByte() {
        ByteBuffer page = ByteBuffer.wrap(new byte[] {0x00});

        PageDecoder<Boolean> decoder = new PlainBooleanDecoder();
        decoder.load(page, 8);

        for (int i = 0; i < 8; i++) {
            assertThat(decoder.next()).isFalse();
        }
    }

    @Test
    void allTrueAllOneByte() {
        ByteBuffer page = ByteBuffer.wrap(new byte[] {(byte) 0xFF});

        PageDecoder<Boolean> decoder = new PlainBooleanDecoder();
        decoder.load(page, 8);

        for (int i = 0; i < 8; i++) {
            assertThat(decoder.next()).isTrue();
        }
    }
}
