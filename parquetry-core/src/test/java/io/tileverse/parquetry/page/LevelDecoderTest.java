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
package io.tileverse.parquetry.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

class LevelDecoderTest {

    @Test
    void computeBitWidth() {
        assertThat(LevelDecoder.computeBitWidth(0)).isZero();
        assertThat(LevelDecoder.computeBitWidth(1)).isEqualTo(1);
        assertThat(LevelDecoder.computeBitWidth(2)).isEqualTo(2);
        assertThat(LevelDecoder.computeBitWidth(3)).isEqualTo(2);
        assertThat(LevelDecoder.computeBitWidth(4)).isEqualTo(3);
        assertThat(LevelDecoder.computeBitWidth(7)).isEqualTo(3);
        assertThat(LevelDecoder.computeBitWidth(8)).isEqualTo(4);
        assertThat(LevelDecoder.computeBitWidth(255)).isEqualTo(8);
    }

    @Test
    void bitWidthZeroAlwaysReturnsZero() {
        LevelDecoder d = new LevelDecoder(0);
        d.load(ByteBuffer.wrap(new byte[0]));
        for (int i = 0; i < 100; i++) {
            assertThat(d.next()).isZero();
        }
    }

    @Test
    void rleRunOfSingleByteValue() {
        // RLE run: header = (length=3) << 1 = 6 (0x06), then 1-byte value = 5 (0x05)
        // Decodes to [5, 5, 5]
        byte[] bytes = {0x06, 0x05};
        LevelDecoder d = new LevelDecoder(3);
        d.load(ByteBuffer.wrap(bytes));
        assertThat(d.next()).isEqualTo(5);
        assertThat(d.next()).isEqualTo(5);
        assertThat(d.next()).isEqualTo(5);
    }

    @Test
    void bitPackedRunOfThreeBitValues() {
        // bit-packed: groups=1 (8 values), bitWidth=3 -> 3 bytes after header.
        // Header = (groups=1) << 1 | 1 = 3 (0x03)
        // Values [0..7] at bitWidth=3, packed LSB-first:
        //   byte 0: val0=000, val1=001, bits[0..5] of val2=01 -> 0b01_001_000 = 0x48... wait
        // Let's trace carefully (LSB first per Parquet spec):
        //   val0=0 (000), bits 0-2
        //   val1=1 (001), bits 3-5
        //   val2=2 (010), bits 6-7 carry 2 bits (10), then bit 0 of next byte
        //   byte 0 = bits[0..7] = 000|001|10 = 0b10_001_000 = 0x88
        //   val2 still needs 1 more bit (0), val3=3 (011) bits 1-3, val4=4 (100) bits 4-6,
        //   val5=5 (101) bit 7 (first bit)
        //   byte 1 = 0 | 011_0 | 100_0 ... let me be precise:
        //     bit 0: remainder of val2 = bit2 = 0
        //     bits 1-3: val3=3 = 011
        //     bits 4-6: val4=4 = 100
        //     bit 7: first bit of val5=5 (101), bit0 = 1
        //   byte 1 = 1_100_011_0 = 0b1_100_0110 = 0xC6
        //   val5 still needs 2 more bits (10), val6=6 (110) 3 bits, val7=7 (111) 3 bits
        //   byte 2:
        //     bits 0-1: remainder of val5 = bits 1-2 of 101 = 10
        //     bits 2-4: val6=6 = 110
        //     bits 5-7: val7=7 = 111
        //   byte 2 = 111_110_10 = 0b11111010 = 0xFA
        // Canonical vector: [0x88, 0xC6, 0xFA]
        byte[] bytes = {0x03, (byte) 0x88, (byte) 0xC6, (byte) 0xFA};
        LevelDecoder d = new LevelDecoder(3);
        d.load(ByteBuffer.wrap(bytes));
        for (int expected = 0; expected < 8; expected++) {
            assertThat(d.next()).as("value at index " + expected).isEqualTo(expected);
        }
    }

    @Test
    void skipAdvancesPastValues() {
        // RLE run length 10 of value 7, bitWidth=3
        byte[] bytes = {(byte) (10 << 1), 0x07};
        LevelDecoder d = new LevelDecoder(3);
        d.load(ByteBuffer.wrap(bytes));
        d.skip(5);
        assertThat(d.next()).isEqualTo(7); // 6th value, still in run
    }
}
