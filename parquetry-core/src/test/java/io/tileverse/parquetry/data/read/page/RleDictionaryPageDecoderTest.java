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
package io.tileverse.parquetry.data.read.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;

import org.junit.jupiter.api.Test;

class RleDictionaryPageDecoderTest {

    /**
     * Verify that indexes [0, 1, 2, 0, 1, 2] are correctly dereferenced against a three-element INT32 dictionary [10,
     * 20, 30].
     *
     * <p>Encoding: bit-packed at bitWidth=2, one group of 8 values (padded with 0,0).
     *
     * <p>Bit layout (LSB-first, 2 bits per value):
     *
     * <pre>
     *   values:  0  1  2  0  1  2  0  0   (8 values, last 2 are padding)
     *   bits:   00 01 10 00 01 10 00 00
     *   byte 0: bits[7..0] = 10_01_00_00 wait:
     *     bit 0-1: val0=0 -> 00
     *     bit 2-3: val1=1 -> 01
     *     bit 4-5: val2=2 -> 10
     *     bit 6-7: val3=0 -> 00
     *   byte 0 = 0b00_10_01_00 = 0x24
     *     bit 8-9:  val4=1 -> 01
     *     bit 10-11: val5=2 -> 10
     *     bit 12-13: pad=0 -> 00
     *     bit 14-15: pad=0 -> 00
     *   byte 1 = 0b00_00_10_01 = 0x09
     * </pre>
     */
    @Test
    void dereferencesIndexes() {
        Dictionary.IntDict dict = new Dictionary.IntDict(intBuf(10, 20, 30));

        // bit-packed header: (groups=1) << 1 | 1 = 3
        // byte 0: 0x24, byte 1: 0x09  (see layout in javadoc above)
        ByteBuffer page = ByteBuffer.wrap(new byte[] {
            2, // bit width
            3, // RLE header: bit-packed, 1 group of 8
            (byte) 0x24, // byte 0 of packed indexes
            (byte) 0x09 // byte 1 of packed indexes
        });

        PageDecoder<Integer> decoder = new RleDictionaryPageDecoder<>(dict);
        decoder.load(MemorySegment.ofBuffer(page), 6);

        assertThat(decoder.next()).isEqualTo(10); // index 0
        assertThat(decoder.next()).isEqualTo(20); // index 1
        assertThat(decoder.next()).isEqualTo(30); // index 2
        assertThat(decoder.next()).isEqualTo(10); // index 0
        assertThat(decoder.next()).isEqualTo(20); // index 1
        assertThat(decoder.next()).isEqualTo(30); // index 2
    }

    /**
     * Verify that skip correctly advances past values in the index stream.
     *
     * <p>Dictionary is [10, 20, 30]. Indexes [2, 0, 1, 2] encoded at bitWidth=2.
     *
     * <p>Bit layout:
     *
     * <pre>
     *   values:  2  0  1  2  0  0  0  0  (8 values, last 4 are padding)
     *   byte 0: bit 0-1: val0=2 (10), bit 2-3: val1=0 (00),
     *           bit 4-5: val2=1 (01), bit 6-7: val3=2 (10)
     *   byte 0 = 0b10_01_00_10 = 0x92
     *   byte 1 = 0x00 (padding)
     * </pre>
     *
     * After skip(1), next() reads index 0 -> value 10; then index 1 -> value 20.
     */
    @Test
    void skipAdvancesPastValues() {
        Dictionary.IntDict dict = new Dictionary.IntDict(intBuf(10, 20, 30));

        ByteBuffer page = ByteBuffer.wrap(new byte[] {
            2, // bit width
            3, // RLE header: bit-packed, 1 group of 8
            (byte) 0x92, // byte 0: indexes [2, 0, 1, 2]
            (byte) 0x00 // byte 1: padding zeros
        });

        PageDecoder<Integer> decoder = new RleDictionaryPageDecoder<>(dict);
        decoder.load(MemorySegment.ofBuffer(page), 4);

        decoder.skip(1); // skip index 2
        assertThat(decoder.next()).isEqualTo(10); // index 0
        assertThat(decoder.next()).isEqualTo(20); // index 1
        assertThat(decoder.next()).isEqualTo(30); // index 2
    }

    /** Verify that bulk decodeInts matches successive next() calls. */
    @Test
    void decodeIntsBulk() {
        Dictionary.IntDict dict = new Dictionary.IntDict(intBuf(10, 20, 30));

        // bit-packed header: (groups=1) << 1 | 1 = 3
        // byte 0: 0x24, byte 1: 0x09  (indexes [0, 1, 2, 0, 1, 2])
        ByteBuffer page = ByteBuffer.wrap(new byte[] {
            2, // bit width
            3, // RLE header: bit-packed, 1 group of 8
            (byte) 0x24, // byte 0 of packed indexes
            (byte) 0x09 // byte 1 of packed indexes
        });

        PageDecoder<Integer> decoder = new RleDictionaryPageDecoder<>(dict);
        decoder.load(MemorySegment.ofBuffer(page), 6);

        int[] dst = new int[6];
        decoder.decodeInts(6, dst, 0);

        assertThat(dst).containsExactly(10, 20, 30, 10, 20, 30);
    }

    private static IntBuffer intBuf(int... values) {
        return IntBuffer.wrap(values);
    }
}
