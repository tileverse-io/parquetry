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
package io.tileverse.parquetry.internal.read.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.BitSet;

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
        d.load(MemorySegment.ofArray(new byte[0]));
        int[] dst = new int[100];
        d.decode(100, dst, 0);
        for (int value : dst) {
            assertThat(value).isZero();
        }
    }

    @Test
    void rleRunOfSingleByteValue() {
        // RLE run: header = (length=3) << 1 = 6 (0x06), then 1-byte value = 5 (0x05)
        // Decodes to [5, 5, 5]
        byte[] bytes = {0x06, 0x05};
        LevelDecoder d = new LevelDecoder(3);
        d.load(MemorySegment.ofArray(bytes));
        int[] dst = new int[3];
        d.decode(3, dst, 0);
        assertThat(dst).containsExactly(5, 5, 5);
    }

    @Test
    void bitPackedRunOfThreeBitValues() {
        // bit-packed: groups=1 (8 values), bitWidth=3 -> 3 bytes after header.
        // Header = (groups=1) << 1 | 1 = 3 (0x03)
        // Values [0..7] at bitWidth=3, packed LSB-first:
        //   val0=0 (000), bits 0-2
        //   val1=1 (001), bits 3-5
        //   val2=2 (010), bits 6-7 carry 2 bits (10), then bit 0 of next byte
        //   byte 0 = bits[0..7] = 000|001|10 = 0b10_001_000 = 0x88
        //   val2 still needs 1 more bit (0), val3=3 (011) bits 1-3, val4=4 (100) bits 4-6,
        //   val5=5 (101) bit 7 (first bit)
        //   byte 1:
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
        d.load(MemorySegment.ofArray(bytes));
        int[] dst = new int[8];
        d.decode(8, dst, 0);
        assertThat(dst).containsExactly(0, 1, 2, 3, 4, 5, 6, 7);
    }

    @Test
    void skipAdvancesPastValues() {
        // RLE run length 10 of value 7, bitWidth=3
        byte[] bytes = {(byte) (10 << 1), 0x07};
        LevelDecoder d = new LevelDecoder(3);
        d.load(MemorySegment.ofArray(bytes));
        d.skip(5);
        int[] dst = new int[1];
        d.decode(1, dst, 0);
        assertThat(dst[0]).as("6th value, still in run").isEqualTo(7);
    }

    @Test
    void bulkDecodeFillsArrayFromMultipleRuns() {
        // Build a level stream with: RLE run of 3 zeros, bit-packed run of 8 ones, RLE run of 2 zeros.
        // bitWidth = 1 for a single-level def stream (column max def = 1).
        LevelDecoder decoder = new LevelDecoder(1);
        // bit-width-1 levels: RLE run header = (3 << 1) | 0 = 6 (varint single byte). RLE value byte = 0.
        // Bit-packed run header = (1 << 1) | 1 = 3 (varint single byte) for groups=1 (8 values).
        // Bit-packed body: 0b11111111 = 0xff.
        // Trailing RLE run: header = (2 << 1) | 0 = 4 (varint single byte). RLE value byte = 0.
        decoder.load(MemorySegment.ofArray(new byte[] {6, 0, 3, (byte) 0xff, 4, 0}));

        int[] dst = new int[13];
        decoder.decode(13, dst, 0);

        assertThat(dst).containsExactly(0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0);
    }

    @Test
    void bulkDecodeFromMemorySegmentMatchesByteBuffer() {
        // The flat-optional def-level shape: RLE zeros, bit-packed ones, RLE zeros. Decoding the same
        // bytes from a MemorySegment must match decoding from a ByteBuffer.
        byte[] bytes = {6, 0, 3, (byte) 0xff, 4, 0};

        LevelDecoder fromSegment = new LevelDecoder(1);
        fromSegment.load(MemorySegment.ofArray(bytes));
        int[] segmentLevels = new int[13];
        fromSegment.decode(13, segmentLevels, 0);

        LevelDecoder fromBuffer = new LevelDecoder(1);
        fromBuffer.load(MemorySegment.ofArray(bytes));
        int[] bufferLevels = new int[13];
        fromBuffer.decode(13, bufferLevels, 0);

        assertThat(segmentLevels)
                .containsExactly(0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0)
                .isEqualTo(bufferLevels);
    }

    @Test
    void decodeEqualsSetsBitsWhereLevelMatchesTargetAcrossMixedRuns() {
        // RLE 3 zeros, bit-packed 8 ones, RLE 2 zeros -> levels [0,0,0,1,1,1,1,1,1,1,1,0,0].
        // With target=1 the validity bits are the eight middle ones (indices 3..10).
        byte[] bytes = {6, 0, 3, (byte) 0xff, 4, 0};
        LevelDecoder d = new LevelDecoder(1);
        d.load(MemorySegment.ofArray(bytes));

        BitSet validity = new BitSet(13);
        d.decodeEquals(13, 1, validity, 0);

        BitSet expected = new BitSet(13);
        expected.set(3, 11);
        assertThat(validity).isEqualTo(expected);
    }

    @Test
    void decodeEqualsBulkSetsAnRleRunOfTheTargetValue() {
        // RLE run of value 1, length 10, bitWidth 1: header = (10 << 1) | 0 = 20 (0x14), value byte = 1.
        byte[] bytes = {0x14, 0x01};
        LevelDecoder d = new LevelDecoder(1);
        d.load(MemorySegment.ofArray(bytes));

        BitSet validity = new BitSet(10);
        d.decodeEquals(10, 1, validity, 0);

        assertThat(validity.cardinality()).isEqualTo(10);
    }

    @Test
    void decodeEqualsHonorsTheBaseOffset() {
        // RLE run of value 1, length 4: header = (4 << 1) = 8, value byte = 1.
        byte[] bytes = {0x08, 0x01};
        LevelDecoder d = new LevelDecoder(1);
        d.load(MemorySegment.ofArray(bytes));

        BitSet validity = new BitSet(10);
        d.decodeEquals(4, 1, validity, 5);

        BitSet expected = new BitSet(10);
        expected.set(5, 9);
        assertThat(validity).isEqualTo(expected);
    }

    @Test
    void decodeValidityBitmapPacksPresenceLsbFirstAcrossMixedRuns() {
        // RLE 3 zeros, bit-packed 8 ones, RLE 2 zeros -> levels [0,0,0,1,1,1,1,1,1,1,1,0,0].
        // With target=1 the present rows are indices 3..10 (eight rows); the bitmap is LSB-first.
        byte[] bytes = {6, 0, 3, (byte) 0xff, 4, 0};
        LevelDecoder d = new LevelDecoder(1);
        d.load(MemorySegment.ofArray(bytes));

        MemorySegment bitmap = MemorySegment.ofArray(new byte[2]);
        int validCount = d.decodeValidityBitmap(13, 1, bitmap);

        assertThat(validCount).isEqualTo(8);
        for (int row = 0; row < 13; row++) {
            boolean present = bitSet(bitmap, row);
            assertThat(present).as("row %d presence", row).isEqualTo(row >= 3 && row <= 10);
        }
    }

    @Test
    void decodeValidityBitmapAllPresentIsOneRleRun() {
        // RLE run of value 1, length 10: header = (10 << 1) = 20 (0x14), value byte = 1.
        byte[] bytes = {0x14, 0x01};
        LevelDecoder d = new LevelDecoder(1);
        d.load(MemorySegment.ofArray(bytes));

        MemorySegment bitmap = MemorySegment.ofArray(new byte[2]);
        int validCount = d.decodeValidityBitmap(10, 1, bitmap);

        assertThat(validCount).isEqualTo(10);
        for (int row = 0; row < 10; row++) {
            assertThat(bitSet(bitmap, row)).as("row %d present", row).isTrue();
        }
    }

    @Test
    void decodeValidityBitmapBitWidthZeroAllPresentWhenTargetIsZero() {
        LevelDecoder d = new LevelDecoder(0);
        d.load(MemorySegment.ofArray(new byte[0]));

        MemorySegment bitmap = MemorySegment.ofArray(new byte[1]);
        int validCount = d.decodeValidityBitmap(5, 0, bitmap);

        assertThat(validCount).isEqualTo(5);
        for (int row = 0; row < 5; row++) {
            assertThat(bitSet(bitmap, row)).as("row %d present", row).isTrue();
        }
    }

    private static boolean bitSet(MemorySegment bitmap, int row) {
        int b = bitmap.get(java.lang.foreign.ValueLayout.JAVA_BYTE, row >>> 3) & 0xff;
        return ((b >>> (row & 7)) & 1) != 0;
    }
}
