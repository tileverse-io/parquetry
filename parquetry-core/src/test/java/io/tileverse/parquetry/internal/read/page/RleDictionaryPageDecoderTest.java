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
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.ParquetLayouts;

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

    /** Verify that segment-destination decodeInts dereferences indexes and leaves slots before dstIndex untouched. */
    @Test
    void decodeIntsBulkIntoSegment() {
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

        MemorySegment dst = MemorySegment.ofArray(new byte[6 * Integer.BYTES]);
        dst.setAtIndex(ParquetLayouts.INT32, 0, -1); // sentinel - must be left alone
        decoder.decodeInts(5, dst, 1);

        assertThat(dst.getAtIndex(ParquetLayouts.INT32, 0)).isEqualTo(-1);
        assertThat(dst.getAtIndex(ParquetLayouts.INT32, 1)).isEqualTo(10);
        assertThat(dst.getAtIndex(ParquetLayouts.INT32, 2)).isEqualTo(20);
        assertThat(dst.getAtIndex(ParquetLayouts.INT32, 3)).isEqualTo(30);
        assertThat(dst.getAtIndex(ParquetLayouts.INT32, 4)).isEqualTo(10);
        assertThat(dst.getAtIndex(ParquetLayouts.INT32, 5)).isEqualTo(20);
        assertThat(decoder.next()).isEqualTo(30);
    }

    /** Verify that decodeIndices returns the raw dictionary indexes, not the dereferenced values. */
    @Test
    void decodeIndicesReturnsRawIndexesNotValues() {
        Dictionary.IntDict dict = new Dictionary.IntDict(intBuf(10, 20, 30));

        // byte 0: 0x24, byte 1: 0x09  (indexes [0, 1, 2, 0, 1, 2])
        ByteBuffer page = ByteBuffer.wrap(new byte[] {
            2, // bit width
            3, // RLE header: bit-packed, 1 group of 8
            (byte) 0x24, // byte 0 of packed indexes
            (byte) 0x09 // byte 1 of packed indexes
        });

        RleDictionaryPageDecoder<Integer> decoder = new RleDictionaryPageDecoder<>(dict);
        decoder.load(MemorySegment.ofBuffer(page), 6);

        int[] dst = new int[6];
        decoder.decodeIndices(6, dst, 0);

        assertThat(dst).containsExactly(0, 1, 2, 0, 1, 2);
    }

    /** Verify that segment-destination decodeIndicesInto writes the raw indexes, not the dereferenced values. */
    @Test
    void decodeIndicesIntoSegmentWritesRawIndexesNotValues() {
        Dictionary.IntDict dict = new Dictionary.IntDict(intBuf(10, 20, 30));

        // byte 0: 0x24, byte 1: 0x09  (indexes [0, 1, 2, 0, 1, 2])
        ByteBuffer page = ByteBuffer.wrap(new byte[] {
            2, // bit width
            3, // RLE header: bit-packed, 1 group of 8
            (byte) 0x24, // byte 0 of packed indexes
            (byte) 0x09 // byte 1 of packed indexes
        });

        RleDictionaryPageDecoder<Integer> decoder = new RleDictionaryPageDecoder<>(dict);
        decoder.load(MemorySegment.ofBuffer(page), 6);

        MemorySegment dst = MemorySegment.ofArray(new byte[6 * Integer.BYTES]);
        dst.setAtIndex(ParquetLayouts.INT32, 5, -1); // sentinel - must be left alone
        decoder.decodeIndicesInto(5, dst);

        assertThat(dst.getAtIndex(ParquetLayouts.INT32, 0)).isEqualTo(0);
        assertThat(dst.getAtIndex(ParquetLayouts.INT32, 1)).isEqualTo(1);
        assertThat(dst.getAtIndex(ParquetLayouts.INT32, 2)).isEqualTo(2);
        assertThat(dst.getAtIndex(ParquetLayouts.INT32, 3)).isEqualTo(0);
        assertThat(dst.getAtIndex(ParquetLayouts.INT32, 4)).isEqualTo(1);
        assertThat(dst.getAtIndex(ParquetLayouts.INT32, 5)).isEqualTo(-1);
        assertThat(decoder.next()).isEqualTo(30); // the cursor sits on the sixth index (2)
    }

    /** Verify that bulk decodeDoubles dereferences indexes against a DOUBLE dictionary without boxing artifacts. */
    @Test
    void decodeDoublesBulk() {
        Dictionary.DoubleDict dict = new Dictionary.DoubleDict(doubleBuf(1.5, 2.5, 3.5));

        // byte 0: 0x24, byte 1: 0x09  (indexes [0, 1, 2, 0, 1, 2])
        ByteBuffer page = ByteBuffer.wrap(new byte[] {
            2, // bit width
            3, // RLE header: bit-packed, 1 group of 8
            (byte) 0x24, // byte 0 of packed indexes
            (byte) 0x09 // byte 1 of packed indexes
        });

        RleDictionaryPageDecoder<Double> decoder = new RleDictionaryPageDecoder<>(dict);
        decoder.load(MemorySegment.ofBuffer(page), 6);

        double[] dst = new double[6];
        decoder.decodeDoubles(6, dst, 0);

        assertThat(dst).containsExactly(1.5, 2.5, 3.5, 1.5, 2.5, 3.5);
    }

    /** Verify that segment-destination decodeDoubles dereferences indexes against a DOUBLE dictionary. */
    @Test
    void decodeDoublesBulkIntoSegment() {
        Dictionary.DoubleDict dict = new Dictionary.DoubleDict(doubleBuf(1.5, 2.5, 3.5));

        // byte 0: 0x24, byte 1: 0x09  (indexes [0, 1, 2, 0, 1, 2])
        ByteBuffer page = ByteBuffer.wrap(new byte[] {
            2, // bit width
            3, // RLE header: bit-packed, 1 group of 8
            (byte) 0x24, // byte 0 of packed indexes
            (byte) 0x09 // byte 1 of packed indexes
        });

        RleDictionaryPageDecoder<Double> decoder = new RleDictionaryPageDecoder<>(dict);
        decoder.load(MemorySegment.ofBuffer(page), 6);

        MemorySegment dst = MemorySegment.ofArray(new byte[6 * Double.BYTES]);
        dst.setAtIndex(ParquetLayouts.DOUBLE, 0, -1.0); // sentinel - must be left alone
        decoder.decodeDoubles(5, dst, 1);

        assertThat(dst.getAtIndex(ParquetLayouts.DOUBLE, 0)).isEqualTo(-1.0);
        assertThat(dst.getAtIndex(ParquetLayouts.DOUBLE, 1)).isEqualTo(1.5);
        assertThat(dst.getAtIndex(ParquetLayouts.DOUBLE, 2)).isEqualTo(2.5);
        assertThat(dst.getAtIndex(ParquetLayouts.DOUBLE, 3)).isEqualTo(3.5);
        assertThat(dst.getAtIndex(ParquetLayouts.DOUBLE, 4)).isEqualTo(1.5);
        assertThat(dst.getAtIndex(ParquetLayouts.DOUBLE, 5)).isEqualTo(2.5);
        assertThat(decoder.next()).isEqualTo(3.5);
    }

    /** Verify that segment-destination decodeLongs dereferences indexes against an INT64 dictionary. */
    @Test
    void decodeLongsBulkIntoSegment() {
        Dictionary.LongDict dict = new Dictionary.LongDict(longBuf(100L, 200L, 300L));

        // byte 0: 0x24, byte 1: 0x09  (indexes [0, 1, 2, 0, 1, 2])
        ByteBuffer page = ByteBuffer.wrap(new byte[] {
            2, // bit width
            3, // RLE header: bit-packed, 1 group of 8
            (byte) 0x24, // byte 0 of packed indexes
            (byte) 0x09 // byte 1 of packed indexes
        });

        RleDictionaryPageDecoder<Long> decoder = new RleDictionaryPageDecoder<>(dict);
        decoder.load(MemorySegment.ofBuffer(page), 6);

        MemorySegment dst = MemorySegment.ofArray(new byte[6 * Long.BYTES]);
        dst.setAtIndex(ParquetLayouts.INT64, 0, -1L); // sentinel - must be left alone
        decoder.decodeLongs(5, dst, 1);

        assertThat(dst.getAtIndex(ParquetLayouts.INT64, 0)).isEqualTo(-1L);
        assertThat(dst.getAtIndex(ParquetLayouts.INT64, 1)).isEqualTo(100L);
        assertThat(dst.getAtIndex(ParquetLayouts.INT64, 2)).isEqualTo(200L);
        assertThat(dst.getAtIndex(ParquetLayouts.INT64, 3)).isEqualTo(300L);
        assertThat(dst.getAtIndex(ParquetLayouts.INT64, 4)).isEqualTo(100L);
        assertThat(dst.getAtIndex(ParquetLayouts.INT64, 5)).isEqualTo(200L);
        assertThat(decoder.next()).isEqualTo(300L);
    }

    /** Verify that segment-destination decodeFloats dereferences indexes against a FLOAT dictionary. */
    @Test
    void decodeFloatsBulkIntoSegment() {
        Dictionary.FloatDict dict = new Dictionary.FloatDict(floatBuf(1.5f, 2.5f, 3.5f));

        // byte 0: 0x24, byte 1: 0x09  (indexes [0, 1, 2, 0, 1, 2])
        ByteBuffer page = ByteBuffer.wrap(new byte[] {
            2, // bit width
            3, // RLE header: bit-packed, 1 group of 8
            (byte) 0x24, // byte 0 of packed indexes
            (byte) 0x09 // byte 1 of packed indexes
        });

        RleDictionaryPageDecoder<Float> decoder = new RleDictionaryPageDecoder<>(dict);
        decoder.load(MemorySegment.ofBuffer(page), 6);

        MemorySegment dst = MemorySegment.ofArray(new byte[6 * Float.BYTES]);
        dst.setAtIndex(ParquetLayouts.FLOAT, 0, -1f); // sentinel - must be left alone
        decoder.decodeFloats(5, dst, 1);

        assertThat(dst.getAtIndex(ParquetLayouts.FLOAT, 0)).isEqualTo(-1f);
        assertThat(dst.getAtIndex(ParquetLayouts.FLOAT, 1)).isEqualTo(1.5f);
        assertThat(dst.getAtIndex(ParquetLayouts.FLOAT, 2)).isEqualTo(2.5f);
        assertThat(dst.getAtIndex(ParquetLayouts.FLOAT, 3)).isEqualTo(3.5f);
        assertThat(dst.getAtIndex(ParquetLayouts.FLOAT, 4)).isEqualTo(1.5f);
        assertThat(dst.getAtIndex(ParquetLayouts.FLOAT, 5)).isEqualTo(2.5f);
        assertThat(decoder.next()).isEqualTo(3.5f);
    }

    private static IntBuffer intBuf(int... values) {
        return IntBuffer.wrap(values);
    }

    private static LongBuffer longBuf(long... values) {
        return LongBuffer.wrap(values);
    }

    private static FloatBuffer floatBuf(float... values) {
        return FloatBuffer.wrap(values);
    }

    private static DoubleBuffer doubleBuf(double... values) {
        return DoubleBuffer.wrap(values);
    }
}
