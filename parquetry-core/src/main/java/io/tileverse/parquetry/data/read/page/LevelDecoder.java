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
package io.tileverse.parquetry.data.read.page;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.util.BitSet;

/**
 * Decodes Parquet repetition and definition level streams.
 *
 * <p>Both level streams use the RLE-Bit-Packed hybrid encoding (Parquet spec). A run is either:
 *
 * <ul>
 *   <li><b>Bit-packed</b>: varint header with low bit set; followed by N values bit-packed at the column's level
 *       bit-width
 *   <li><b>RLE</b>: varint header with low bit clear; followed by a single value (in little-endian
 *       {@code ceil(bitWidth/8)} bytes) that repeats run-length times
 * </ul>
 *
 * <p>Used identically for both rep and def levels; the bit width per stream is computed from the column's max level
 * (see {@link #computeBitWidth(int)}).
 *
 * <p>The decoder reads its input one byte at a time from a {@link MemorySegment}, assembling multi-byte values itself.
 */
public final class LevelDecoder {

    private final int bitWidth;
    private final int bytesPerRleValue;

    private MemorySegment segment;
    private long position;

    private int remainingInRun;
    private boolean currentRunIsRle;
    private int rleValue;
    // Accumulates raw bytes LSB-first; up to 64 bits buffered across reads.
    private long bitPackedBuffer;
    private int bitsInBuffer;

    public LevelDecoder(int bitWidth) {
        if (bitWidth < 0 || bitWidth > 32) {
            throw new IllegalArgumentException("bitWidth must be in [0, 32]; got " + bitWidth);
        }
        this.bitWidth = bitWidth;
        this.bytesPerRleValue = (bitWidth + 7) / 8;
    }

    /** Compute the minimum bit width to encode level values in [0, maxLevel]. */
    public static int computeBitWidth(int maxLevel) {
        if (maxLevel < 0) {
            throw new IllegalArgumentException("maxLevel must be non-negative; got " + maxLevel);
        }
        if (maxLevel == 0) {
            return 0;
        }
        return 32 - Integer.numberOfLeadingZeros(maxLevel);
    }

    /**
     * Load the level stream's bytes. For DataPage V1 the stream is prefixed by a 4-byte LE length; for DataPage V2 the
     * page header carries the length explicitly and the bytes here start at the first run header. The caller supplies
     * the right slice.
     */
    public void load(MemorySegment bytes) {
        this.segment = bytes;
        this.position = 0L;
        this.remainingInRun = 0;
        this.bitPackedBuffer = 0L;
        this.bitsInBuffer = 0;
    }

    /** Skip {@code n} values without materializing them. */
    public void skip(int n) {
        if (bitWidth == 0) {
            return;
        }
        for (int i = 0; i < n; i++) {
            nextValue();
        }
    }

    /**
     * Decodes the next {@code n} level values into {@code dst} starting at {@code offset}. The batch driver uses this
     * to fill a column vector's rep/def stream in one call.
     */
    public void decode(int n, int[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = nextValue();
        }
    }

    /**
     * Sets bit {@code base + i} in {@code out} for each of the next {@code count} levels that equals
     * {@code targetLevel}, leaving the other bits untouched. RLE runs of the target value set a whole range in one
     * call; only bit-packed runs are walked value by value. This is the def-level to validity-bitset path: it avoids
     * materializing an {@code int[]} of levels and turns an all-defined page (a single RLE run) into one range set.
     */
    public void decodeEquals(int count, int targetLevel, BitSet out, int base) {
        if (bitWidth == 0) {
            if (targetLevel == 0) {
                out.set(base, base + count);
            }
            return;
        }
        int produced = 0;
        while (produced < count) {
            if (remainingInRun == 0) {
                readNextRunHeader();
            }
            int take = Math.min(remainingInRun, count - produced);
            setEqualBits(out, base + produced, take, targetLevel);
            remainingInRun -= take;
            produced += take;
        }
    }

    /**
     * Sets the bits in {@code [from, from + take)} whose level equals {@code targetLevel}, consuming {@code take}
     * values of the current run. An RLE run sets the whole range in one call; a bit-packed run is walked value by
     * value.
     */
    private void setEqualBits(BitSet out, int from, int take, int targetLevel) {
        if (currentRunIsRle) {
            if (rleValue == targetLevel) {
                out.set(from, from + take);
            }
            return;
        }
        for (int i = 0; i < take; i++) {
            if (readBitPackedValue() == targetLevel) {
                out.set(from + i);
            }
        }
    }

    /**
     * Pulls the next level value from the underlying RLE/bit-packed stream. Internal driver for {@link #decode},
     * {@link #skip}, and the package-local page decoders that yield one value at a time; not part of the public
     * surface.
     */
    int nextValue() {
        if (bitWidth == 0) {
            return 0;
        }
        if (remainingInRun == 0) {
            readNextRunHeader();
        }
        remainingInRun--;
        if (currentRunIsRle) {
            return rleValue;
        }
        return readBitPackedValue();
    }

    private void readNextRunHeader() {
        long header = readVarint();
        if ((header & 1L) == 1L) {
            // Bit-packed run: high bits are the number of groups of 8 values.
            int groups = (int) (header >>> 1);
            remainingInRun = groups * 8;
            currentRunIsRle = false;
            bitPackedBuffer = 0L;
            bitsInBuffer = 0;
        } else {
            // RLE run: high bits are the run length; followed by the repeated value.
            remainingInRun = (int) (header >>> 1);
            currentRunIsRle = true;
            rleValue = readFixedWidthLE(bytesPerRleValue);
        }
    }

    /**
     * Pull the next bit-packed value from the internal bit buffer, refilling from the segment one byte at a time as
     * needed. Values are packed LSB-first per the Parquet spec.
     */
    private int readBitPackedValue() {
        while (bitsInBuffer < bitWidth) {
            int b = readByte();
            bitPackedBuffer |= ((long) b) << bitsInBuffer;
            bitsInBuffer += 8;
        }
        long mask = (1L << bitWidth) - 1L;
        int value = (int) (bitPackedBuffer & mask);
        bitPackedBuffer >>>= bitWidth;
        bitsInBuffer -= bitWidth;
        return value;
    }

    private int readFixedWidthLE(int n) {
        int value = 0;
        for (int i = 0; i < n; i++) {
            int b = readByte();
            value |= b << (i * 8);
        }
        return value;
    }

    private long readVarint() {
        long result = 0L;
        int shift = 0;
        while (true) {
            int b = readByte();
            result |= ((long) (b & 0x7f)) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            if (shift > 70) {
                throw new IllegalStateException("Varint too long in level stream");
            }
        }
    }

    /** Read the next unsigned byte (0-255) from the loaded segment. */
    private int readByte() {
        return segment.get(JAVA_BYTE, position++) & 0xff;
    }
}
