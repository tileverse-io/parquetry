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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

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

    /**
     * Skips {@code n} values without materializing them. The cost is per run, never per value: an RLE run is jumped in
     * constant time, and a bit-packed run costs one buffer adjustment plus a cursor step.
     */
    public void skip(int n) {
        if (bitWidth == 0) {
            return;
        }
        int remaining = n;
        while (remaining > 0) {
            if (remainingInRun == 0) {
                readNextRunHeader();
            }
            int take = Math.min(remainingInRun, remaining);
            if (!currentRunIsRle) {
                dropBitPackedBits((long) take * bitWidth);
            }
            remainingInRun -= take;
            remaining -= take;
        }
    }

    /**
     * Drops {@code bits} bits of the current bit-packed run: the buffered bits first, then whole bytes of the stream,
     * then the leading bits of one refill byte. Leaves the buffer holding only the bits after the drop, which is the
     * state {@link #readBitPackedValue()} resumes from.
     */
    private void dropBitPackedBits(long bits) {
        if (bits < bitsInBuffer) {
            bitPackedBuffer >>>= bits;
            bitsInBuffer -= (int) bits;
            return;
        }
        long beyondBuffer = bits - bitsInBuffer;
        bitPackedBuffer = 0L;
        bitsInBuffer = 0;
        position += beyondBuffer >>> 3;
        int leftover = (int) (beyondBuffer & 7L);
        if (leftover > 0) {
            int refill = readByte();
            bitPackedBuffer = ((long) refill) >>> leftover;
            bitsInBuffer = 8 - leftover;
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
     * Decodes the next {@code n} level values into {@code dst} as 32-bit native-order ints, written unaligned (pooled
     * segments make no alignment promise). {@code dst} must cover at least {@code 4L * n} bytes.
     */
    public void decodeInto(int n, MemorySegment dst) {
        for (int i = 0; i < n; i++) {
            dst.setAtIndex(ValueLayout.JAVA_INT_UNALIGNED, i, nextValue());
        }
    }

    /**
     * Decodes {@code count} definition levels straight into an off-heap LSB-first validity bitmap: bit {@code i} is set
     * when level {@code i} equals {@code targetLevel} (the value is present at the leaf), cleared otherwise.
     * {@code out} must cover at least {@code ceil(count / 8)} bytes; this method writes every covered byte, never
     * relying on the caller to pre-zero. Returns the count of set bits (the present, non-null values). An all-present
     * page is a single RLE run and fills the bitmap in whole bytes.
     */
    public int decodeValidityBitmap(int count, int targetLevel, MemorySegment out) {
        ValidityBitmapSink sink = new ValidityBitmapSink(out);
        if (bitWidth == 0) {
            sink.acceptRun(targetLevel == 0, count);
            sink.flush();
            return sink.validCount();
        }
        walkRuns(count, targetLevel, sink);
        sink.flush();
        return sink.validCount();
    }

    /**
     * RLE / bit-packed run walker for the {@code bitWidth > 0} case. Consumes {@code count} levels, handing each run to
     * {@code sink}: an RLE run is one {@link ValidityBitmapSink#acceptRun} call (the whole run matches the target or
     * not), a bit-packed run is walked value by value through {@link ValidityBitmapSink#acceptValue}.
     */
    private void walkRuns(int count, int targetLevel, ValidityBitmapSink sink) {
        int produced = 0;
        while (produced < count) {
            if (remainingInRun == 0) {
                readNextRunHeader();
            }
            int take = Math.min(remainingInRun, count - produced);
            if (currentRunIsRle) {
                sink.acceptRun(rleValue == targetLevel, take);
            } else {
                for (int i = 0; i < take; i++) {
                    sink.acceptValue(readBitPackedValue() == targetLevel);
                }
            }
            remainingInRun -= take;
            produced += take;
        }
    }

    /**
     * Packs matched / unmatched levels LSB-first into an off-heap segment: bit-packed levels one bit at a time,
     * accumulating until a byte is full; RLE runs in whole {@code 0xFF}/{@code 0x00} bytes once the run reaches a byte
     * boundary. {@link #flush()} writes the final partial byte after the last run.
     */
    private static final class ValidityBitmapSink {

        private final MemorySegment out;
        private long byteIndex;
        private int currentByte;
        private int bitsInByte;
        private int validCount;

        private ValidityBitmapSink(MemorySegment out) {
            this.out = out;
        }

        /** A run of {@code count} levels that all do ({@code matches}) or all do not equal the target level. */
        void acceptRun(boolean matches, int count) {
            int remaining = count;
            while (remaining > 0 && bitsInByte != 0) {
                acceptValue(matches);
                remaining--;
            }
            int wholeBytes = remaining >>> 3;
            if (wholeBytes > 0) {
                out.asSlice(byteIndex, wholeBytes).fill(matches ? (byte) 0xff : (byte) 0x00);
                byteIndex += wholeBytes;
                if (matches) {
                    validCount += wholeBytes * 8;
                }
                remaining -= wholeBytes * 8;
            }
            for (int i = 0; i < remaining; i++) {
                acceptValue(matches);
            }
        }

        /** One bit-packed level that does ({@code match}) or does not equal the target level. */
        void acceptValue(boolean match) {
            if (match) {
                currentByte |= 1 << bitsInByte;
                validCount++;
            }
            bitsInByte++;
            if (bitsInByte == 8) {
                out.set(JAVA_BYTE, byteIndex++, (byte) currentByte);
                currentByte = 0;
                bitsInByte = 0;
            }
        }

        private void flush() {
            if (bitsInByte > 0) {
                out.set(JAVA_BYTE, byteIndex++, (byte) currentByte);
                currentByte = 0;
                bitsInByte = 0;
            }
        }

        private int validCount() {
            return validCount;
        }
    }

    /**
     * Pulls the next level value from the underlying RLE/bit-packed stream. Internal driver for {@link #decode},
     * {@link #skip}, and the package-local page decoders that yield one value at a time; not public API.
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
