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

import io.tileverse.parquetry.format.MalformedFileException;

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
 * (see {@link #forMaxLevel(int, String)} and {@link #computeBitWidth(int)}).
 *
 * <p>The decoder reads its input one byte at a time from a {@link MemorySegment}, assembling multi-byte values itself.
 * A stream that ends before the requested values, a run longer than a page can hold, or a value above the stream's
 * maximum fails with a {@link MalformedFileException} naming the stream.
 */
public final class LevelDecoder {

    private static final int MAX_VARINT_SHIFT = 63;

    private final int bitWidth;
    private final int bytesPerRleValue;
    // Largest legal value, compared unsigned and never above the largest value of the bit width: the max level of a
    // level stream, the last entry of a dictionary for its indices.
    private final int maxValue;
    // Only a maximum below the largest value of the bit width leaves room for bit-packed values above it.
    private final boolean checksBitPackedValues;
    private final String streamName;

    private MemorySegment segment;
    private long position;

    private int remainingInRun;
    private boolean currentRunIsRle;
    private int rleValue;
    // Accumulates raw bytes LSB-first; up to 64 bits buffered across reads.
    private long bitPackedBuffer;
    private int bitsInBuffer;

    public LevelDecoder(int bitWidth) {
        this(bitWidth, "levels");
    }

    /**
     * A decoder accepting any value of the bit width.
     *
     * @param bitWidth the width of each value, in {@code [0, 32]}
     * @param streamName what the stream holds, as a plural noun such as {@code "dictionary indices"}; error messages
     *     name it
     */
    public LevelDecoder(int bitWidth, String streamName) {
        this(bitWidth, largestValueOfWidth(bitWidth), streamName);
    }

    private LevelDecoder(int bitWidth, int maxValue, String streamName) {
        if (bitWidth < 0 || bitWidth > 32) {
            throw new IllegalArgumentException("bitWidth must be in [0, 32]; got " + bitWidth);
        }
        int largestOfWidth = largestValueOfWidth(bitWidth);
        this.bitWidth = bitWidth;
        this.bytesPerRleValue = (bitWidth + 7) / 8;
        this.maxValue = Integer.compareUnsigned(maxValue, largestOfWidth) < 0 ? maxValue : largestOfWidth;
        this.checksBitPackedValues = this.maxValue != largestOfWidth;
        this.streamName = streamName;
    }

    /**
     * A decoder for the level stream of a column whose levels reach {@code maxLevel}: the bit width follows from the
     * max level, and a level above it fails with a {@link MalformedFileException}.
     *
     * @param streamName what the stream holds, as a plural noun such as {@code "definition levels"}; error messages
     *     name it
     */
    public static LevelDecoder forMaxLevel(int maxLevel, String streamName) {
        return new LevelDecoder(computeBitWidth(maxLevel), maxLevel, streamName);
    }

    /**
     * A decoder for values of {@code bitWidth} bits bounded by {@code maxValue}, such as dictionary indices bounded by
     * the last dictionary entry. A value above {@code maxValue}, compared unsigned, fails with a
     * {@link MalformedFileException}.
     *
     * @param streamName what the stream holds, as a plural noun such as {@code "dictionary indices"}; error messages
     *     name it
     */
    public static LevelDecoder forMaxValue(int bitWidth, int maxValue, String streamName) {
        return new LevelDecoder(bitWidth, maxValue, streamName);
    }

    /** The largest value of {@code bitWidth} bits, read as unsigned: all 32 bits set for a width of 32. */
    private static int largestValueOfWidth(int bitWidth) {
        return (int) ((1L << bitWidth) - 1L);
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
        while (remainingInRun == 0) {
            readNextRunHeader();
        }
        remainingInRun--;
        if (currentRunIsRle) {
            return rleValue;
        }
        return readBitPackedValue();
    }

    /**
     * Reads the next run header. A run of zero values yields nothing: callers keep reading headers until a run holds
     * values, and the end of the stream stops a stream made only of empty runs.
     */
    private void readNextRunHeader() {
        long header = readVarint();
        long runCount = header >>> 1;
        if ((header & 1L) == 1L) {
            // Bit-packed run: high bits are the number of groups of 8 values.
            remainingInRun = bitPackedRunLength(runCount);
            currentRunIsRle = false;
            bitPackedBuffer = 0L;
            bitsInBuffer = 0;
        } else {
            // RLE run: high bits are the run length; followed by the repeated value.
            remainingInRun = rleRunLength(runCount);
            currentRunIsRle = true;
            rleValue = readRleValue();
        }
    }

    private int bitPackedRunLength(long groups) {
        if (groups > Integer.MAX_VALUE / 8) {
            throw bitPackedRunTooLong(groups);
        }
        return (int) groups * 8;
    }

    private int rleRunLength(long length) {
        if (length > Integer.MAX_VALUE) {
            throw rleRunTooLong(length);
        }
        return (int) length;
    }

    /** The repeated value of an RLE run, checked once for the whole run against the stream's maximum. */
    private int readRleValue() {
        int value = readFixedWidthLE(bytesPerRleValue);
        requireAtMostMaximum(value);
        return value;
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
        if (checksBitPackedValues) {
            requireAtMostMaximum(value);
        }
        return value;
    }

    private void requireAtMostMaximum(int value) {
        if (Integer.compareUnsigned(value, maxValue) > 0) {
            throw valueAboveMaximum(value);
        }
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
            if (shift > MAX_VARINT_SHIFT) {
                throw varintTooLong();
            }
        }
    }

    /**
     * Reads the next unsigned byte (0-255) from the loaded segment. Running out of bytes means the stream holds fewer
     * values than its page declares. The segment's own bounds check detects it: translating that failure here costs
     * nothing per byte, where a check of ours would repeat the segment's.
     */
    private int readByte() {
        try {
            return segment.get(JAVA_BYTE, position++) & 0xff;
        } catch (IndexOutOfBoundsException e) {
            throw streamEnded();
        }
    }

    // The failures below are built out of line: the methods raising them run once per value or byte, and the JIT only
    // inlines them into the decode loops while they stay small.

    private MalformedFileException streamEnded() {
        return new MalformedFileException(
                streamName + " end at byte " + segment.byteSize() + ", short of the values declared by their page");
    }

    private MalformedFileException varintTooLong() {
        return new MalformedFileException(streamName + " hold a run header varint longer than 10 bytes");
    }

    private MalformedFileException bitPackedRunTooLong(long groups) {
        return new MalformedFileException(streamName + " declare a bit-packed run of " + groups
                + " groups of 8 values, more than a page can hold");
    }

    private MalformedFileException rleRunTooLong(long length) {
        return new MalformedFileException(
                streamName + " declare an RLE run of " + length + " values, more than a page can hold");
    }

    private MalformedFileException valueAboveMaximum(int value) {
        return new MalformedFileException(streamName + " hold the value " + Integer.toUnsignedString(value)
                + ", above their maximum " + Integer.toUnsignedString(maxValue));
    }
}
