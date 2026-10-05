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

import io.tileverse.parquetry.format.MalformedFileException;

/**
 * Shared DELTA_BINARY_PACKED decode engine. Works on {@code long} internally; INT32 and INT64 decoders delegate here.
 *
 * <p>Per parquet-format Encodings.md, the encoding is block-based: a header gives block size, miniblock count, total
 * value count, and the first value (zigzag varint). Each block is preceded by a zigzag min-delta and N bit-widths (one
 * byte per miniblock); then the deltas are bit-packed in miniblocks at the per-miniblock width.
 *
 * <p>Value reconstruction: {@code value[i] = value[i-1] + (packed[i] + min_delta)}.
 *
 * <p>The decoder reads its input one byte at a time from a {@link MemorySegment}; {@link #position()} reports how many
 * bytes have been consumed, which lets callers that pack several DELTA_BINARY_PACKED segments back-to-back (DELTA_BYTE
 * ARRAY, DELTA_LENGTH_BYTE_ARRAY) find where the next segment begins.
 */
final class DeltaBinaryPackedDecoder {

    private static final int MAX_VARINT_SHIFT = 63;

    private MemorySegment segment;
    private long position;
    private long limit;

    // Header fields
    private int blockSize;
    private int miniblocksPerBlock;
    private int valuesPerMiniblock;
    private int totalValueCount;
    private long lastValue;

    // Current block state
    private long currentMinDelta;
    private int[] currentMiniblockWidths;
    private int miniblockIndex; // index within current block
    private int positionInMiniblock; // values consumed in current miniblock

    // Bit-packed value buffer (LSB-first, per Parquet spec)
    private long bitBuffer;
    private int bitsInBuffer;

    private int valuesEmitted;

    void load(MemorySegment page) {
        this.segment = page;
        this.position = 0L;
        this.limit = page.byteSize();
        this.blockSize = readHeaderCount("block size", 1L);
        this.miniblocksPerBlock = readHeaderCount("miniblock count", 1L);
        requireMiniblocksDivideBlock();
        this.valuesPerMiniblock = blockSize / miniblocksPerBlock;
        this.totalValueCount = readHeaderCount("total value count", 0L);
        requireWidthBytesForEachMiniblock();
        long firstValue = readZigzagVarint();
        this.lastValue = firstValue;
        this.valuesEmitted = 0;
        // Sentinel values trigger "fetch new block" on first call to next() after the first value
        this.miniblockIndex = miniblocksPerBlock;
        this.positionInMiniblock = valuesPerMiniblock;
        // A stream of at most one value holds no block, hence no miniblock widths to buffer.
        this.currentMiniblockWidths = new int[totalValueCount > 1 ? miniblocksPerBlock : 0];
        this.bitBuffer = 0L;
        this.bitsInBuffer = 0;
    }

    /** Reads a header varint and requires it to lie in {@code [minimum, Integer.MAX_VALUE]}. */
    private int readHeaderCount(String field, long minimum) {
        long value = readVarint();
        if (value < minimum || value > Integer.MAX_VALUE) {
            throw new MalformedFileException("DELTA_BINARY_PACKED header declares a " + field + " of "
                    + Long.toUnsignedString(value) + ", outside [" + minimum + ", " + Integer.MAX_VALUE + "]");
        }
        return (int) value;
    }

    private void requireMiniblocksDivideBlock() {
        if (miniblocksPerBlock > blockSize || blockSize % miniblocksPerBlock != 0) {
            throw new MalformedFileException("DELTA_BINARY_PACKED header splits blocks of " + blockSize
                    + " values into " + miniblocksPerBlock + " miniblocks, an uneven split");
        }
    }

    /**
     * Each block after the first value opens with one bit-width byte per miniblock. A stream holding more than one
     * value but fewer bytes than that declares more miniblocks than it can hold, and its count must not size the width
     * buffer.
     */
    private void requireWidthBytesForEachMiniblock() {
        if (totalValueCount > 1 && miniblocksPerBlock > limit - position) {
            throw new MalformedFileException("DELTA_BINARY_PACKED header declares " + miniblocksPerBlock
                    + " miniblocks per block but only " + (limit - position) + " bytes follow it");
        }
    }

    /** Number of bytes consumed from the loaded segment so far. */
    long position() {
        return position;
    }

    long next() {
        if (valuesEmitted == 0) {
            valuesEmitted++;
            return lastValue;
        }
        if (positionInMiniblock == valuesPerMiniblock) {
            miniblockIndex++;
            positionInMiniblock = 0;
            bitBuffer = 0L;
            bitsInBuffer = 0;
            if (miniblockIndex >= miniblocksPerBlock) {
                loadNextBlock();
                miniblockIndex = 0;
            }
        }
        int width = currentMiniblockWidths[miniblockIndex];
        long packed = (width == 0) ? 0L : readBitPacked(width);
        long delta = packed + currentMinDelta;
        long value = lastValue + delta;
        lastValue = value;
        positionInMiniblock++;
        valuesEmitted++;
        return value;
    }

    void skip(int n) {
        for (int i = 0; i < n; i++) {
            next();
        }
    }

    int totalValueCount() {
        return totalValueCount;
    }

    /**
     * Fails when the loaded stream declares fewer than {@code neededValues} values. Decoding past the declared count
     * would read the padding of the last miniblock, or the bytes after the stream, as values.
     */
    void requireDeclaredValues(int neededValues) {
        if (totalValueCount < neededValues) {
            throw new MalformedFileException("DELTA_BINARY_PACKED stream declares " + totalValueCount
                    + " values but its page needs " + neededValues);
        }
    }

    /**
     * The number of values whose deltas have actual bit-packed data in the stream, which is {@code >=
     * totalValueCount()} because the last used miniblock is always written full-size (padded). Callers that pack
     * several DELTA_BINARY_PACKED segments back-to-back (as in DELTA_BYTE_ARRAY) drain this many values to position the
     * cursor at the start of the next segment.
     *
     * <p>Only the miniblocks that contain values hold data. Trailing miniblocks of the last block have a bit-width byte
     * but no data bytes; rounding the last block up to a full block (instead of to the last used miniblock) would drain
     * past the end of the stream.
     */
    int paddedValueCount() {
        int valuesAfterHeader = totalValueCount - 1;
        if (valuesAfterHeader <= 0) {
            return totalValueCount;
        }
        int wholeBlocks = valuesAfterHeader / blockSize;
        int remainder = valuesAfterHeader % blockSize;
        int encodedValues = wholeBlocks * blockSize;
        if (remainder > 0) {
            int usedMiniblocks = (remainder + valuesPerMiniblock - 1) / valuesPerMiniblock;
            encodedValues += usedMiniblocks * valuesPerMiniblock;
        }
        return 1 + encodedValues;
    }

    private void loadNextBlock() {
        currentMinDelta = readZigzagVarint();
        for (int i = 0; i < miniblocksPerBlock; i++) {
            currentMiniblockWidths[i] = readMiniblockWidth();
        }
    }

    /** A miniblock's bit width, at most the 64 bits of a decoded delta. */
    private int readMiniblockWidth() {
        int width = readByte();
        if (width > Long.SIZE) {
            throw miniblockWidthTooWide(width);
        }
        return width;
    }

    /**
     * Reads {@code width} bits (LSB-first) from the miniblock stream. The buffer holds at most one byte (8 bits) at a
     * time and the gathered value accumulates separately; a width near 64 never shifts bits past the end of the 64-bit
     * buffer. A single accumulator would silently drop the high bits of wide miniblocks.
     */
    private long readBitPacked(int width) {
        long value = 0L;
        int gathered = 0;
        while (gathered < width) {
            if (bitsInBuffer == 0) {
                bitBuffer = readByte();
                bitsInBuffer = 8;
            }
            int take = Math.min(width - gathered, bitsInBuffer);
            long lowBits = bitBuffer & ((1L << take) - 1L);
            value |= lowBits << gathered;
            bitBuffer >>>= take;
            bitsInBuffer -= take;
            gathered += take;
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

    private long readZigzagVarint() {
        long raw = readVarint();
        return (raw >>> 1) ^ -(raw & 1);
    }

    /**
     * Reads the next byte. Running out of bytes means the stream holds fewer values than it declares. The segment's own
     * bounds check detects it: translating that failure here costs nothing per byte, where a check of ours would repeat
     * the segment's.
     */
    private int readByte() {
        try {
            return segment.get(JAVA_BYTE, position++) & 0xff;
        } catch (IndexOutOfBoundsException e) {
            throw streamEnded(limit);
        }
    }

    // The failures below are built out of line: the methods raising them run once per value or byte, and the JIT only
    // inlines them into the decode loops while they stay small.

    private static MalformedFileException streamEnded(long byteSize) {
        return new MalformedFileException(
                "DELTA_BINARY_PACKED stream ends after " + byteSize + " bytes, short of the values it declares");
    }

    private static MalformedFileException varintTooLong() {
        return new MalformedFileException("DELTA_BINARY_PACKED stream holds a varint longer than 10 bytes");
    }

    private static MalformedFileException miniblockWidthTooWide(int width) {
        return new MalformedFileException("DELTA_BINARY_PACKED miniblock bit width " + width + " exceeds " + Long.SIZE);
    }
}
