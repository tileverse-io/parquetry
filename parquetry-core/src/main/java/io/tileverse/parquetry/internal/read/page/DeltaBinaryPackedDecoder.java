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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;

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

    private MemorySegment segment;
    private long position;

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
        this.blockSize = (int) readVarint();
        this.miniblocksPerBlock = (int) readVarint();
        this.valuesPerMiniblock = blockSize / miniblocksPerBlock;
        this.totalValueCount = (int) readVarint();
        long firstValue = readZigzagVarint();
        this.lastValue = firstValue;
        this.valuesEmitted = 0;
        // Sentinel values trigger "fetch new block" on first call to next() after the first value
        this.miniblockIndex = miniblocksPerBlock;
        this.positionInMiniblock = valuesPerMiniblock;
        this.currentMiniblockWidths = new int[miniblocksPerBlock];
        this.bitBuffer = 0L;
        this.bitsInBuffer = 0;
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
            currentMiniblockWidths[i] = readByte();
        }
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
            if (shift > 70) {
                throw new IllegalStateException("Varint too long in DELTA_BINARY_PACKED");
            }
        }
    }

    private long readZigzagVarint() {
        long raw = readVarint();
        return (raw >>> 1) ^ -(raw & 1);
    }

    private int readByte() {
        return segment.get(JAVA_BYTE, position++) & 0xff;
    }
}
