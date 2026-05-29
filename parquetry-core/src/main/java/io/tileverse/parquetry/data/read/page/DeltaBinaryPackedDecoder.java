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
     * Returns the number of values actually encoded in complete blocks, including any padding values appended to fill
     * the last block. This is always {@code >= totalValueCount()}.
     *
     * <p>DELTA_BINARY_PACKED encoders always write at least one full block after the header value, even when
     * {@code totalValueCount == 1}. Callers that pack several DELTA_BINARY_PACKED segments back-to-back (as in
     * DELTA_BYTE_ARRAY) must drain this many values from each segment to leave the cursor positioned at the start of
     * the next segment.
     */
    int paddedValueCount() {
        if (totalValueCount == 0) {
            return 0;
        }
        // Encoders always write at least one block; even a single-value encoding has block bytes
        // appended when the source array was padded to blockSize.
        int valuesAfterHeader = Math.max(blockSize, totalValueCount - 1);
        int fullBlocks = (valuesAfterHeader + blockSize - 1) / blockSize;
        return 1 + fullBlocks * blockSize;
    }

    private void loadNextBlock() {
        currentMinDelta = readZigzagVarint();
        for (int i = 0; i < miniblocksPerBlock; i++) {
            currentMiniblockWidths[i] = readByte();
        }
    }

    private long readBitPacked(int width) {
        while (bitsInBuffer < width) {
            int b = readByte();
            bitBuffer |= ((long) b) << bitsInBuffer;
            bitsInBuffer += 8;
        }
        long mask = (width == 64) ? -1L : ((1L << width) - 1L);
        long value = bitBuffer & mask;
        bitBuffer >>>= width;
        bitsInBuffer -= width;
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
