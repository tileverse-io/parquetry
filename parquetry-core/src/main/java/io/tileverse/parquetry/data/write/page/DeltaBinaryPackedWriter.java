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
package io.tileverse.parquetry.data.write.page;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Shared DELTA_BINARY_PACKED encode engine. Works on {@code long} internally; INT32 and INT64 encoders both call into
 * here after widening their input.
 *
 * <p>Inverse of {@link io.tileverse.parquetry.data.read.page.DeltaBinaryPackedDecoder}. Wire format per parquet-format
 * Encodings.md:
 *
 * <pre>
 *   header  = blockSize (uvarint) miniblocksPerBlock (uvarint) totalValueCount (uvarint) firstValue (zigzag varint)
 *   block_i = minDelta (zigzag varint) bitWidths[1..M] deltas[1..valuesPerBlock] (bit-packed at the per-miniblock width)
 * </pre>
 *
 * <p>The Parquet 2 standard shape is used: 128-value blocks, 4 miniblocks of 32 values each. The {@code minDelta} and
 * per-miniblock bit-widths are computed only from the real deltas in each block; if the final block is short, the
 * trailing slots are treated as zero in both the width calculation and the bit-packed payload. The decoder reads the
 * complete block (including the padding zeros), so the same width across the miniblock applies.
 */
final class DeltaBinaryPackedWriter {

    static final int BLOCK_SIZE = 128;
    static final int MINIBLOCKS_PER_BLOCK = 4;
    static final int VALUES_PER_MINIBLOCK = BLOCK_SIZE / MINIBLOCKS_PER_BLOCK;

    private DeltaBinaryPackedWriter() {}

    /**
     * Encode {@code n} values; emit only the bytes the decoder needs to consume to read {@code n} real values.
     *
     * <p>Used by {@code Int32}, {@code Int64}, and {@code DELTA_LENGTH_BYTE_ARRAY} encoders. The last block's partial
     * miniblock is byte-trimmed - no padding bytes are emitted for slots beyond {@code n}.
     */
    static int write(long[] values, int n, WritableByteChannel dst) throws IOException {
        return write(values, n, n, false, dst);
    }

    /**
     * Encode {@code n} values and emit the full padded last block, so a consumer that calls
     * {@code skip(paddedValueCount - n)} after reading the real values lands at the end of the encoded stream.
     *
     * <p>Used by {@code DELTA_BYTE_ARRAY}, which drains padding via {@code skip()} between the prefix-length and
     * suffix-length streams to locate the suffix-bytes section.
     */
    static int writeFullyPadded(long[] values, int n, WritableByteChannel dst) throws IOException {
        return write(values, n, n, true, dst);
    }

    private static int write(long[] values, int n, int totalValueCount, boolean padFullBlock, WritableByteChannel dst)
            throws IOException {
        ByteArrayOutputStream scratch = new ByteArrayOutputStream();
        writeHeader(scratch, totalValueCount);
        if (n == 0) {
            // Spec leaves the first-value field present even when totalValueCount is zero.
            writeZigzagVarint(scratch, 0L);
            byte[] empty = scratch.toByteArray();
            ChannelWrites.writeFully(dst, ByteBuffer.wrap(empty));
            return empty.length;
        }
        writeZigzagVarint(scratch, values[0]);

        long previous = values[0];
        int blockStart = 1;
        // In fully-padded mode, emit at least one block even if n==1; the decoder reports
        // paddedValueCount == 1 + BLOCK_SIZE and DeltaByteArrayDecoder will skip(BLOCK_SIZE) to drain it.
        boolean needAtLeastOneBlock = padFullBlock && n == 1;
        while (blockStart < n || needAtLeastOneBlock) {
            needAtLeastOneBlock = false;
            int blockEnd = Math.min(blockStart + BLOCK_SIZE, n);
            previous = writeBlock(scratch, values, blockStart, blockEnd, previous, padFullBlock);
            blockStart = blockEnd;
        }
        byte[] payload = scratch.toByteArray();
        ChannelWrites.writeFully(dst, ByteBuffer.wrap(payload));
        return payload.length;
    }

    private static void writeHeader(ByteArrayOutputStream scratch, int totalValueCount) {
        writeUVarint(scratch, BLOCK_SIZE);
        writeUVarint(scratch, MINIBLOCKS_PER_BLOCK);
        writeUVarint(scratch, totalValueCount);
    }

    private static long writeBlock(
            ByteArrayOutputStream scratch,
            long[] values,
            int blockStart,
            int blockEnd,
            long previous,
            boolean padFullBlock) {
        int valuesInBlock = blockEnd - blockStart;
        long[] deltas = new long[valuesInBlock];
        long minDelta = computeDeltas(values, blockStart, valuesInBlock, previous, deltas);

        long[] adjusted = new long[valuesInBlock];
        int[] widths = computeMiniblockWidths(deltas, minDelta, valuesInBlock, adjusted);

        writeZigzagVarint(scratch, minDelta);
        for (int mb = 0; mb < MINIBLOCKS_PER_BLOCK; mb++) {
            scratch.write(widths[mb]);
        }
        for (int mb = 0; mb < MINIBLOCKS_PER_BLOCK; mb++) {
            int width = widths[mb];
            if (width == 0) {
                continue;
            }
            int start = mb * VALUES_PER_MINIBLOCK;
            int valuesToWrite =
                    padFullBlock ? VALUES_PER_MINIBLOCK : Math.min(VALUES_PER_MINIBLOCK, valuesInBlock - start);
            writeMiniblock(scratch, adjusted, start, valuesInBlock, valuesToWrite, width);
        }
        return (valuesInBlock > 0) ? values[blockEnd - 1] : previous;
    }

    private static long computeDeltas(long[] values, int blockStart, int valuesInBlock, long previous, long[] deltas) {
        if (valuesInBlock == 0) {
            return 0L;
        }
        long minDelta = Long.MAX_VALUE;
        long prev = previous;
        for (int i = 0; i < valuesInBlock; i++) {
            deltas[i] = values[blockStart + i] - prev;
            if (deltas[i] < minDelta) {
                minDelta = deltas[i];
            }
            prev = values[blockStart + i];
        }
        return minDelta;
    }

    private static int[] computeMiniblockWidths(long[] deltas, long minDelta, int valuesInBlock, long[] adjusted) {
        int[] widths = new int[MINIBLOCKS_PER_BLOCK];
        for (int mb = 0; mb < MINIBLOCKS_PER_BLOCK; mb++) {
            long maxAdjusted = 0L;
            for (int i = 0; i < VALUES_PER_MINIBLOCK; i++) {
                int idx = mb * VALUES_PER_MINIBLOCK + i;
                if (idx < valuesInBlock) {
                    adjusted[idx] = deltas[idx] - minDelta;
                    if (Long.compareUnsigned(adjusted[idx], maxAdjusted) > 0) {
                        maxAdjusted = adjusted[idx];
                    }
                }
            }
            widths[mb] = (maxAdjusted == 0L) ? 0 : 64 - Long.numberOfLeadingZeros(maxAdjusted);
        }
        return widths;
    }

    /**
     * Write {@code valuesToWrite} values bit-packed at {@code width}. The first {@code Math.min(realCount-start,
     * valuesToWrite)} come from {@code adjusted}; the remainder are zero-padded so the miniblock fills the requested
     * number of slots.
     *
     * <p>Only the bytes actually needed for {@code valuesToWrite} values are emitted, rounded up to a byte boundary.
     * Consumers can ask for partial miniblocks (the {@code DeltaLengthByteArray} path) or full ones (the
     * {@code DeltaByteArray} path, which drains padding values via {@code skip()} and therefore needs the bytes to
     * exist).
     */
    private static void writeMiniblock(
            ByteArrayOutputStream out, long[] adjusted, int start, int realCount, int valuesToWrite, int width) {
        long buffer = 0L;
        int bitsBuffered = 0;
        long mask = (width == 64) ? -1L : ((1L << width) - 1L);
        for (int i = 0; i < valuesToWrite; i++) {
            int idx = start + i;
            long value = (idx < realCount) ? (adjusted[idx] & mask) : 0L;
            buffer |= value << bitsBuffered;
            bitsBuffered += width;
            while (bitsBuffered >= 8) {
                out.write((int) (buffer & 0xff));
                buffer >>>= 8;
                bitsBuffered -= 8;
            }
        }
        if (bitsBuffered > 0) {
            out.write((int) (buffer & 0xff));
        }
    }

    private static void writeUVarint(ByteArrayOutputStream out, long value) {
        while ((value & ~0x7fL) != 0L) {
            out.write((int) ((value & 0x7f) | 0x80));
            value >>>= 7;
        }
        out.write((int) value);
    }

    private static void writeZigzagVarint(ByteArrayOutputStream out, long value) {
        writeUVarint(out, (value << 1) ^ (value >> 63));
    }
}
