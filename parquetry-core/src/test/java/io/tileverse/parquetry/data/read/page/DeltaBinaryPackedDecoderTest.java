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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

class DeltaBinaryPackedDecoderTest {

    @Test
    void roundTripSequentialInts() {
        int[] values = new int[32];
        for (int i = 0; i < 32; i++) values[i] = 100 + i * 7;
        byte[] encoded = encodeInts(values, 32, 4); // 32 values, 4 miniblocks

        PageDecoder<Integer> decoder = new DeltaBinaryPackedInt32Decoder();
        decoder.load(ByteBuffer.wrap(encoded), values.length);
        for (int i = 0; i < values.length; i++) {
            assertThat(decoder.next()).as("value " + i).isEqualTo(values[i]);
        }
    }

    @Test
    void roundTripMixedDeltas() {
        // Mix of positive, negative deltas, and zero deltas (forces min_delta != 0)
        int[] values = {1000, 999, 998, 1100, 1100, 1100, 1050, 1051, 1052};
        // Pad to block size; use blockSize=16, miniblocksPerBlock=4
        int[] padded = new int[16];
        System.arraycopy(values, 0, padded, 0, values.length);
        for (int i = values.length; i < padded.length; i++) padded[i] = values[values.length - 1];
        byte[] encoded = encodeInts(padded, 16, 4);

        PageDecoder<Integer> decoder = new DeltaBinaryPackedInt32Decoder();
        decoder.load(ByteBuffer.wrap(encoded), values.length);
        for (int i = 0; i < values.length; i++) {
            assertThat(decoder.next()).as("value " + i).isEqualTo(values[i]);
        }
    }

    @Test
    void int64RoundTrip() {
        long[] values = new long[128];
        for (int i = 0; i < values.length; i++) values[i] = 1_000_000_000L + i * 13L;
        byte[] encoded = encodeLongs(values, 128, 4);

        PageDecoder<Long> decoder = new DeltaBinaryPackedInt64Decoder();
        decoder.load(ByteBuffer.wrap(encoded), values.length);
        for (int i = 0; i < values.length; i++) {
            assertThat(decoder.next()).as("value " + i).isEqualTo(values[i]);
        }
    }

    @Test
    void bulkDecodeIntsFillsArray() {
        int[] values = {100, 107, 114, 121, 128};
        // Pad to block size 8 with the last value
        int[] padded = new int[8];
        System.arraycopy(values, 0, padded, 0, values.length);
        for (int i = values.length; i < padded.length; i++) padded[i] = values[values.length - 1];
        byte[] encoded = encodeInts(padded, 8, 2, values.length);

        PageDecoder<Integer> decoder = new DeltaBinaryPackedInt32Decoder();
        decoder.load(ByteBuffer.wrap(encoded), values.length);

        int[] dst = new int[values.length];
        decoder.decodeInts(values.length, dst, 0);

        assertThat(dst).containsExactly(values);
    }

    @Test
    void bulkDecodeLongsFillsArray() {
        long[] values = {1_000_000_000L, 1_000_000_013L, 1_000_000_026L, 1_000_000_039L};
        // Pad to block size 4 with the last value
        long[] padded = new long[4];
        System.arraycopy(values, 0, padded, 0, values.length);
        for (int i = values.length; i < padded.length; i++) padded[i] = values[values.length - 1];
        byte[] encoded = encodeLongs(padded, 4, 2, values.length);

        PageDecoder<Long> decoder = new DeltaBinaryPackedInt64Decoder();
        decoder.load(ByteBuffer.wrap(encoded), values.length);

        long[] dst = new long[values.length];
        decoder.decodeLongs(values.length, dst, 0);

        assertThat(dst).containsExactly(values);
    }

    /**
     * Encode int values into DELTA_BINARY_PACKED. The values array length must be a multiple of blockSize. Produces a
     * single complete page suitable for the decoder.
     */
    static byte[] encodeInts(int[] values, int blockSize, int miniblocksPerBlock) {
        return encodeInts(values, blockSize, miniblocksPerBlock, values.length);
    }

    /**
     * Encode int values into DELTA_BINARY_PACKED with an explicit total value count in the header. Useful when the
     * values array is padded to a block boundary but the actual count is smaller.
     */
    static byte[] encodeInts(int[] values, int blockSize, int miniblocksPerBlock, int totalValueCount) {
        long[] longs = new long[values.length];
        for (int i = 0; i < values.length; i++) longs[i] = values[i];
        return encodeLongs(longs, blockSize, miniblocksPerBlock, totalValueCount);
    }

    static byte[] encodeLongs(long[] values, int blockSize, int miniblocksPerBlock) {
        return encodeLongs(values, blockSize, miniblocksPerBlock, values.length);
    }

    /**
     * Encode long values into DELTA_BINARY_PACKED with an explicit total value count in the header. Useful when the
     * values array is padded to a block boundary but the actual count is smaller.
     */
    static byte[] encodeLongs(long[] values, int blockSize, int miniblocksPerBlock, int totalValueCount) {
        int valuesPerMiniblock = blockSize / miniblocksPerBlock;
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        writeUVarint(out, blockSize);
        writeUVarint(out, miniblocksPerBlock);
        writeUVarint(out, totalValueCount);
        writeZigzagVarint(out, values[0]);

        long previous = values[0];
        for (int blockStart = 1; blockStart < values.length; blockStart += blockSize) {
            int blockEnd = Math.min(blockStart + blockSize, values.length);
            // Compute deltas for this block
            long[] deltas = new long[blockEnd - blockStart];
            long minDelta = Long.MAX_VALUE;
            for (int i = 0; i < deltas.length; i++) {
                deltas[i] = values[blockStart + i] - previous;
                if (deltas[i] < minDelta) {
                    minDelta = deltas[i];
                }
                previous = values[blockStart + i];
            }
            // Adjusted deltas (non-negative)
            long[] adjusted = new long[deltas.length];
            int[] widths = new int[miniblocksPerBlock];
            for (int mb = 0; mb < miniblocksPerBlock; mb++) {
                long max = 0;
                for (int i = 0; i < valuesPerMiniblock; i++) {
                    int idx = mb * valuesPerMiniblock + i;
                    if (idx < deltas.length) {
                        adjusted[idx] = deltas[idx] - minDelta;
                        if (adjusted[idx] > max) {
                            max = adjusted[idx];
                        }
                    }
                }
                widths[mb] = (max == 0) ? 0 : 64 - Long.numberOfLeadingZeros(max);
            }

            // Write header: min delta + widths
            writeZigzagVarint(out, minDelta);
            for (int mb = 0; mb < miniblocksPerBlock; mb++) {
                out.write(widths[mb]);
            }
            // Write bit-packed adjusted deltas per miniblock
            for (int mb = 0; mb < miniblocksPerBlock; mb++) {
                int width = widths[mb];
                if (width == 0) {
                    continue;
                }
                long buf = 0L;
                int bits = 0;
                long mask = (width == 64) ? -1L : ((1L << width) - 1L);
                for (int i = 0; i < valuesPerMiniblock; i++) {
                    int idx = mb * valuesPerMiniblock + i;
                    long v = (idx < adjusted.length) ? (adjusted[idx] & mask) : 0L;
                    buf |= v << bits;
                    bits += width;
                    while (bits >= 8) {
                        out.write((int) (buf & 0xff));
                        buf >>>= 8;
                        bits -= 8;
                    }
                }
                if (bits > 0) {
                    out.write((int) (buf & 0xff));
                }
            }
        }
        return out.toByteArray();
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
