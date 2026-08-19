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
package io.tileverse.parquetry.internal.write.page;

import java.io.IOException;

/**
 * Shared RLE/bit-packed hybrid writer used by all encoders that emit RLE-Bit-Packed streams: level streams, dictionary
 * indices, and boolean RLE pages.
 *
 * <p>Inverse of {@link io.tileverse.parquetry.internal.read.page.LevelDecoder}. For each input run the writer chooses
 * the smaller of two encodings:
 *
 * <ul>
 *   <li><b>RLE</b>: a varint header {@code (length << 1)} followed by a {@code ceil(bitWidth/8)}-byte little-endian
 *       value. Used when one value repeats {@code >= 8} times.
 *   <li><b>Bit-packed</b>: a varint header {@code ((groups << 1) | 1)} followed by {@code groups * bitWidth} bytes,
 *       where each group covers exactly 8 values packed LSB-first. The number of values encoded is always a multiple of
 *       8; trailing slots in the last group are filled with zero.
 * </ul>
 *
 * <p>The encoding strategy here is intentionally simple: scan for runs of equal consecutive values and emit them as RLE
 * if {@code >= 8} long, otherwise accumulate into a bit-packed group. This matches what Parquet readers expect and what
 * reference encoders (parquet-java, arrow-cpp) produce, even if it is not always the absolute minimum byte-count
 * encoding.
 */
public final class RleBitPackedHybridWriter {

    private static final int MIN_RLE_RUN = 8;

    private RleBitPackedHybridWriter() {}

    /** Encode {@code n} values of width {@code bitWidth} from {@code values} into {@code dst}. */
    public static int write(int[] values, int n, int bitWidth, LittleEndianSink dst) throws IOException {
        if (bitWidth < 0 || bitWidth > 32) {
            throw new IllegalArgumentException("bitWidth must be in [0, 32]; got " + bitWidth);
        }
        if (n == 0) {
            return 0;
        }
        if (bitWidth == 0) {
            // No bits per value; nothing to emit. Decoder returns zero for any read at this width.
            return 0;
        }
        int start = dst.size();
        int bytesPerRleValue = (bitWidth + 7) / 8;
        int cursor = 0;
        while (cursor < n) {
            int runLength = scanRunLength(values, cursor, n);
            if (runLength >= MIN_RLE_RUN) {
                writeRleRun(dst, runLength, values[cursor], bytesPerRleValue);
                cursor += runLength;
            } else {
                int bitPackedEnd = scanBitPackedSpan(values, cursor, n);
                writeBitPackedRun(dst, values, cursor, bitPackedEnd, bitWidth);
                cursor = bitPackedEnd;
            }
        }
        return dst.size() - start;
    }

    /**
     * Encode {@code n} all-zero values as a single bit-width-0 RLE run: one varint run header and no value bytes.
     * Returns bytes written.
     *
     * <p>{@link #write} drops a bit-width-0 stream to zero bytes, which is what level streams need (Parquet omits level
     * data when the max level is 0). A dictionary index stream cannot drop it: even when the chunk dictionary holds a
     * single value and every index is 0, the reader must still learn how many values the page holds. An empty stream
     * makes a strict reader throw "Reading past RLE/BitPacking stream" when it reads the first value, because the run
     * header that declares the value count is missing.
     */
    public static int writeZeroWidthRun(int n, LittleEndianSink dst) throws IOException {
        if (n == 0) {
            return 0;
        }
        int start = dst.size();
        writeRleRun(dst, n, 0, 0);
        return dst.size() - start;
    }

    /** Return the length of the run of equal values starting at {@code start}. */
    private static int scanRunLength(int[] values, int start, int n) {
        int end = start + 1;
        while (end < n && values[end] == values[start]) {
            end++;
        }
        return end - start;
    }

    /**
     * Return the exclusive end of a bit-packed span starting at {@code start}. The span stops when an RLE-worthy run
     * (>= {@link #MIN_RLE_RUN} equal values) begins, and is rounded up to a multiple of 8 to fit complete bit-packed
     * groups.
     */
    private static int scanBitPackedSpan(int[] values, int start, int n) {
        int cursor = start;
        while (cursor < n) {
            int runLength = scanRunLength(values, cursor, n);
            if (runLength >= MIN_RLE_RUN) {
                break;
            }
            cursor += runLength;
        }
        // Pad to a multiple of 8 since each bit-packed group covers 8 values.
        int span = cursor - start;
        int padded = (span + 7) & ~7;
        return Math.min(start + padded, n);
    }

    private static void writeRleRun(LittleEndianSink out, int runLength, int value, int bytesPerRleValue) {
        writeVarint(out, runLength << 1);
        for (int b = 0; b < bytesPerRleValue; b++) {
            out.writeByte((value >>> (b * 8)) & 0xff);
        }
    }

    /** Encode {@code [start, end)} as one or more bit-packed groups; pad the trailing group with zeros if needed. */
    private static void writeBitPackedRun(LittleEndianSink out, int[] values, int start, int end, int bitWidth) {
        int span = end - start;
        // The decoder reads in multiples of 8; ensure we emit complete groups.
        int paddedSpan = (span + 7) & ~7;
        int groups = paddedSpan / 8;
        writeVarint(out, (groups << 1) | 1);

        long buffer = 0L;
        int bitsBuffered = 0;
        long mask = (bitWidth == 64) ? -1L : ((1L << bitWidth) - 1L);
        for (int i = 0; i < paddedSpan; i++) {
            long value = (i < span) ? (values[start + i] & mask) : 0L;
            buffer |= value << bitsBuffered;
            bitsBuffered += bitWidth;
            while (bitsBuffered >= 8) {
                out.writeByte((int) (buffer & 0xff));
                buffer >>>= 8;
                bitsBuffered -= 8;
            }
        }
        if (bitsBuffered > 0) {
            out.writeByte((int) (buffer & 0xff));
        }
    }

    private static void writeVarint(LittleEndianSink out, int value) {
        long v = value & 0xffffffffL;
        while ((v & ~0x7fL) != 0L) {
            out.writeByte((int) ((v & 0x7f) | 0x80));
            v >>>= 7;
        }
        out.writeByte((int) v);
    }
}
