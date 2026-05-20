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
package io.tileverse.parquetry.page;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/**
 * DELTA_BYTE_ARRAY page decoder.
 *
 * <p>Page layout (parquet-format Encodings.md):
 *
 * <pre>
 *   prefix-lengths (DELTA_BINARY_PACKED i32, N entries)
 *   suffix-lengths (DELTA_BINARY_PACKED i32, N entries)
 *   suffix-bytes   (concatenated)
 * </pre>
 *
 * <p>Reconstruction: {@code V[i] = V[i-1][0..P[i]] + S[i]}. The first prefix length must be zero; each subsequent value
 * reuses a leading slice of the previous value and appends the suffix bytes from the page.
 *
 * <p>Each returned value is a read-only {@link MemorySegment} over a fresh {@code byte[]} so the materialized values
 * remain stable across subsequent {@link #next()} calls (and across the page buffer's recycle into the pool).
 */
final class DeltaByteArrayDecoder implements PageDecoder<MemorySegment> {

    private int[] prefixLengths;
    private int[] suffixLengths;
    private ByteBuffer suffixBytes;
    private byte[] previousValue;
    private int cursor;

    @Override
    public void load(ByteBuffer page, int valueCount) {
        // Use a slice so the caller's buffer position is not disturbed.
        // DeltaBinaryPackedDecoder advances view.position() as it reads, so after
        // decoding both length arrays the buffer position sits at the suffix bytes.
        ByteBuffer view = page.slice();

        prefixLengths = decodeDeltaInts(view);
        suffixLengths = decodeDeltaInts(view);
        suffixBytes = view;
        previousValue = new byte[0];
        cursor = 0;
    }

    @Override
    public MemorySegment next() {
        int prefixLen = prefixLengths[cursor];
        int suffixLen = suffixLengths[cursor];
        cursor++;

        byte[] value = new byte[prefixLen + suffixLen];
        if (prefixLen > 0) {
            System.arraycopy(previousValue, 0, value, 0, prefixLen);
        }
        if (suffixLen > 0) {
            suffixBytes.get(value, prefixLen, suffixLen);
        }
        previousValue = value;
        return MemorySegment.ofArray(value).asReadOnly();
    }

    @Override
    public void skip(int n) {
        // Must still reconstruct each value to keep previousValue current for prefix reuse.
        for (int i = 0; i < n; i++) {
            next();
        }
    }

    /**
     * Decodes a DELTA_BINARY_PACKED integer sequence from {@code buffer}, advancing its position past all block bytes
     * including any padding values used to fill the last block. Returns only the {@code totalValueCount} real values as
     * an int array.
     *
     * <p>DELTA_BINARY_PACKED encodes values in fixed-size blocks. The encoder pads the last block to a full block
     * boundary. If we stop reading after {@code totalValueCount} real values, the buffer position may be mid-block.
     * Draining the padding values forces the buffer position to advance past the entire encoding so the next segment
     * starts correctly.
     */
    private static int[] decodeDeltaInts(ByteBuffer buffer) {
        DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder();
        decoder.load(buffer);
        int realCount = decoder.totalValueCount();
        int paddedCount = decoder.paddedValueCount();
        int[] values = new int[realCount];
        for (int i = 0; i < realCount; i++) {
            values[i] = (int) decoder.next();
        }
        // Drain padding values so buffer.position() advances past the complete block bytes.
        int paddingCount = paddedCount - realCount;
        if (paddingCount > 0) {
            decoder.skip(paddingCount);
        }
        return values;
    }
}
