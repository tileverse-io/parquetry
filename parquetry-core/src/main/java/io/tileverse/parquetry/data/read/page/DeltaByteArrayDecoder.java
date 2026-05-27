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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;

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
public final class DeltaByteArrayDecoder implements PageDecoder<MemorySegment> {

    private int[] prefixLengths;
    private int[] suffixLengths;
    private MemorySegment suffixBytes;
    private long suffixOffset;
    private byte[] previousValue;
    private int cursor;

    @Override
    public void load(MemorySegment page, int valueCount) {
        // The two length arrays are packed back-to-back, followed by the suffix bytes. Each DeltaBinaryPackedDecoder
        // reports how many bytes it consumed, so the next section starts right after it.
        DeltaInts prefixes = decodeDeltaInts(page, 0L);
        prefixLengths = prefixes.values();
        DeltaInts suffixes = decodeDeltaInts(page, prefixes.nextOffset());
        suffixLengths = suffixes.values();
        suffixBytes = page.asSlice(suffixes.nextOffset());
        suffixOffset = 0L;
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
            MemorySegment.copy(suffixBytes, JAVA_BYTE, suffixOffset, value, prefixLen, suffixLen);
            suffixOffset += suffixLen;
        }
        previousValue = value;
        return MemorySegment.ofArray(value).asReadOnly();
    }

    @Override
    public void decodeBinary(int n, MemorySegment[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = next();
        }
    }

    @Override
    public void skip(int n) {
        // Must still reconstruct each value to keep previousValue current for prefix reuse.
        for (int i = 0; i < n; i++) {
            next();
        }
    }

    /** A decoded DELTA_BINARY_PACKED integer array paired with the page offset immediately after it. */
    // S6218: internal decode carrier, never compared or hashed. Array-aware equals/hashCode would be dead weight.
    @SuppressWarnings("java:S6218")
    private record DeltaInts(int[] values, long nextOffset) {}

    /**
     * Decodes a DELTA_BINARY_PACKED integer sequence starting at {@code offset}, returning the {@code totalValueCount}
     * real values plus the offset of the byte right after the complete encoding (including padding).
     *
     * <p>DELTA_BINARY_PACKED encodes values in fixed-size blocks; the encoder pads the last block to a full block
     * boundary. Stopping after {@code totalValueCount} real values can leave the cursor mid-block, so the padding
     * values are drained to advance past the entire encoding before reading the offset.
     */
    private static DeltaInts decodeDeltaInts(MemorySegment page, long offset) {
        DeltaBinaryPackedDecoder decoder = new DeltaBinaryPackedDecoder();
        decoder.load(page.asSlice(offset));
        int realCount = decoder.totalValueCount();
        int paddedCount = decoder.paddedValueCount();
        int[] values = new int[realCount];
        for (int i = 0; i < realCount; i++) {
            values[i] = (int) decoder.next();
        }
        int paddingCount = paddedCount - realCount;
        if (paddingCount > 0) {
            decoder.skip(paddingCount);
        }
        return new DeltaInts(values, offset + decoder.position());
    }
}
