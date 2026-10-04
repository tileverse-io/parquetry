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

import static io.tileverse.parquetry.format.ParquetLayouts.INT32;

import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.format.MalformedFileException;

/**
 * PLAIN decoder for BYTE_ARRAY: a 4-byte little-endian length prefix followed by that many bytes per value.
 *
 * <p>Each value is returned as a read-only {@link MemorySegment} that views the original page bytes (zero-copy) but has
 * its own immutable bounds, so consumers can read it concurrently without position-sharing hazards.
 */
public final class PlainBinaryDecoder implements PageDecoder<MemorySegment> {

    private MemorySegment segment;
    private long offset;
    private long limit;

    @Override
    public void load(MemorySegment page, int valueCount) {
        this.segment = page;
        this.offset = 0L;
        this.limit = page.byteSize();
    }

    @Override
    public MemorySegment next() {
        int length = readLengthPrefix();
        MemorySegment value = segment.asSlice(offset, length).asReadOnly();
        offset += length;
        return value;
    }

    @Override
    public void decodeBinary(int n, MemorySegment[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = next();
        }
    }

    @Override
    public void decodeBinaryLayout(int n, int[] positions, int[] lengths, int dst) {
        for (int i = 0; i < n; i++) {
            int length = readLengthPrefix();
            positions[dst + i] = Math.toIntExact(offset);
            lengths[dst + i] = length;
            offset += length;
        }
    }

    @Override
    public void skip(int n) {
        for (int i = 0; i < n; i++) {
            int length = readLengthPrefix();
            offset += length;
        }
    }

    /**
     * Reads the next value's length prefix and steps past it, leaving the cursor on the value's first byte. The prefix
     * and the announced value must both lie inside the page.
     */
    private int readLengthPrefix() {
        requireLengthPrefixInPage();
        int length = segment.get(INT32, offset);
        offset += Integer.BYTES;
        requireValueInPage(length);
        return length;
    }

    private void requireLengthPrefixInPage() {
        if (limit - offset < Integer.BYTES) {
            throw valuesEnded(limit);
        }
    }

    private void requireValueInPage(int length) {
        if (length < 0 || length > limit - offset) {
            throw valueBeyondPage(offset - Integer.BYTES, length, limit - offset);
        }
    }

    // The failures below are built out of line: the checks raising them run once per value, and the JIT only inlines
    // them into the decode loops while they stay small.

    private static MalformedFileException valuesEnded(long byteSize) {
        return new MalformedFileException(
                "PLAIN BYTE_ARRAY values end after " + byteSize + " bytes, short of the values declared by their page");
    }

    private static MalformedFileException valueBeyondPage(long prefixOffset, int length, long remaining) {
        return new MalformedFileException("PLAIN BYTE_ARRAY value at byte " + prefixOffset + " declares a length of "
                + length + " but " + remaining + " bytes remain in the page");
    }
}
