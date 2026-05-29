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

import static io.tileverse.parquetry.format.ParquetLayouts.INT32;

import java.lang.foreign.MemorySegment;

/**
 * PLAIN decoder for BYTE_ARRAY: a 4-byte little-endian length prefix followed by that many bytes per value.
 *
 * <p>Each value is returned as a read-only {@link MemorySegment} that views the original page bytes (zero-copy) but has
 * its own immutable bounds, so consumers can read it concurrently without position-sharing hazards.
 */
public final class PlainBinaryDecoder implements PageDecoder<MemorySegment> {

    private MemorySegment segment;
    private long offset;

    @Override
    public void load(MemorySegment page, int valueCount) {
        this.segment = page;
        this.offset = 0L;
    }

    @Override
    public MemorySegment next() {
        int length = segment.get(INT32, offset);
        offset += Integer.BYTES;
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
    public void skip(int n) {
        for (int i = 0; i < n; i++) {
            int length = segment.get(INT32, offset);
            offset += Integer.BYTES + length;
        }
    }
}
