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

import static io.tileverse.parquetry.format.ParquetLayouts.INT64;

import java.lang.foreign.MemorySegment;

/** PLAIN decoder for INT64: eight bytes, little-endian per value. */
public final class PlainInt64Decoder implements PageDecoder<Long> {

    private static final int BYTES_PER_VALUE = Long.BYTES;

    private MemorySegment segment;
    private long offset;

    @Override
    public void load(MemorySegment page, int valueCount) {
        this.segment = page;
        this.offset = 0L;
    }

    @Override
    public Long next() {
        long value = segment.get(INT64, offset);
        offset += BYTES_PER_VALUE;
        return value;
    }

    @Override
    public void decodeLongs(int n, long[] dst, int dstOffset) {
        MemorySegment.copy(segment, INT64, offset, dst, dstOffset, n);
        offset += (long) n * BYTES_PER_VALUE;
    }

    @Override
    public void skip(int n) {
        offset += (long) n * BYTES_PER_VALUE;
    }
}
