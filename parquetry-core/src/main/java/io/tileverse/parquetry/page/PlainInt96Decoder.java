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
 * PLAIN decoder for INT96: twelve bytes, little-endian per value.
 *
 * <p>INT96 is a deprecated legacy timestamp type used by some older Parquet writers. Each value is returned as a
 * read-only twelve-byte {@link MemorySegment} view of the page bytes (zero-copy).
 */
public final class PlainInt96Decoder implements PageDecoder<MemorySegment> {

    private static final int INT96_BYTES = 12;

    private ByteBuffer buffer;

    @Override
    public void load(ByteBuffer page, int valueCount) {
        this.buffer = page;
    }

    @Override
    public MemorySegment next() {
        ByteBuffer slice = buffer.slice().limit(INT96_BYTES);
        buffer.position(buffer.position() + INT96_BYTES);
        return MemorySegment.ofBuffer(slice).asReadOnly();
    }

    @Override
    public void decodeBinary(int n, MemorySegment[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = next();
        }
    }

    @Override
    public void skip(int n) {
        buffer.position(buffer.position() + n * INT96_BYTES);
    }
}
