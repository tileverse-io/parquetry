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

import java.nio.ByteBuffer;

/**
 * PLAIN decoder for INT96: twelve bytes, little-endian per value.
 *
 * <p>INT96 is a deprecated legacy timestamp type used by some older Parquet writers. Each value is returned as a
 * read-only {@link ByteBuffer} slice of twelve bytes backed by the original page buffer (zero-copy).
 */
final class PlainInt96Decoder implements PageDecoder<ByteBuffer> {

    private static final int INT96_BYTES = 12;

    private ByteBuffer buffer;

    @Override
    public void load(ByteBuffer page, int valueCount) {
        this.buffer = page;
    }

    @Override
    public ByteBuffer next() {
        ByteBuffer slice = buffer.slice().limit(INT96_BYTES).asReadOnlyBuffer();
        buffer.position(buffer.position() + INT96_BYTES);
        return slice;
    }

    @Override
    public void skip(int n) {
        buffer.position(buffer.position() + n * INT96_BYTES);
    }
}
