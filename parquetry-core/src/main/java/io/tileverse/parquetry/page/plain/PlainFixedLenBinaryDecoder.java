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
package io.tileverse.parquetry.page.plain;

import java.nio.ByteBuffer;

import io.tileverse.parquetry.page.PageDecoder;

/**
 * PLAIN decoder for FIXED_LEN_BYTE_ARRAY: exactly N bytes per value, where N comes from the column schema's
 * {@code typeLength} field.
 *
 * <p>Each value is returned as a read-only {@link ByteBuffer} slice backed by the original page buffer (zero-copy). The
 * slice's {@code remaining()} equals {@code length}.
 */
public final class PlainFixedLenBinaryDecoder implements PageDecoder<ByteBuffer> {

    private final int length;
    private ByteBuffer buffer;

    /** @param length bytes per value; must be non-negative (zero is valid for degenerate schemas) */
    public PlainFixedLenBinaryDecoder(int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative; got " + length);
        }
        this.length = length;
    }

    @Override
    public void load(ByteBuffer page, int valueCount) {
        this.buffer = page;
    }

    @Override
    public ByteBuffer next() {
        ByteBuffer slice = buffer.slice().limit(length).asReadOnlyBuffer();
        buffer.position(buffer.position() + length);
        return slice;
    }

    @Override
    public void skip(int n) {
        buffer.position(buffer.position() + n * length);
    }
}
