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
import java.nio.ByteOrder;

/**
 * PLAIN decoder for BYTE_ARRAY: a 4-byte little-endian length prefix followed by that many bytes per value.
 *
 * <p>Each value is returned as a read-only {@link ByteBuffer} slice backed by the original page buffer (zero-copy). The
 * slice's {@code remaining()} equals the byte array length.
 */
public final class PlainBinaryDecoder implements PageDecoder<ByteBuffer> {

    private ByteBuffer buffer;

    @Override
    public void load(ByteBuffer page, int valueCount) {
        this.buffer = page.order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public ByteBuffer next() {
        int length = buffer.getInt();
        ByteBuffer slice = buffer.slice().limit(length).asReadOnlyBuffer();
        buffer.position(buffer.position() + length);
        return slice;
    }

    @Override
    public void skip(int n) {
        for (int i = 0; i < n; i++) {
            int length = buffer.getInt();
            buffer.position(buffer.position() + length);
        }
    }
}
