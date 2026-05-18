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
package io.tileverse.parquetry.codec.impl;

import java.nio.ByteBuffer;

/**
 * Package-private utilities for working with {@link ByteBuffer}s in codec implementations.
 */
final class ByteBuffers {

    private ByteBuffers() {}

    /**
     * Returns a byte array containing the remaining bytes of {@code buf}, advancing the
     * position to the limit. When the buffer is a heap buffer backed by exactly its full
     * array (offset 0, position 0, limit == array length), returns the backing array
     * directly to avoid a copy; otherwise copies the remaining bytes.
     */
    static byte[] toArray(ByteBuffer buf) {
        if (buf.hasArray() && buf.arrayOffset() == 0 && buf.position() == 0 && buf.limit() == buf.array().length) {
            buf.position(buf.limit());
            return buf.array();
        }
        byte[] copy = new byte[buf.remaining()];
        buf.get(copy);
        return copy;
    }
}
