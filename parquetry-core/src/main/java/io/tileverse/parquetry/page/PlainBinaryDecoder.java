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

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/**
 * PLAIN decoder for BYTE_ARRAY: a 4-byte little-endian length prefix followed by that many bytes per value.
 *
 * <p>Each value is returned as a read-only {@link MemorySegment} that views the original page bytes (zero-copy) but has
 * its own immutable bounds, so consumers can read it concurrently without position-sharing hazards.
 */
public final class PlainBinaryDecoder implements PageDecoder<MemorySegment> {

    private ByteBuffer buffer;

    @Override
    public void load(ByteBuffer page, int valueCount) {
        this.buffer = page.order(LITTLE_ENDIAN);
    }

    @Override
    public MemorySegment next() {
        int length = buffer.getInt();
        ByteBuffer slice = buffer.slice().limit(length);
        buffer.position(buffer.position() + length);
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
        for (int i = 0; i < n; i++) {
            int length = buffer.getInt();
            buffer.position(buffer.position() + length);
        }
    }
}
