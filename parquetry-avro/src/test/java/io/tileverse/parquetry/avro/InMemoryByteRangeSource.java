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
package io.tileverse.parquetry.avro;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import io.tileverse.parquetry.io.ByteRangeSource;

/** Test-only in-memory ByteRangeSource over a byte array. */
final class InMemoryByteRangeSource implements ByteRangeSource {

    private final byte[] data;

    InMemoryByteRangeSource(byte[] data) {
        this.data = data;
    }

    @Override
    public long size() {
        return data.length;
    }

    @Override
    public int read(long offset, MemorySegment dst) {
        if (offset >= data.length) {
            return -1;
        }
        int length = (int) Math.min(dst.byteSize(), data.length - offset);
        MemorySegment.copy(MemorySegment.ofArray(data), offset, dst, 0, length);
        return length;
    }

    @Override
    public void close() {
        // Intentionally a no-op: an in-memory source holds no OS resources to release.
    }

    byte[] bytes() {
        return data;
    }

    static byte[] toArray(MemorySegment segment) {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }
}
