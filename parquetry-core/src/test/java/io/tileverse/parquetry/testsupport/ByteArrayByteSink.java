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
package io.tileverse.parquetry.testsupport;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import io.tileverse.parquetry.io.ByteSink;

/**
 * In-memory {@link ByteSink} for write-side tests. Captures appended bytes via a backing {@link ByteArrayOutputStream}.
 */
public final class ByteArrayByteSink implements ByteSink {

    private final ByteArrayOutputStream sink = new ByteArrayOutputStream();
    private long position;

    @Override
    public void write(MemorySegment src) {
        byte[] bytes = src.toArray(ValueLayout.JAVA_BYTE);
        sink.writeBytes(bytes);
        position += bytes.length;
    }

    @Override
    public long position() {
        return position;
    }

    public byte[] toByteArray() {
        return sink.toByteArray();
    }

    public int size() {
        return sink.size();
    }

    @Override
    public void close() {
        // In-memory sink; nothing to release.
    }
}
