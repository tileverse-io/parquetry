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

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import io.tileverse.parquetry.io.ByteSink;

/** A {@link ByteSink} that accumulates into memory, for tests that read the bytes straight back. */
final class InMemoryByteSink implements ByteSink {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    @Override
    public void write(MemorySegment src) {
        out.writeBytes(src.toArray(ValueLayout.JAVA_BYTE));
    }

    @Override
    public long position() {
        return out.size();
    }

    @Override
    public void close() {
        // nothing to release
    }

    byte[] toByteArray() {
        return out.toByteArray();
    }
}
