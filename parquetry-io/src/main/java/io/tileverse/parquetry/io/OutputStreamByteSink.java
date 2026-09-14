/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.io;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * A {@link ByteSink} backed by an {@link OutputStream}, appending sequentially. The stream is OWNED: {@link #close()}
 * closes it, which commits an object-storage upload whose backing stream commits on close. Not thread-safe; one writer
 * at a time.
 *
 * <p>Bytes go straight to the stream, never through {@link java.nio.channels.Channels#newChannel(OutputStream)}: that
 * adapter is an interruptible channel which closes its stream when the writing thread is interrupted, and for a stream
 * that commits on close that would turn a cancelled write into a commit.
 */
final class OutputStreamByteSink implements ByteSink {

    private static final int COPY_CHUNK_BYTES = 64 * 1024;

    private final OutputStream out;
    private long position;
    private boolean closed;

    OutputStreamByteSink(OutputStream out) {
        this.out = out;
    }

    @Override
    public void write(MemorySegment src) {
        byte[] chunk = new byte[(int) Math.min(COPY_CHUNK_BYTES, src.byteSize())];
        try {
            for (long offset = 0; offset < src.byteSize(); offset += chunk.length) {
                int length = (int) Math.min(chunk.length, src.byteSize() - offset);
                MemorySegment.copy(src, ValueLayout.JAVA_BYTE, offset, chunk, 0, length);
                out.write(chunk, 0, length);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Write failed at position " + position, e);
        }
        position += src.byteSize();
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            out.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Close failed", e);
        }
    }
}
