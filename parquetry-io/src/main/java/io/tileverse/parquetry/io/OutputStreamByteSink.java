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
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 * A {@link ByteSink} backed by an {@link OutputStream}, appending sequentially. The stream is OWNED: {@link #close()}
 * closes it, which commits an object-storage upload whose backing stream commits on close. Not thread-safe; one writer
 * at a time.
 */
final class OutputStreamByteSink implements ByteSink {

    private final OutputStream out;

    // channel wraps out and owns nothing extra; closing out is sufficient (closing channel would close out early).
    private final WritableByteChannel channel;

    private long position;
    private boolean closed;

    OutputStreamByteSink(OutputStream out) {
        this.out = out;
        this.channel = Channels.newChannel(out);
    }

    @Override
    public void write(MemorySegment src) {
        ByteBuffer buffer = src.asByteBuffer();
        try {
            while (buffer.hasRemaining()) {
                channel.write(buffer);
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
