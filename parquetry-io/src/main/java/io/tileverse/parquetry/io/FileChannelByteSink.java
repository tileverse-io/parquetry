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
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * A {@link ByteSink} backed by a {@link FileChannel}, appending sequentially. Not thread-safe; one writer at a time.
 */
final class FileChannelByteSink implements ByteSink {

    private final FileChannel channel;
    private final boolean ownsChannel;
    private long position;

    private FileChannelByteSink(FileChannel channel, boolean ownsChannel, long initialPosition) {
        this.channel = channel;
        this.ownsChannel = ownsChannel;
        this.position = initialPosition;
    }

    static FileChannelByteSink owning(Path path) {
        try {
            FileChannel channel = FileChannel.open(
                    path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            return new FileChannelByteSink(channel, true, 0);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot open " + path + " for writing", e);
        }
    }

    static FileChannelByteSink borrowing(FileChannel channel) {
        try {
            return new FileChannelByteSink(channel, false, channel.position());
        } catch (IOException e) {
            throw new UncheckedIOException("Could not determine channel position", e);
        }
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
    public void transferFrom(FileChannel src, long srcPosition, long count) {
        long remaining = count;
        long pos = srcPosition;
        try {
            while (remaining > 0L) {
                long transferred = src.transferTo(pos, remaining, channel);
                if (transferred == 0L) {
                    throw new IOException("FileChannel.transferTo made no progress");
                }
                pos += transferred;
                remaining -= transferred;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("transferFrom failed at position " + position, e);
        }
        position += count;
    }

    @Override
    public long position() {
        return position;
    }

    @Override
    public void close() {
        if (!ownsChannel) {
            return;
        }
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Close failed", e);
        }
    }
}
