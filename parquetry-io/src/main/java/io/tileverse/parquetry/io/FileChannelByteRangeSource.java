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
package io.tileverse.parquetry.io;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Local-file {@link ByteRangeSource} backed by positional {@link FileChannel#read(ByteBuffer, long)} reads, which map
 * to {@code pread} (POSIX) / offset'd {@code ReadFile} (Windows): stateless, thread-safe, and correct over NFS and SMB
 * with a single shared channel under concurrent reads.
 */
final class FileChannelByteRangeSource implements ByteRangeSource {

    private final FileChannel channel;
    private final long size;
    private final boolean ownsChannel;

    private FileChannelByteRangeSource(FileChannel channel, long size, boolean ownsChannel) {
        this.channel = channel;
        this.size = size;
        this.ownsChannel = ownsChannel;
    }

    static FileChannelByteRangeSource owning(Path path) {
        try {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
            return new FileChannelByteRangeSource(channel, channel.size(), true);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not open " + path, e);
        }
    }

    static FileChannelByteRangeSource borrowing(FileChannel channel) {
        try {
            return new FileChannelByteRangeSource(channel, channel.size(), false);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not determine channel size", e);
        }
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public int read(long offset, MemorySegment dst) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0, got " + offset);
        }
        if (dst.isReadOnly()) {
            throw new IllegalArgumentException("dst must be writable");
        }
        if (dst.byteSize() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("dst.byteSize() must be <= Integer.MAX_VALUE, got " + dst.byteSize());
        }
        if (dst.byteSize() == 0) {
            return 0;
        }
        if (offset >= size) {
            return -1;
        }
        ByteBuffer buffer = dst.asByteBuffer();
        int total = 0;
        try {
            while (buffer.hasRemaining()) {
                int read = channel.read(buffer, offset + total);
                if (read < 0) {
                    break;
                }
                total += read;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Read failed at offset " + offset, e);
        }
        return total == 0 ? -1 : total;
    }

    @Override
    public void close() {
        if (!ownsChannel) {
            return;
        }
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not close channel", e);
        }
    }
}
