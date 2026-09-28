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

/**
 * A {@link ByteRangeSource} over a {@link FileChannel} owned by the caller, read positionally
 * ({@link FileChannel#read(ByteBuffer, long)}: {@code pread} on POSIX, an offset {@code ReadFile} on Windows), keeping
 * concurrent reads independent of each other. The channel must stay open for the source's lifetime: {@link #close()}
 * leaves it open, and a channel closed by its owner fails every later read, since the source cannot reopen what it does
 * not own. The source is unnamed, a channel having no path of its own.
 */
final class BorrowedChannelByteRangeSource implements ByteRangeSource {

    private final FileChannel channel;
    private final long size;

    /** @throws UncheckedIOException if the channel size cannot be determined */
    BorrowedChannelByteRangeSource(FileChannel channel) {
        this.channel = channel;
        this.size = sizeOf(channel);
    }

    private static long sizeOf(FileChannel channel) {
        try {
            return channel.size();
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
        ReadArguments.requireReadable(offset, dst);
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
            throw new UncheckedIOException("Read failed at offset " + (offset + total), e);
        }
        return total == 0 ? -1 : total;
    }

    @Override
    public void close() {
        // the caller owns the channel and closes it after the last read
    }
}
