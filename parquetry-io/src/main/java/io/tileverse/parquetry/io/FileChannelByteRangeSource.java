/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Local-file {@link ByteRangeSource} backed by positional {@link FileChannel#read(ByteBuffer, long)} reads, which map
 * to {@code pread} (POSIX) / offset'd {@code ReadFile} (Windows): stateless, thread-safe, and correct over NFS and SMB
 * with a single shared channel under concurrent reads.
 *
 * <p>NFS resilience: a read that fails with a stale file handle (ESTALE) or a closed channel reopens the channel once
 * and retries, which recovers transient handle invalidation without the cost of holding a channel open across idle
 * periods. The channel is opened at construction and held until {@link #close()} - parquetry opens and closes a source
 * per operation, narrowing the window for staleness to one read rather than the process lifetime. A borrowed channel
 * cannot reopen (the source does not own it) and propagates the error.
 */
final class FileChannelByteRangeSource implements ByteRangeSource {

    /** Reopens the backing channel; the reopen seam for the owning source, overridable in tests. */
    @FunctionalInterface
    interface ChannelOpener {
        FileChannel open() throws IOException;
    }

    private final Path path;
    private final ChannelOpener opener;
    private final long size;
    private final boolean ownsChannel;
    private final ReentrantLock reopenLock = new ReentrantLock();
    private volatile FileChannel channel;
    private volatile boolean closed;

    private FileChannelByteRangeSource(
            Path path, ChannelOpener opener, FileChannel channel, long size, boolean ownsChannel) {
        this.path = path;
        this.opener = opener;
        this.channel = channel;
        this.size = size;
        this.ownsChannel = ownsChannel;
    }

    static FileChannelByteRangeSource owning(Path path) {
        return owning(path, () -> FileChannel.open(path, StandardOpenOption.READ));
    }

    static FileChannelByteRangeSource owning(Path path, ChannelOpener opener) {
        try {
            FileChannel channel = opener.open();
            return new FileChannelByteRangeSource(path, opener, channel, channel.size(), true);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not open " + path, e);
        }
    }

    static FileChannelByteRangeSource borrowing(FileChannel channel) {
        try {
            return new FileChannelByteRangeSource(null, null, channel, channel.size(), false);
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
        boolean reopened = false;
        while (buffer.hasRemaining()) {
            FileChannel current = channel;
            try {
                int read = current.read(buffer, offset + total);
                if (read < 0) {
                    break;
                }
                total += read;
            } catch (IOException e) {
                if (ownsChannel && !reopened && !closed && isRecoverable(e)) {
                    reopen(current);
                    reopened = true;
                    continue;
                }
                throw new UncheckedIOException("Read failed at offset " + (offset + total), e);
            }
        }
        return total == 0 ? -1 : total;
    }

    /**
     * Replaces the failed channel with a fresh one, once. Concurrent readers that hit the same stale channel funnel
     * through the lock; only the first reopens (the others find the channel already replaced and return).
     */
    private void reopen(FileChannel failed) {
        reopenLock.lock();
        try {
            if (channel == failed && !closed) {
                closeQuietly(failed);
                channel = opener.open();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Could not reopen " + path, e);
        } finally {
            reopenLock.unlock();
        }
    }

    private static boolean isRecoverable(IOException e) {
        if (e instanceof ClosedChannelException) {
            return true;
        }
        String message = e.getMessage();
        return message != null && message.toLowerCase(Locale.ROOT).contains("stale file handle");
    }

    @Override
    public void close() {
        if (!ownsChannel) {
            return;
        }
        closed = true;
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Could not close channel", e);
        }
    }

    private static void closeQuietly(FileChannel channel) {
        try {
            channel.close();
        } catch (IOException ignored) {
            // closing a stale or already-closed channel is expected during reopen
        }
    }
}
