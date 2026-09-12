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

import java.io.EOFException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

/**
 * A thread-safe, positional, read-only byte source of known size. parquetry's single read dependency.
 *
 * <p>Concurrent calls at different offsets are safe; implementations never carry a shared read position. There is
 * deliberately no JDK parent interface (no {@link java.nio.channels.ReadableByteChannel}) - parquetry only ever reads
 * positionally, and a sequential-read method would invite misuse.
 */
public interface ByteRangeSource extends AutoCloseable {

    /** Total byte length of the source; stable for the source's lifetime. */
    long size();

    /**
     * A stable name for the underlying object - typically its URI or absolute path - which parquetry pairs with
     * {@link #size()} as the identity of the file's content when it caches metadata derived from it. Two sources over
     * the same object return equal names; two sources over different objects do not. An implementation returns a name
     * only when that name plus the length pins the content: an object rewritten in place to the same length under the
     * same name would otherwise be served the metadata of its previous content.
     *
     * <p>Empty, the default, means the source cannot make that promise, and every reader opened over it derives its own
     * metadata.
     */
    default Optional<String> sourceIdentifier() {
        return Optional.empty();
    }

    /**
     * Reads up to {@code dst.byteSize()} bytes starting at {@code offset} into {@code dst}, returning the number of
     * bytes actually read (fewer than requested only when {@code offset + dst.byteSize()} runs past {@link #size()}),
     * or {@code -1} if {@code offset} is at or past end of source. Returns {@code 0} only when {@code dst.byteSize()}
     * is zero; for a non-empty destination with {@code offset < size()} the read always makes progress (it never
     * returns {@code 0}), which is what lets {@link #readFully} loop safely. Concurrent calls at different offsets are
     * safe.
     *
     * <p>{@code dst} must be writable and no larger than {@link Integer#MAX_VALUE} bytes.
     *
     * @throws UncheckedIOException if the underlying read fails
     * @throws IllegalArgumentException if {@code offset} is negative, {@code dst} is read-only, or
     *     {@code dst.byteSize()} exceeds {@link Integer#MAX_VALUE}
     */
    int read(long offset, MemorySegment dst);

    /**
     * Reads exactly {@code dst.byteSize()} bytes at {@code offset}. The ergonomic call for parquetry's known-length
     * reads (footer, column chunks, indexes).
     *
     * @throws UncheckedIOException wrapping an {@link EOFException} on a short read
     */
    default void readFully(long offset, MemorySegment dst) {
        long length = dst.byteSize();
        long done = 0;
        while (done < length) {
            MemorySegment remaining = dst.asSlice(done, length - done);
            int read = read(offset + done, remaining);
            if (read < 0) {
                throw new UncheckedIOException(new EOFException("Reached end of source at offset " + (offset + done)
                        + " with " + (length - done) + " bytes still requested"));
            }
            done += read;
        }
    }

    /** Narrowed: no checked exception (matches parquetry's RuntimeException-only public API). */
    @Override
    void close();

    /**
     * Opens a {@link FileChannel} over {@code path} and OWNS it; {@link #close()} closes the channel.
     *
     * @throws UncheckedIOException if the file cannot be opened or its size cannot be determined
     */
    static ByteRangeSource ofFile(Path path) {
        return FileChannelByteRangeSource.owning(Objects.requireNonNull(path, "path"));
    }

    /**
     * Wraps an existing {@link FileChannel}; BORROWS it - {@link #close()} does not close the channel. The channel must
     * stay open for the lifetime of the returned source.
     *
     * @throws UncheckedIOException if the channel size cannot be determined
     */
    static ByteRangeSource ofChannel(FileChannel channel) {
        return FileChannelByteRangeSource.borrowing(Objects.requireNonNull(channel, "channel"));
    }
}
