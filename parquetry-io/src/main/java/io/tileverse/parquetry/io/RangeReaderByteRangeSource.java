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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.file.FileStorageProvider;

import io.tileverse.io.ByteRange;

/**
 * Adapts a tileverse-storage {@link RangeReader} to a {@link ByteRangeSource}. The adapter either borrows the reader,
 * leaving {@link #close()} a no-op for a reader shared and cached across reads, or owns it, closing the reader with the
 * source for a reader handed out fresh by a {@code Storage} or opened over a local file.
 *
 * <p>Every failure is reported through the unchecked I/O contract of {@link ByteRangeSource}. A storage failure becomes
 * an {@link UncheckedIOException} over the reader's own {@link IOException} when it has one, over a
 * {@link NoSuchFileException} for a missing object, and over a plain {@link IOException} otherwise. A read after
 * {@link #close()} of an owned reader, or after the owner of a borrowed reader closed it (reported by the reader as an
 * {@link IllegalStateException}), becomes an {@link UncheckedIOException} over a {@link ClosedChannelException}.
 */
final class RangeReaderByteRangeSource implements ByteRangeSource {

    private final RangeReader reader;
    private final boolean ownsReader;
    private final long size;
    private volatile boolean closed;

    /**
     * @throws IllegalStateException if the reader cannot report its size; an owned reader is closed before the throw
     * @throws UncheckedIOException if asking the reader for its size fails; an owned reader is closed before the throw
     */
    RangeReaderByteRangeSource(RangeReader reader, boolean ownsReader) {
        this.reader = reader;
        this.ownsReader = ownsReader;
        this.size = sizeOf(reader, ownsReader);
    }

    /**
     * Opens a local file through tileverse-storage's file reader and owns it. The reader inspects the file at once and
     * opens its channel on the first read; an idle timeout of zero keeps that channel open until {@link #close()}.
     *
     * @throws UncheckedIOException over a {@link NoSuchFileException} when the file does not exist, or over the failure
     *     raised while inspecting it
     * @throws IllegalArgumentException if {@code path} is a directory, or the timeout is negative or above zero and
     *     below one millisecond
     */
    static ByteRangeSource openFile(Path path, Duration idleTimeout) {
        try {
            RangeReader reader = FileStorageProvider.openRangeReader(path, idleTimeout);
            return new RangeReaderByteRangeSource(reader, true);
        } catch (StorageException e) {
            throw translate(e, "Could not open " + path, path.toString());
        }
    }

    private static long sizeOf(RangeReader reader, boolean ownsReader) {
        String source = reader.getSourceIdentifier();
        OptionalLong knownSize;
        try {
            knownSize = reader.size();
        } catch (StorageException e) {
            throw withOwnedReaderClosed(reader, ownsReader, translate(e, "Could not size " + source, source));
        }
        if (knownSize.isPresent()) {
            return knownSize.getAsLong();
        }
        IllegalStateException noSize = new IllegalStateException("RangeReader cannot determine source size: " + source);
        throw withOwnedReaderClosed(reader, ownsReader, noSize);
    }

    /** Closes an owned reader before {@code failure} is thrown, keeping a failure to close as a suppressed one. */
    private static <T extends RuntimeException> T withOwnedReaderClosed(
            RangeReader reader, boolean ownsReader, T failure) {
        if (!ownsReader) {
            return failure;
        }
        try {
            reader.close();
        } catch (IOException | RuntimeException closeFailure) {
            failure.addSuppressed(closeFailure);
        }
        return failure;
    }

    @Override
    public long size() {
        return size;
    }

    /**
     * The reader's own source identifier, typically the object's URI. A reader that declines to name its source leaves
     * this adapter unnamed rather than failing the read.
     */
    @Override
    public Optional<String> sourceIdentifier() {
        return Optional.ofNullable(reader.getSourceIdentifier());
    }

    @Override
    public int read(long offset, MemorySegment dst) {
        ReadArguments.requireReadable(offset, dst);
        long length = dst.byteSize();
        if (length == 0) {
            return 0;
        }
        if (offset >= size) {
            return -1;
        }
        if (closed) {
            throw readAfterClose(offset, null);
        }
        // RangeReader.readRange may return a short count for cloud backends; loop to honor the
        // ByteRangeSource guarantee that an in-bounds read is fully satisfied.
        int requested = (int) length;
        int total = 0;
        try {
            while (total < requested) {
                ByteBuffer target = dst.asSlice(total, (long) requested - total).asByteBuffer();
                int read = reader.readRange(offset + total, requested - total, target);
                if (read <= 0) {
                    break;
                }
                total += read;
            }
        } catch (StorageException e) {
            String source = reader.getSourceIdentifier();
            throw translate(e, "Read failed at offset " + (offset + total) + " of " + source, source);
        } catch (IllegalStateException e) {
            throw readAfterClose(offset + total, e);
        }
        return total == 0 ? -1 : total;
    }

    /**
     * The failure for a read on a closed reader: over a {@link ClosedChannelException}, with the reader's own report of
     * its closed state as the cause when the reader is the one refusing the read.
     */
    private UncheckedIOException readAfterClose(long offset, IllegalStateException readerReport) {
        ClosedChannelException closedChannel = new ClosedChannelException();
        if (readerReport != null) {
            closedChannel.initCause(readerReport);
        }
        String source = reader.getSourceIdentifier();
        return new UncheckedIOException("Read at offset " + offset + " after close of " + source, closedChannel);
    }

    /**
     * The unchecked I/O failure for a storage failure: over the reader's own {@link IOException} when it has one, the
     * storage failure kept as a suppressed exception; else over a {@link NoSuchFileException} for a missing object or a
     * plain {@link IOException}, with the storage failure as the cause.
     */
    private static UncheckedIOException translate(StorageException failure, String message, String source) {
        if (failure.getCause() instanceof IOException io) {
            UncheckedIOException translated = new UncheckedIOException(message, io);
            translated.addSuppressed(failure);
            return translated;
        }
        IOException io = failure instanceof NotFoundException
                ? new NoSuchFileException(source)
                : new IOException(failure.getMessage());
        io.initCause(failure);
        return new UncheckedIOException(message, io);
    }

    /**
     * Hands the whole batch to the reader in one call, which lets the backend merge, reorder and parallelize the ranges
     * under its own policy, and passes the reader's own accounting of the call back to the caller. The reader validates
     * the batch before any I/O and fills in full every request that it can serve; the one thing left to this source is
     * to turn the reader's end-of-source signal (a short count) into the exception promised by this source.
     */
    @Override
    public BatchReadResult readFully(List<RangeRequest> requests) {
        BatchReadResult result = reader.readRanges(requests);
        for (int i = 0; i < requests.size(); i++) {
            requireFullyRead(requests.get(i), result.bytesRead(i));
        }
        return result;
    }

    /**
     * A count short of the request's length means the range ran past end of source, because a reader fills in full
     * every request that it can serve at all; {@link ByteRangeSource#readFully(List)} promises an exception for that
     * case rather than a partly filled target.
     */
    private void requireFullyRead(RangeRequest request, int bytesRead) {
        ByteRange range = request.range();
        if (bytesRead == range.length()) {
            return;
        }
        throw new UncheckedIOException(new EOFException("Reached end of source at offset " + range.offset() + " with "
                + (range.length() - bytesRead) + " bytes still requested in "
                + sourceIdentifier().orElse("an unnamed source")));
    }

    @Override
    public void close() {
        if (ownsReader) {
            closed = true;
            closeReader(reader);
        }
    }

    private static void closeReader(RangeReader reader) {
        try {
            reader.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close RangeReader " + reader.getSourceIdentifier(), e);
        }
    }
}
