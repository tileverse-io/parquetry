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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeRequest;

import io.tileverse.io.ByteRange;

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
                        + " with " + (length - done) + " bytes still requested in "
                        + sourceIdentifier().orElse("an unnamed source")));
            }
            done += read;
        }
    }

    /**
     * Reads every request's range into its target, each filled to its full length, and reports what the call cost. The
     * batch form of {@link #readFully(long, MemorySegment)}, for a plan whose ranges are all known before the first
     * byte is asked for.
     *
     * <p>Targets must be distinct buffers or non-overlapping windows on one buffer. Each is written from its current
     * position and left with its position advanced by its range length. An implementation may write a target in any
     * order and from a thread other than the caller's, which rules out a target backed by a segment allocated from
     * {@link java.lang.foreign.Arena#ofConfined()}. Once the call returns or throws, no thread of the implementation is
     * still writing to any target: the caller may reuse or release every target at that point, after a failure
     * included.
     *
     * <p>Every count in the returned result equals its request's length, because a short read fails the call instead.
     * The transport numbers beside those counts describe what serving the call cost below this source. A caller reads
     * them to weigh bytes moved against bytes asked for.
     *
     * <p>The default reads the ranges in file order, joining only the ranges that touch byte for byte over one
     * contiguous target, and never asks for a byte that no request named. It reports one fetch per read that it issued,
     * and nothing served from a cache. An implementation overrides it when its backend serves several ranges in one
     * round trip.
     *
     * @param requests the ranges to read and their landing buffers
     * @return the bytes landed per request, and what the call cost
     * @throws IllegalArgumentException if a request is null or its target no longer holds its range
     * @throws UncheckedIOException wrapping an {@link EOFException} when a range runs past end of source
     */
    default BatchReadResult readFully(List<RangeRequest> requests) {
        RangeRequest.validate(requests);
        long readsIssued = 0;
        long bytesTransferred = 0;
        for (List<RangeRequest> run : runsOfTouchingRanges(requests)) {
            int runLength = totalLength(run);
            if (runLength == 0) {
                continue;
            }
            readRunAsOneRead(run, runLength);
            readsIssued++;
            bytesTransferred += runLength;
        }
        return BatchReadResult.of(requests, requestedLengths(requests), readsIssued, bytesTransferred, 0);
    }

    /**
     * The requests in ascending offset order, grouped into runs, each run served by one read. Request order would
     * otherwise turn a row group into preads that jump between columns, the access pattern served worst by a network
     * filesystem.
     */
    private static List<List<RangeRequest>> runsOfTouchingRanges(List<RangeRequest> requests) {
        List<RangeRequest> ordered = new ArrayList<>(requests);
        ordered.sort(Comparator.comparingLong(request -> request.range().offset()));
        List<List<RangeRequest>> runs = new ArrayList<>();
        List<RangeRequest> current = null;
        for (RangeRequest request : ordered) {
            boolean joins = current != null && joinsRun(current, request);
            if (!joins) {
                current = new ArrayList<>();
                runs.add(current);
            }
            current.add(request);
        }
        return runs;
    }

    /**
     * Whether {@code next} extends {@code run} with no hole: its range starts where the run's last range ends, its
     * target starts where that target's range ends, and the run's first target has the capacity to span them all.
     */
    private static boolean joinsRun(List<RangeRequest> run, RangeRequest next) {
        RangeRequest last = run.getLast();
        ByteRange lastRange = last.range();
        ByteRange nextRange = next.range();
        if (lastRange.end() != nextRange.offset()) {
            return false;
        }
        ByteBuffer runTarget = run.getFirst().target();
        long joinedLength = (long) totalLength(run) + nextRange.length();
        if (joinedLength > runTarget.capacity() - runTarget.position()) {
            return false;
        }
        return adjacentInMemory(last, next);
    }

    /** Whether the right target begins exactly where the left target's range ends, in one region of memory. */
    private static boolean adjacentInMemory(RangeRequest left, RangeRequest right) {
        MemorySegment leftBytes = MemorySegment.ofBuffer(left.target());
        MemorySegment rightBytes = MemorySegment.ofBuffer(right.target());
        if (!shareOneRegion(leftBytes, rightBytes)) {
            return false;
        }
        return leftBytes.address() + left.range().length() == rightBytes.address();
    }

    /**
     * Whether two windows sit in the same array, or under the same arena. Two native segments of one arena need not
     * come from one allocation; {@link #joinsRun} keeps a joined write inside the room left in the run's first target
     * by checking that target's capacity.
     */
    private static boolean shareOneRegion(MemorySegment left, MemorySegment right) {
        Object leftBase = left.heapBase().orElse(null);
        Object rightBase = right.heapBase().orElse(null);
        if (leftBase != null || rightBase != null) {
            return leftBase == rightBase;
        }
        return left.scope().equals(right.scope());
    }

    private void readRunAsOneRead(List<RangeRequest> run, int runLength) {
        RangeRequest first = run.getFirst();
        ByteBuffer span = first.target().duplicate();
        span.limit(span.position() + runLength);
        readFully(first.range().offset(), MemorySegment.ofBuffer(span));
        advancePositions(run);
    }

    private static void advancePositions(List<RangeRequest> run) {
        for (RangeRequest request : run) {
            ByteBuffer target = request.target();
            target.position(target.position() + request.range().length());
        }
    }

    private static int totalLength(List<RangeRequest> run) {
        int total = 0;
        for (RangeRequest request : run) {
            total += request.range().length();
        }
        return total;
    }

    /** Each request's own length, equal to the bytes landed for it because every request is filled to completion. */
    private static int[] requestedLengths(List<RangeRequest> requests) {
        int[] lengths = new int[requests.size()];
        for (int i = 0; i < requests.size(); i++) {
            lengths[i] = requests.get(i).range().length();
        }
        return lengths;
    }

    /** Narrowed: no checked exception (matches parquetry's RuntimeException-only public API). */
    @Override
    void close();

    /**
     * Opens {@code path} through tileverse-storage's local file reader and OWNS it; {@link #close()} releases the file.
     * The channel opens on the first read and stays open until close, and a read failing on a stale NFS handle or a
     * closed channel is retried on a fresh channel. The source is named by the file's real path, symbolic links
     * resolved.
     *
     * @throws UncheckedIOException over a {@link java.nio.file.NoSuchFileException} when the file does not exist, or
     *     over the failure raised while inspecting it; a file unreadable by the caller opens and fails on its first
     *     read
     * @throws IllegalArgumentException if {@code path} is a directory
     */
    static ByteRangeSource ofFile(Path path) {
        return ofFile(path, Duration.ZERO);
    }

    /**
     * As {@link #ofFile(Path)}, releasing the file once it sits unread for {@code idleTimeout} and reopening it, by its
     * real path, on the next read; {@link Duration#ZERO} holds the file until {@link #close()}. The reopen finds
     * whatever file sits at that path by then, while metadata cached under the source's name still describes the
     * previous file.
     *
     * @throws IllegalArgumentException if {@code idleTimeout} is negative, or above zero and below one millisecond
     */
    static ByteRangeSource ofFile(Path path, Duration idleTimeout) {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(idleTimeout, "idleTimeout");
        return RangeReaderByteRangeSource.openFile(path, idleTimeout);
    }

    /**
     * Wraps an existing {@link FileChannel}; BORROWS it - {@link #close()} does not close the channel. The channel must
     * stay open for the lifetime of the returned source.
     *
     * @throws UncheckedIOException if the channel size cannot be determined
     */
    static ByteRangeSource ofChannel(FileChannel channel) {
        return new BorrowedChannelByteRangeSource(Objects.requireNonNull(channel, "channel"));
    }

    /**
     * Reads through a tileverse-storage {@link RangeReader} and BORROWS it - {@link #close()} does not close the
     * reader, which the caller closes after the last read. The fit for a reader shared and cached across reads.
     *
     * @throws IllegalStateException if the reader cannot report its size
     * @throws UncheckedIOException if asking the reader for its size fails
     */
    static ByteRangeSource of(RangeReader reader) {
        return new RangeReaderByteRangeSource(Objects.requireNonNull(reader, "reader"), false);
    }

    /**
     * Reads through a tileverse-storage {@link RangeReader} and OWNS it - {@link #close()} closes the reader. The fit
     * for a reader handed out fresh by a {@code Storage} for this source alone.
     *
     * @throws IllegalStateException if the reader cannot report its size; the reader is closed before the throw
     * @throws UncheckedIOException if asking the reader for its size fails; the reader is closed before the throw
     */
    static ByteRangeSource owning(RangeReader reader) {
        return new RangeReaderByteRangeSource(Objects.requireNonNull(reader, "reader"), true);
    }
}
