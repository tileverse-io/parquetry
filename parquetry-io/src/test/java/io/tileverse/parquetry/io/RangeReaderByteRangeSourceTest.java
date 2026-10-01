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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.RangeRequest;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.StorageFactory;

import io.tileverse.io.ByteRange;

class RangeReaderByteRangeSourceTest {

    private static final byte[] CONTENT = "tileverse-storage adapter parity".getBytes(StandardCharsets.US_ASCII);

    @Test
    void readsMatchAByteRangeSourceOverTheSameFile(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("data.bin");
        Files.write(file, CONTENT);
        try (Storage storage = StorageFactory.open(dir.toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString());
                ByteRangeSource viaReader = ByteRangeSource.of(reader);
                ByteRangeSource viaFile = ByteRangeSource.ofFile(file)) {

            assertThat(viaReader.size()).isEqualTo(viaFile.size());

            byte[] a = new byte[10];
            byte[] b = new byte[10];
            viaReader.readFully(7, MemorySegment.ofArray(a));
            viaFile.readFully(7, MemorySegment.ofArray(b));
            assertThat(a).isEqualTo(b);
        }
    }

    @Test
    void readAtOrAfterEndReturnsMinusOne(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("data.bin");
        Files.write(file, CONTENT);
        try (Storage storage = StorageFactory.open(dir.toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString());
                ByteRangeSource source = ByteRangeSource.of(reader)) {
            assertThat(source.read(CONTENT.length, MemorySegment.ofArray(new byte[4])))
                    .isEqualTo(-1);
        }
    }

    @Test
    void inBoundsReadIsFullySatisfiedAndTailIsShort(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("data.bin");
        Files.write(file, CONTENT);
        try (Storage storage = StorageFactory.open(dir.toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString());
                ByteRangeSource source = ByteRangeSource.of(reader)) {

            // An in-bounds request must deliver every requested byte.
            byte[] inBounds = new byte[8];
            assertThat(source.read(4, MemorySegment.ofArray(inBounds))).isEqualTo(8);

            // A request whose end runs past the source returns only the available tail.
            byte[] tail = new byte[10];
            assertThat(source.read(CONTENT.length - 4, MemorySegment.ofArray(tail)))
                    .isEqualTo(4);
        }
    }

    @Test
    void closeDoesNotCloseTheBorrowedReader(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("data.bin");
        Files.write(file, CONTENT);
        try (Storage storage = StorageFactory.open(dir.toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ByteRangeSource source = ByteRangeSource.of(reader);
            source.close();
            assertThat(reader.size()).isPresent();
        }
    }

    @Test
    void closeClosesTheOwnedReader() {
        RecordingRangeReader reader = new RecordingRangeReader(OptionalLong.of(0));

        ByteRangeSource.owning(reader).close();

        assertThat(reader.closed).isTrue();
    }

    @Test
    void owningSourceForwardsTheReaderIdentity() {
        RecordingRangeReader reader = new RecordingRangeReader(OptionalLong.of(0));

        assertThat(ByteRangeSource.owning(reader).sourceIdentifier()).contains("recording");
    }

    @Test
    void anOwnedReaderWithoutASizeIsClosedBeforeTheFailureIsReported() {
        RecordingRangeReader reader = new RecordingRangeReader(OptionalLong.empty());

        assertThatThrownBy(() -> ByteRangeSource.owning(reader)).isInstanceOf(IllegalStateException.class);

        assertThat(reader.closed).isTrue();
    }

    @Test
    void aBorrowedReaderWithoutASizeStaysOpenWhenTheFailureIsReported() {
        RecordingRangeReader reader = new RecordingRangeReader(OptionalLong.empty());

        assertThatThrownBy(() -> ByteRangeSource.of(reader)).isInstanceOf(IllegalStateException.class);

        assertThat(reader.closed).isFalse();
    }

    /**
     * A storage failure reaches the caller as an unchecked I/O exception over the reader's own I/O cause when it has
     * one, over a missing-file exception for a missing object, and over a plain I/O exception otherwise.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("readFailures")
    void aStorageFailureIsReportedAsAnIoFailure(
            String description,
            RuntimeException failure,
            Class<? extends IOException> expectedCause,
            Throwable expectedRoot) {
        RecordingRangeReader reader = new RecordingRangeReader(OptionalLong.of(8), failure);
        ByteRangeSource source = ByteRangeSource.of(reader);
        MemorySegment dst = MemorySegment.ofArray(new byte[4]);

        assertThatThrownBy(() -> source.read(0, dst))
                .isInstanceOf(UncheckedIOException.class)
                .cause()
                .isInstanceOf(expectedCause)
                .hasCause(expectedRoot);
    }

    private static Stream<Arguments> readFailures() {
        IOException ioCause = new IOException("disk gone", new RuntimeException("errno 5"));
        StorageException withIoCause = new StorageException("read", ioCause);
        StorageException withoutCause = new StorageException("backend refused");
        NotFoundException missing = new NotFoundException("no such object");
        IllegalStateException closedByOwner = new IllegalStateException("reader is closed");
        return Stream.of(
                Arguments.of("storage failure with an I/O cause", withIoCause, IOException.class, ioCause.getCause()),
                Arguments.of("storage failure without a cause", withoutCause, IOException.class, withoutCause),
                Arguments.of("missing object", missing, NoSuchFileException.class, missing),
                Arguments.of("reader closed by its owner", closedByOwner, ClosedChannelException.class, closedByOwner));
    }

    /** With the reader's own I/O cause promoted, the storage failure and its message stay reachable. */
    @Test
    void theStorageFailureStaysInTheChain() {
        StorageException failure = new StorageException("Read failed after 3 attempts", new IOException("stale"));
        RecordingRangeReader reader = new RecordingRangeReader(OptionalLong.of(8), failure);
        ByteRangeSource source = ByteRangeSource.of(reader);
        MemorySegment dst = MemorySegment.ofArray(new byte[4]);

        assertThatThrownBy(() -> source.read(0, dst))
                .hasCause(failure.getCause())
                .hasSuppressedException(failure);
    }

    @Test
    void aClosedOwnedSourceStillAnswersEmptyAndPastEndReads() {
        RecordingRangeReader reader = new RecordingRangeReader(OptionalLong.of(8));
        ByteRangeSource source = ByteRangeSource.owning(reader);
        source.close();

        assertThat(source.read(0, MemorySegment.ofArray(new byte[0]))).isZero();
        assertThat(source.read(8, MemorySegment.ofArray(new byte[4]))).isEqualTo(-1);
    }

    @Test
    void aFailureToCloseAfterAFailedSizeLookupIsSuppressed() {
        StorageException sizeFailure = new StorageException("size", new IOException("gone"));
        IOException closeFailure = new IOException("close refused");
        RecordingRangeReader reader = RecordingRangeReader.failingToSize(sizeFailure, closeFailure);

        assertThatThrownBy(() -> ByteRangeSource.owning(reader))
                .isInstanceOf(UncheckedIOException.class)
                .hasCause(sizeFailure.getCause())
                .hasSuppressedException(closeFailure);
        assertThat(reader.closed).isTrue();
    }

    @Test
    void aReadOnlyDestinationIsRejectedBeforeTheReaderIsAsked() {
        RecordingRangeReader reader = new RecordingRangeReader(OptionalLong.of(8));
        ByteRangeSource source = ByteRangeSource.of(reader);
        MemorySegment readOnly = MemorySegment.ofArray(new byte[4]).asReadOnly();

        assertThatThrownBy(() -> source.read(0, readOnly)).isInstanceOf(IllegalArgumentException.class);
        assertThat(reader.reads).isZero();
    }

    @Test
    void aReadAfterClosingAnOwnedReaderFailsAsAClosedChannel() {
        RecordingRangeReader reader = new RecordingRangeReader(OptionalLong.of(8));
        ByteRangeSource source = ByteRangeSource.owning(reader);
        source.close();
        MemorySegment dst = MemorySegment.ofArray(new byte[4]);

        assertThatThrownBy(() -> source.read(0, dst))
                .isInstanceOf(UncheckedIOException.class)
                .cause()
                .isInstanceOf(ClosedChannelException.class);
    }

    @Test
    void aMissingObjectFailsTheOpenAsAMissingFileAndClosesTheOwnedReader() {
        NotFoundException failure = new NotFoundException("no such object");
        RecordingRangeReader reader = RecordingRangeReader.failingToSize(failure);

        assertThatThrownBy(() -> ByteRangeSource.owning(reader))
                .isInstanceOf(UncheckedIOException.class)
                .cause()
                .isInstanceOf(NoSuchFileException.class)
                .hasCause(failure);
        assertThat(reader.closed).isTrue();
    }

    @Test
    void aStorageFailureWhileSizingLeavesABorrowedReaderOpen() {
        StorageException failure = new StorageException("size", new IOException("gone"));
        RecordingRangeReader reader = RecordingRangeReader.failingToSize(failure);

        assertThatThrownBy(() -> ByteRangeSource.of(reader))
                .isInstanceOf(UncheckedIOException.class)
                .hasCause(failure.getCause());
        assertThat(reader.closed).isFalse();
    }

    /** The caller owns a borrowed reader's lifetime; closing the source over it changes nothing. */
    @Test
    void aReadAfterClosingABorrowingSourceStillReads() {
        RecordingRangeReader reader = new RecordingRangeReader(OptionalLong.of(8));
        ByteRangeSource source = ByteRangeSource.of(reader);
        source.close();
        byte[] dst = new byte[4];

        assertThat(source.read(2, MemorySegment.ofArray(dst))).isEqualTo(4);
        assertThat(dst).containsExactly(2, 3, 4, 5);
    }

    /**
     * A reader over {@code size} bytes whose i-th byte is {@code i}, recording reads and closes. Every read throws
     * {@code readFailure}, every size lookup throws {@code sizeFailure}, and closing throws {@code closeFailure}, each
     * when one is given.
     */
    @Test
    void oneBatchReachesTheReaderAsOneCall() {
        OtherThreadRangeReader reader = new OtherThreadRangeReader(CONTENT);
        try (ByteRangeSource source = ByteRangeSource.of(reader)) {
            ByteBuffer first = ByteBuffer.allocate(6);
            ByteBuffer second = ByteBuffer.allocate(5);

            source.readFully(List.of(RangeRequest.of(0, 6, first), RangeRequest.of(20, 5, second)));

            assertThat(reader.batchCalls).hasValue(1);
            assertThat(reader.batchedRanges).hasValue(2);
            assertThat(first.array()).isEqualTo(Arrays.copyOfRange(CONTENT, 0, 6));
            assertThat(second.array()).isEqualTo(Arrays.copyOfRange(CONTENT, 20, 25));
        }
    }

    @Test
    void theReadersOwnAccountingOfTheCallReachesTheCaller() {
        OtherThreadRangeReader reader = new OtherThreadRangeReader(CONTENT);
        try (ByteRangeSource source = ByteRangeSource.of(reader)) {
            List<RangeRequest> requests = List.of(
                    RangeRequest.of(0, 6, ByteBuffer.allocate(6)), RangeRequest.of(20, 5, ByteBuffer.allocate(5)));

            BatchReadResult result = source.readFully(requests);

            assertThat(result.requests()).isEqualTo(2);
            assertThat(result.bytesRead(0)).isEqualTo(6);
            assertThat(result.bytesRead(1)).isEqualTo(5);
            assertThat(result.bytesRequested()).isEqualTo(11);
            assertThat(result.fetches())
                    .as("the backend served the batch in one round trip")
                    .isEqualTo(1);
            assertThat(result.bytesTransferred()).isEqualTo(11);
            assertThat(result.bytesFromCache()).isZero();
        }
    }

    @Test
    void aRangePastEndOfSourceFailsTheBatch() {
        OtherThreadRangeReader reader = new OtherThreadRangeReader(CONTENT);
        try (ByteRangeSource source = ByteRangeSource.of(reader)) {
            List<RangeRequest> requests = List.of(RangeRequest.of(CONTENT.length - 4L, 10, ByteBuffer.allocate(10)));

            assertThatThrownBy(() -> source.readFully(requests))
                    .isInstanceOf(UncheckedIOException.class)
                    .hasCauseInstanceOf(EOFException.class)
                    .hasMessageContaining("offset " + (CONTENT.length - 4))
                    .hasMessageContaining("6 bytes still requested")
                    .hasMessageContaining("fake://content");
        }
    }

    @Test
    void aSharedArenaTargetTakesBytesWrittenByAnotherThread() {
        OtherThreadRangeReader reader = new OtherThreadRangeReader(CONTENT);
        try (Arena arena = Arena.ofShared();
                ByteRangeSource source = ByteRangeSource.of(reader)) {
            MemorySegment landing = arena.allocate(8);

            source.readFully(List.of(RangeRequest.of(0, 8, landing.asByteBuffer())));

            assertThat(landing.toArray(ValueLayout.JAVA_BYTE)).isEqualTo(Arrays.copyOfRange(CONTENT, 0, 8));
        }
    }

    @Test
    void aConfinedArenaTargetIsRefusedByTheWritingThread() {
        OtherThreadRangeReader reader = new OtherThreadRangeReader(CONTENT);
        try (Arena arena = Arena.ofConfined();
                ByteRangeSource source = ByteRangeSource.of(reader)) {
            MemorySegment landing = arena.allocate(8);
            List<RangeRequest> requests = List.of(RangeRequest.of(0, 8, landing.asByteBuffer()));

            assertThatThrownBy(() -> source.readFully(requests)).isInstanceOf(WrongThreadException.class);
        }
    }

    @Test
    void aTargetReleasedRightAfterTheCallIsNeverWrittenIntoAgain() throws InterruptedException {
        LateWritingRangeReader breaksThePromise = new LateWritingRangeReader(CONTENT);
        try (Arena arena = Arena.ofShared();
                ByteRangeSource source = ByteRangeSource.of(breaksThePromise)) {
            MemorySegment landing = arena.allocate(8);
            source.readFully(List.of(RangeRequest.of(0, 8, landing.asByteBuffer())));
        }

        Optional<Throwable> lateWrite = breaksThePromise.releaseAndAwaitTheLateWrite();

        assertThat(lateWrite)
                .as("a write after the release is refused, never landed in a recycled region")
                .containsInstanceOf(IllegalStateException.class);
    }

    @Test
    void aTargetIsSafeToReuseAfterAFailedBatch() {
        FailingAfterWriteRangeReader reader = new FailingAfterWriteRangeReader(CONTENT);
        try (Arena arena = Arena.ofShared();
                ByteRangeSource source = ByteRangeSource.of(reader)) {
            MemorySegment landing = arena.allocate(8);
            List<RangeRequest> doomed = List.of(RangeRequest.of(0, 8, landing.asByteBuffer()));

            assertThatThrownBy(() -> source.readFully(doomed)).isInstanceOf(StorageException.class);

            reader.stopFailing();
            source.readFully(List.of(RangeRequest.of(8, 8, landing.asByteBuffer())));

            assertThat(landing.toArray(ValueLayout.JAVA_BYTE)).isEqualTo(Arrays.copyOfRange(CONTENT, 8, 16));
        }
    }

    /** A reader over an empty object that reports a configurable size and records its close. */
    private static final class RecordingRangeReader implements RangeReader {

        private final OptionalLong size;
        private final RuntimeException readFailure;
        private final RuntimeException sizeFailure;
        private final IOException closeFailure;
        private int reads;
        private boolean closed;

        RecordingRangeReader(OptionalLong size) {
            this(size, null);
        }

        RecordingRangeReader(OptionalLong size, RuntimeException readFailure) {
            this(size, readFailure, null, null);
        }

        private RecordingRangeReader(
                OptionalLong size,
                RuntimeException readFailure,
                RuntimeException sizeFailure,
                IOException closeFailure) {
            this.size = size;
            this.readFailure = readFailure;
            this.sizeFailure = sizeFailure;
            this.closeFailure = closeFailure;
        }

        static RecordingRangeReader failingToSize(RuntimeException sizeFailure) {
            return failingToSize(sizeFailure, null);
        }

        static RecordingRangeReader failingToSize(RuntimeException sizeFailure, IOException closeFailure) {
            return new RecordingRangeReader(OptionalLong.empty(), null, sizeFailure, closeFailure);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public OptionalLong size() {
            if (sizeFailure != null) {
                throw sizeFailure;
            }
            return size;
        }

        @Override
        public int readRange(long offset, int length, ByteBuffer target) {
            reads++;
            if (readFailure != null) {
                throw readFailure;
            }
            long available = size.orElse(0) - offset;
            int count = (int) Math.max(0, Math.min(length, available));
            for (int i = 0; i < count; i++) {
                target.put((byte) (offset + i));
            }
            return count;
        }

        @Override
        public String getSourceIdentifier() {
            return "recording";
        }
    }

    /** Runs {@code work} on a thread other than the caller's and waits for it, as a streaming backend's I/O thread. */
    private static <T> T onAnotherThread(Callable<T> work) {
        try (ExecutorService worker = Executors.newSingleThreadExecutor()) {
            return worker.submit(work).get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException(cause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    /** A reader over an in-memory object, serving one range at a time and reporting the object's length. */
    private abstract static class ContentRangeReader implements RangeReader {

        private final byte[] content;

        ContentRangeReader(byte[] content) {
            this.content = content;
        }

        @Override
        public int readRange(long offset, int length, ByteBuffer target) {
            if (offset >= content.length) {
                return 0;
            }
            int available = (int) Math.min(length, content.length - offset);
            target.put(content, (int) offset, available);
            return available;
        }

        /** Fills every target in request order, and reports the whole batch as one backend request. */
        BatchReadResult readEachRange(List<RangeRequest> requests) {
            RangeRequest.validate(requests);
            int[] counts = new int[requests.size()];
            long transferred = 0;
            for (int i = 0; i < requests.size(); i++) {
                RangeRequest request = requests.get(i);
                ByteRange range = request.range();
                counts[i] = readRange(range.offset(), range.length(), request.target());
                transferred += counts[i];
            }
            return BatchReadResult.of(requests, counts, 1, transferred, 0);
        }

        @Override
        public OptionalLong size() {
            return OptionalLong.of(content.length);
        }

        @Override
        public String getSourceIdentifier() {
            return "fake://content";
        }

        @Override
        public void close() {
            // holds no resource
        }
    }

    /**
     * A reader filling every batch target from a thread other than the caller's, as allowed by the {@code RangeReader}
     * contract and done by a streaming backend. It waits for that thread before returning, as the contract requires,
     * and counts the calls handed to it and the ranges that they asked for.
     */
    private static final class OtherThreadRangeReader extends ContentRangeReader {

        private final AtomicInteger batchCalls = new AtomicInteger();
        private final AtomicInteger batchedRanges = new AtomicInteger();

        OtherThreadRangeReader(byte[] content) {
            super(content);
        }

        @Override
        public BatchReadResult readRanges(List<RangeRequest> requests) {
            batchCalls.incrementAndGet();
            batchedRanges.addAndGet(requests.size());
            return onAnotherThread(() -> readEachRange(requests));
        }
    }

    /**
     * A reader that breaks the upstream promise on purpose: it reports every byte as read and returns while a worker is
     * still about to write it. The negative control that gives the release case below something to catch.
     */
    private static final class LateWritingRangeReader extends ContentRangeReader {

        private final CountDownLatch released = new CountDownLatch(1);
        private final CountDownLatch finished = new CountDownLatch(1);
        private final AtomicReference<Throwable> lateWriteFailure = new AtomicReference<>();

        LateWritingRangeReader(byte[] content) {
            super(content);
        }

        @Override
        public BatchReadResult readRanges(List<RangeRequest> requests) {
            Thread.ofVirtual().start(() -> writeWhenReleased(requests));
            return BatchReadResult.perRange(requests, requestedLengths(requests));
        }

        private void writeWhenReleased(List<RangeRequest> requests) {
            try {
                released.await();
                readEachRange(requests);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                lateWriteFailure.set(e);
            } catch (RuntimeException e) {
                lateWriteFailure.set(e);
            } finally {
                finished.countDown();
            }
        }

        /** Lets the held-back write run, and reports how it ended. */
        Optional<Throwable> releaseAndAwaitTheLateWrite() throws InterruptedException {
            released.countDown();
            if (!finished.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("the held-back write never finished");
            }
            return Optional.ofNullable(lateWriteFailure.get());
        }

        private static int[] requestedLengths(List<RangeRequest> requests) {
            int[] lengths = new int[requests.size()];
            for (int i = 0; i < requests.size(); i++) {
                lengths[i] = requests.get(i).range().length();
            }
            return lengths;
        }
    }

    /**
     * A reader whose batch call writes on another thread, waits for that thread, and only then fails. A backend fails
     * the same way: the promise that no thread is still writing holds after a failure too.
     */
    private static final class FailingAfterWriteRangeReader extends ContentRangeReader {

        private volatile boolean failing = true;

        FailingAfterWriteRangeReader(byte[] content) {
            super(content);
        }

        @Override
        public BatchReadResult readRanges(List<RangeRequest> requests) {
            BatchReadResult result = onAnotherThread(() -> readEachRange(requests));
            if (failing) {
                throw new StorageException("the object went away mid-batch");
            }
            return result;
        }

        void stopFailing() {
            failing = false;
        }
    }
}
