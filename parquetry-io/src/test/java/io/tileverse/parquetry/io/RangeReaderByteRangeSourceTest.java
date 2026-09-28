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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.OptionalLong;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.storage.NotFoundException;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.StorageFactory;

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
}
