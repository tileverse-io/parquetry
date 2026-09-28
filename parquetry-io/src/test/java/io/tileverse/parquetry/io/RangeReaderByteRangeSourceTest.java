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

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
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

    /** A reader over an empty object that reports a configurable size and records its close. */
    private static final class RecordingRangeReader implements RangeReader {

        private final OptionalLong size;
        private boolean closed;

        RecordingRangeReader(OptionalLong size) {
            this.size = size;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public OptionalLong size() {
            return size;
        }

        @Override
        public int readRange(long offset, int length, ByteBuffer target) {
            return 0;
        }

        @Override
        public String getSourceIdentifier() {
            return "recording";
        }
    }
}
