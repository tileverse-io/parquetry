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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.CopyOptions;
import io.tileverse.storage.DeleteResult;
import io.tileverse.storage.ListOptions;
import io.tileverse.storage.PresignWriteOptions;
import io.tileverse.storage.RangeReader;
import io.tileverse.storage.ReadHandle;
import io.tileverse.storage.ReadOptions;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageCapabilities;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.WriteOptions;

import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * Proves the logical-to-physical key mapping (the table location string is deliberately NOT the physical storage root),
 * outside-root rejection, the list happy path, and the borrow-versus-own close lifecycle.
 */
class StorageIcebergFileIOTest {

    private static final String LOGICAL_ROOT = "file:///logical/tbl";

    @Test
    void openMapsLogicalLocationToPhysicalKey(@TempDir Path tempDir) throws Exception {
        byte[] payload = "hello iceberg".getBytes(StandardCharsets.UTF_8);
        Path dataDir = Files.createDirectories(tempDir.resolve("data"));
        Files.write(dataDir.resolve("part.txt"), payload);

        Storage storage = StorageFactory.open(tempDir.toUri());
        StorageIcebergFileIO io = StorageIcebergFileIO.over(storage, LOGICAL_ROOT);

        try (ByteRangeSource source = io.open(LOGICAL_ROOT + "/data/part.txt")) {
            assertThat(source.size()).isEqualTo(payload.length);
        }
    }

    @Test
    void openRejectsLocationOutsideTheTableRoot(@TempDir Path tempDir) {
        Storage storage = StorageFactory.open(tempDir.toUri());
        StorageIcebergFileIO io = StorageIcebergFileIO.over(storage, LOGICAL_ROOT);

        assertThatThrownBy(() -> io.open("file:///other/x")).isInstanceOf(IcebergFormatException.class);
    }

    @Test
    void listReturnsRoundTrippingLogicalUris(@TempDir Path tempDir) throws Exception {
        Path metaDir = Files.createDirectories(tempDir.resolve("meta"));
        Files.write(metaDir.resolve("a.json"), "aa".getBytes(StandardCharsets.UTF_8));
        Files.write(metaDir.resolve("b.json"), "bbbb".getBytes(StandardCharsets.UTF_8));

        Storage storage = StorageFactory.open(tempDir.toUri());
        StorageIcebergFileIO io = StorageIcebergFileIO.over(storage, LOGICAL_ROOT);

        List<String> entries = io.list(LOGICAL_ROOT + "/meta/");

        assertThat(entries).containsExactly(LOGICAL_ROOT + "/meta/a.json", LOGICAL_ROOT + "/meta/b.json");

        try (ByteRangeSource a = io.open(entries.get(0))) {
            assertThat(a.size()).isEqualTo(2);
        }
        try (ByteRangeSource b = io.open(entries.get(1))) {
            assertThat(b.size()).isEqualTo(4);
        }
    }

    @Test
    void borrowedStorageIsNotClosed() {
        RecordingStorage recording = new RecordingStorage();

        StorageIcebergFileIO.over(recording, "file:///r").close();

        assertThat(recording.closed).isFalse();
    }

    @Test
    void ownedStorageIsClosed() {
        RecordingStorage recording = new RecordingStorage();

        StorageIcebergFileIO.owning(recording, "file:///r").close();

        assertThat(recording.closed).isTrue();
    }

    @Test
    void openSourceClosesItsRangeReader() {
        RecordingRangeReader reader = new RecordingRangeReader();
        RecordingStorage recording = new RecordingStorage(reader);

        StorageIcebergFileIO.over(recording, "file:///r").open("file:///r/x").close();

        assertThat(reader.closed).isTrue();
    }

    /**
     * A Storage whose only meaningful behaviors are recording an idempotent {@link #close()} and serving one reader.
     */
    private static final class RecordingStorage implements Storage {

        private final RecordingRangeReader reader;
        private boolean closed;

        RecordingStorage() {
            this(null);
        }

        RecordingStorage(RecordingRangeReader reader) {
            this.reader = reader;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public URI baseUri() {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageCapabilities capabilities() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<StorageEntry.File> stat(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Stream<StorageEntry> list(String pattern, ListOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RangeReader openRangeReader(String key) {
            if (reader == null) {
                throw new UnsupportedOperationException();
            }
            return reader;
        }

        @Override
        public ReadHandle read(String key, ReadOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageEntry.File put(String key, byte[] data, WriteOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageEntry.File put(String key, Path source, WriteOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputStream openOutputStream(String key, WriteOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DeleteResult deleteAll(Collection<String> keys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageEntry.File copy(String srcKey, String dstKey, CopyOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageEntry.File copy(String srcKey, Storage dst, String dstKey, CopyOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageEntry.File move(String srcKey, String dstKey, CopyOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI presignGet(String key, Duration ttl) {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI presignPut(String key, Duration ttl, PresignWriteOptions options) {
            throw new UnsupportedOperationException();
        }
    }

    /** A reader that reports an empty blob and records an idempotent {@link #close()}. */
    private static final class RecordingRangeReader implements RangeReader {

        private boolean closed;

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public OptionalLong size() {
            return OptionalLong.of(0);
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
