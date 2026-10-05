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
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.file.FileStorageProvider;

/**
 * What the single {@link FileSource} implementation adds around the listing: who closes the Storage, who closes the
 * readers opened by its entries, and what a missing root reports. The glob contract itself is pinned by
 * {@link FileSourceGlobTest}.
 */
class StorageFileSourceTest {

    @Test
    void aLocalEntryOwnsTheReaderItOpens(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("only.parquet"), "HELLO");

        try (FileSource source = FileSource.directory(dir, "*.parquet")) {
            FileEntry entry = onlyEntryOf(source);
            ByteRangeSource bytes = entry.open();
            assertThat(readAll(bytes)).isEqualTo("HELLO");

            bytes.close();

            assertThatExceptionOfType(UncheckedIOException.class)
                    .isThrownBy(() -> readAll(bytes))
                    .withCauseInstanceOf(ClosedChannelException.class);
        }
    }

    @Test
    void anEntryOverACallerStorageBorrowsTheReaderItOpens(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("only.parquet"), "HELLO");

        try (Storage storage = openStorage(dir);
                FileSource source = FileSource.over(storage, "*.parquet")) {
            FileEntry entry = onlyEntryOf(source);
            ByteRangeSource bytes = entry.open();

            bytes.close();

            assertThat(readAll(bytes)).isEqualTo("HELLO");
        }
    }

    @Test
    void listsTheFilesOfACallerStorageMatchingAPattern(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("a.parquet"), "AAAA");
        Files.writeString(dir.resolve("b.parquet"), "BBBBBB");
        Files.writeString(dir.resolve("note.txt"), "x");

        try (Storage storage = openStorage(dir);
                FileSource source = FileSource.over(storage, "*.parquet")) {
            assertThat(relativePathsOf(source)).containsExactly("a.parquet", "b.parquet");
        }
    }

    @Test
    void closingALocalSourceClosesTheStorageItOpened(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("only.parquet"), "HELLO");
        FileSource source = FileSource.directory(dir, "*.parquet");
        FileEntry entry = onlyEntryOf(source);

        source.close();

        assertThatIllegalStateException().isThrownBy(entry::open);
    }

    @Test
    void closingASourceOverACallerStorageLeavesItOpen(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("only.parquet"), "HELLO");

        try (Storage storage = openStorage(dir)) {
            FileSource source = FileSource.over(storage, "*.parquet");
            FileEntry entry = onlyEntryOf(source);

            source.close();

            try (ByteRangeSource bytes = entry.open()) {
                assertThat(readAll(bytes)).isEqualTo("HELLO");
            }
        }
    }

    @Test
    void closingASourceOverAStorageOpenedForARemoteUriClosesIt(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("only.parquet"), "HELLO");
        Storage storage = openStorage(dir);
        FileSource source = StorageFileSource.overOpened(storage, "*.parquet");
        assertThat(relativePathsOf(source)).containsExactly("only.parquet");

        source.close();

        assertThatIllegalStateException()
                .isThrownBy(() -> storage.list("*.parquet").toList());
    }

    @Test
    void anEntryOfASourceOverAStorageOpenedForARemoteUriBorrowsItsReader(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("only.parquet"), "HELLO");
        Storage storage = openStorage(dir);

        try (FileSource source = StorageFileSource.overOpened(storage, "*.parquet")) {
            FileEntry entry = onlyEntryOf(source);
            ByteRangeSource bytes = entry.open();

            bytes.close();

            assertThat(readAll(bytes)).isEqualTo("HELLO");
        }
    }

    @Test
    void closingASourceOverAnObjectOfAStorageOpenedForARemoteUriClosesIt(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("only.parquet"), "HELLO");
        Storage storage = openStorage(dir);
        FileSource source = StorageFileSource.objectOpened(storage, "only.parquet");
        assertThat(relativePathsOf(source)).containsExactly("only.parquet");

        source.close();

        assertThatIllegalStateException()
                .isThrownBy(() -> storage.list("*.parquet").toList());
    }

    @Test
    void theEntryOfAnObjectOfAStorageOpenedForARemoteUriBorrowsItsReader(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("only.parquet"), "HELLO");
        Storage storage = openStorage(dir);

        try (FileSource source = StorageFileSource.objectOpened(storage, "only.parquet")) {
            FileEntry entry = onlyEntryOf(source);
            ByteRangeSource bytes = entry.open();

            bytes.close();

            assertThat(readAll(bytes)).isEqualTo("HELLO");
        }
    }

    @Test
    void theKeyOfARemoteObjectIsItsLastPathSegmentPercentDecoded() {
        URI object = URI.create("s3://bucket/some/dir/my%20file.parquet");

        assertThat(StorageFileSource.objectKey(object)).isEqualTo("my file.parquet");
    }

    /**
     * The size reported by an entry and the size reported by its byte source both come from the listing. A file grown
     * after the listing therefore reads at its listed length. That is the observable difference between the entry
     * overload of {@code openRangeReader} and the key overload.
     */
    @Test
    void anEntryOpensAtTheSizeReportedByTheListing(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("only.parquet"), "HELLO");

        try (FileSource source = FileSource.directory(dir, "*.parquet")) {
            FileEntry entry = onlyEntryOf(source);
            assertThat(entry.sizeBytes()).isEqualTo(5);

            Files.writeString(dir.resolve("only.parquet"), "HELLO AGAIN");

            try (ByteRangeSource bytes = entry.open()) {
                assertThat(bytes.size()).isEqualTo(5);
            }
        }
    }

    /** A file deleted after the listing reports parquetry's own I/O failure, not a tileverse one. */
    @Test
    void anEntryWhoseFileVanishedFailsAsAMissingFile(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("only.parquet"), "HELLO");

        try (FileSource source = FileSource.directory(dir, "*.parquet")) {
            FileEntry entry = onlyEntryOf(source);
            Files.delete(dir.resolve("only.parquet"));

            assertThatExceptionOfType(UncheckedIOException.class)
                    .isThrownBy(entry::open)
                    .withCauseInstanceOf(NoSuchFileException.class);
        }
    }

    @Test
    void aDirectoryThatDoesNotExistListsNothing(@TempDir Path dir) {
        Path absent = dir.resolve("absent");

        try (FileSource source = FileSource.directory(absent, "*.parquet")) {
            assertThat(relativePathsOf(source)).isEmpty();
            assertThat(source.root()).isEqualTo(absent.toUri());
        }
    }

    @Test
    void aRootThatIsAFileListsNothing(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("only.parquet");
        Files.writeString(file, "HELLO");

        try (FileSource source = FileSource.directory(file, "*.parquet")) {
            assertThat(relativePathsOf(source)).isEmpty();
        }
    }

    @Test
    void aSingleFileUnderADirectoryThatDoesNotExistListsNothing(@TempDir Path dir) {
        try (FileSource source = FileSource.file(dir.resolve("absent/absent.parquet"))) {
            assertThat(relativePathsOf(source)).isEmpty();
        }
    }

    /** The filesystem root has no parent directory to serve as the source root. */
    @Test
    void aSingleFileWithoutAParentDirectoryIsRejected() {
        Path root = Path.of("/");

        assertThatIllegalArgumentException().isThrownBy(() -> FileSource.file(root));
    }

    @Test
    void aLocalStorageIsConfiguredWithoutAnIdleTimeout(@TempDir Path dir) {
        StorageConfig config = StorageFileSource.localStorageConfig(dir);

        assertThat(config.getParameter(FileStorageProvider.FILE_IDLE_TIMEOUT)).contains(Duration.ZERO);
        assertThat(config.baseUri()).isEqualTo(dir.toUri());
    }

    @Test
    void aCallerStorageServesOneObjectByKey(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("only.parquet"), "HELLO");
        Files.writeString(dir.resolve("other.parquet"), "OTHER");

        try (Storage storage = openStorage(dir);
                FileSource source = FileSource.object(storage, "only.parquet")) {
            assertThat(relativePathsOf(source)).containsExactly("only.parquet");
        }
    }

    @Test
    void aCallerStorageServesNothingForAnAbsentKey(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("only.parquet"), "HELLO");

        try (Storage storage = openStorage(dir);
                FileSource source = FileSource.object(storage, "absent.parquet")) {
            assertThat(relativePathsOf(source)).isEmpty();
        }
    }

    /** A link to a missing file is left out of the listing. The listing itself succeeds. */
    @Test
    void aBrokenSymbolicLinkIsLeftOutOfTheListing(@TempDir Path dir) throws IOException {
        Files.writeString(dir.resolve("real.parquet"), "HELLO");
        Files.createSymbolicLink(dir.resolve("broken.parquet"), dir.resolve("gone.parquet"));

        try (FileSource source = FileSource.directory(dir, "*.parquet")) {
            assertThat(relativePathsOf(source)).containsExactly("real.parquet");
        }
    }

    /**
     * A Storage owned by the test. Its readers release their file once idle, because a borrowed reader is never closed
     * by its byte source and an open file blocks the deletion of a {@link TempDir} on Windows.
     */
    private static Storage openStorage(Path directory) {
        StorageConfig config = new StorageConfig(directory.toUri());
        config.setParameter(FileStorageProvider.FILE_IDLE_TIMEOUT, Duration.ofMillis(1));
        return new FileStorageProvider().createStorage(config);
    }

    private static FileEntry onlyEntryOf(FileSource source) {
        try (Stream<FileEntry> entries = source.list()) {
            List<FileEntry> listed = entries.toList();
            assertThat(listed).hasSize(1);
            return listed.get(0);
        }
    }

    private static List<String> relativePathsOf(FileSource source) {
        try (Stream<FileEntry> entries = source.list()) {
            return entries.map(FileEntry::relativePath).sorted().toList();
        }
    }

    private static String readAll(ByteRangeSource bytes) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment dst = arena.allocate(bytes.size());
            bytes.readFully(0, dst);
            return new String(dst.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
        }
    }
}
