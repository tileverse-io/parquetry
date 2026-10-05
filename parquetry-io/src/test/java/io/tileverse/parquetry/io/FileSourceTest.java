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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The local-path contract of {@link FileSource#directory} and {@link FileSource#file}: what each lists, and what an
 * entry opens.
 */
class FileSourceTest {

    @Test
    void directoryListsMatchingFilesWithRelativePaths(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("a.parquet"), "AAAA");
        Files.writeString(dir.resolve("b.parquet"), "BBBBBB");
        Files.writeString(dir.resolve("ignore.txt"), "x");

        FileSource source = FileSource.directory(dir, "*.parquet");
        List<FileEntry> files;
        try (Stream<FileEntry> s = source.list()) {
            files = s.sorted(java.util.Comparator.comparing(FileEntry::relativePath))
                    .toList();
        }

        assertThat(files).extracting(FileEntry::relativePath).containsExactly("a.parquet", "b.parquet");
        assertThat(files).extracting(FileEntry::sizeBytes).containsExactly(4L, 6L);
        assertThat(source.root()).isEqualTo(dir.toUri());
        source.close();
    }

    @Test
    void openReadsFileBytes(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("only.parquet"), "HELLO");
        FileSource source = FileSource.file(dir.resolve("only.parquet"));

        FileEntry file;
        try (Stream<FileEntry> s = source.list()) {
            file = s.toList().get(0);
        }
        assertThat(file.relativePath()).isEqualTo("only.parquet");

        try (ByteRangeSource bytes = file.open();
                Arena arena = Arena.ofConfined()) {
            MemorySegment dst = arena.allocate(5);
            bytes.readFully(0, dst);
            byte[] read = dst.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE);
            assertThat(new String(read, java.nio.charset.StandardCharsets.UTF_8))
                    .isEqualTo("HELLO");
        }
        source.close();
    }

    @Test
    void singleFileNameIsNeverReadAsAGlob(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("f[z-a].parquet");
        Files.writeString(file, "x");

        assertThat(relativePathsOf(FileSource.file(file))).containsExactly("f[z-a].parquet");
    }

    @Test
    void singleFileListingDoesNotWalkItsSiblings(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("only.parquet");
        Files.writeString(file, "x");
        Path locked = Files.createDirectory(dir.resolve("locked"));
        assumeTrue(denyDirectoryReads(locked), "needs POSIX permissions and a non-root user");
        try {
            assertThat(relativePathsOf(FileSource.file(file))).containsExactly("only.parquet");
        } finally {
            restoreDirectoryReads(locked);
        }
    }

    @Test
    void singleFileUnderASymlinkedParentIsListed(@TempDir Path dir) throws IOException {
        Path real = Files.createDirectory(dir.resolve("real"));
        Files.writeString(real.resolve("only.parquet"), "x");
        Path link = dir.resolve("link");
        assumeTrue(createSymbolicLink(link, real), "needs a filesystem and a user able to create symbolic links");

        assertThat(relativePathsOf(FileSource.file(link.resolve("only.parquet"))))
                .containsExactly("only.parquet");
    }

    @Test
    void missingSingleFileListsNothing(@TempDir Path dir) {
        assertThat(relativePathsOf(FileSource.file(dir.resolve("absent.parquet"))))
                .isEmpty();
        assertThat(relativePathsOf(FileSource.file(dir.resolve("absent/absent.parquet"))))
                .isEmpty();
    }

    private static List<String> relativePathsOf(FileSource source) {
        try (source;
                Stream<FileEntry> entries = source.list()) {
            return entries.map(FileEntry::relativePath).toList();
        }
    }

    /**
     * Removes every permission from {@code directory} and reports whether that stops a listing of it: false on a
     * filesystem without POSIX permissions or for a user not bound by permissions, such as root, with the directory
     * readable again.
     */
    private static boolean denyDirectoryReads(Path directory) throws IOException {
        if (!FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
            return false;
        }
        Files.setPosixFilePermissions(directory, Set.of());
        boolean denied = false;
        try (Stream<Path> children = Files.list(directory)) {
            children.forEach(child -> {});
        } catch (AccessDeniedException _) {
            denied = true;
        } finally {
            if (!denied) {
                restoreDirectoryReads(directory);
            }
        }
        return denied;
    }

    private static void restoreDirectoryReads(Path directory) throws IOException {
        Files.setPosixFilePermissions(directory, PosixFilePermissions.fromString("rwxr-xr-x"));
    }

    private static boolean createSymbolicLink(Path link, Path target) throws IOException {
        try {
            Files.createSymbolicLink(link, target);
            return true;
        } catch (UnsupportedOperationException | FileSystemException _) {
            return false;
        }
    }
}
