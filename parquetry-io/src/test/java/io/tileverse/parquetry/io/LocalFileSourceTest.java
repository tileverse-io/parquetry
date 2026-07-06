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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalFileSourceTest {

    @Test
    void directoryListsMatchingFilesWithRelativePaths(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("a.parquet"), "AAAA");
        Files.writeString(dir.resolve("b.parquet"), "BBBBBB");
        Files.writeString(dir.resolve("ignore.txt"), "x");

        FileSource source = LocalFileSource.directory(dir, "*.parquet");
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
        FileSource source = LocalFileSource.file(dir.resolve("only.parquet"));

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
}
