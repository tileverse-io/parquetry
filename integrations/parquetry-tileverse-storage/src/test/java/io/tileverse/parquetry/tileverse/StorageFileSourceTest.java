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
package io.tileverse.parquetry.tileverse;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.FileEntry;
import io.tileverse.parquetry.io.FileSource;

class StorageFileSourceTest {

    @Test
    void openFactoryOwnsAndClosesStorage(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("a.parquet"), "AAAA");

        try (FileSource source = StorageFileSource.open(dir.toUri(), "*.parquet", new java.util.Properties())) {
            List<FileEntry> files;
            try (Stream<FileEntry> s = source.list()) {
                files = s.toList();
            }
            assertThat(files).extracting(FileEntry::relativePath).containsExactly("a.parquet");
        }
    }

    @Test
    void listsParquetFilesOverLocalFileStorage(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("a.parquet"), "AAAA");
        Files.writeString(dir.resolve("b.parquet"), "BBBBBB");
        Files.writeString(dir.resolve("note.txt"), "x");

        Storage storage = StorageFactory.open(dir.toUri());
        try (FileSource source = StorageFileSource.over(storage, "*.parquet")) {
            List<FileEntry> files;
            try (Stream<FileEntry> s = source.list()) {
                files = s.sorted(Comparator.comparing(FileEntry::relativePath)).toList();
            }
            assertThat(files).extracting(FileEntry::relativePath).containsExactly("a.parquet", "b.parquet");
            assertThat(files).extracting(FileEntry::sizeBytes).containsExactly(4L, 6L);
        }
    }

    @Test
    void singleObjectOpensWithoutListing(@TempDir Path dir) throws Exception {
        Files.writeString(dir.resolve("data.parquet"), "DATA");
        URI objectUri = dir.resolve("data.parquet").toUri();

        try (FileSource source = ParquetFileSources.openObject(objectUri, new Properties())) {
            List<FileEntry> entries;
            try (Stream<FileEntry> s = source.list()) {
                entries = s.toList();
            }
            assertThat(entries).hasSize(1);
            assertThat(entries.get(0).relativePath()).isEqualTo("data.parquet");
            try (ByteRangeSource bytes = entries.get(0).open()) {
                assertThat(bytes.size()).isPositive();
            }
        }
    }
}
