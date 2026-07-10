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

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * A prefixed {@link StorageIcebergFileIO} maps a table's logical root onto a SUBTREE of a shared warehouse-rooted
 * Storage: {@code logical/x -> <keyPrefix>/x}. The metadata scan lists the whole warehouse through the same Storage.
 */
class StorageIcebergFileIOPrefixTest {

    @TempDir
    Path tempDir;

    @Test
    void aPrefixedIoResolvesLocationsUnderItsSubtree() throws Exception {
        Path file = tempDir.resolve("ns1/tableA/metadata/v1.metadata.json");
        Files.createDirectories(file.getParent());
        Files.writeString(file, "{}");

        try (Storage storage = StorageFactory.open(tempDir.toUri())) {
            StorageIcebergFileIO io = StorageIcebergFileIO.over(storage, "file:///logical/tableA", "ns1/tableA");

            try (ByteRangeSource source = io.open("file:///logical/tableA/metadata/v1.metadata.json")) {
                assertThat(source.size()).isEqualTo(2L);
            }
        }
    }

    @Test
    void theMetadataScanFindsTablesAtAnyDepth() throws Exception {
        Files.createDirectories(tempDir.resolve("ns1/tableA/metadata"));
        Files.writeString(tempDir.resolve("ns1/tableA/metadata/v1.metadata.json"), "{}");
        Files.createDirectories(tempDir.resolve("tableB/metadata"));
        Files.writeString(tempDir.resolve("tableB/metadata/v1.metadata.json"), "{}");
        Files.createDirectories(tempDir.resolve("tableB/data"));
        Files.writeString(tempDir.resolve("tableB/data/f.parquet"), "x");

        try (Storage storage = StorageFactory.open(tempDir.toUri())) {
            StorageIcebergFileIO io = StorageIcebergFileIO.over(storage, "wh://root");

            assertThat(io.listMetadataFiles("wh://root"))
                    .containsExactly(
                            "wh://root/ns1/tableA/metadata/v1.metadata.json",
                            "wh://root/tableB/metadata/v1.metadata.json");
        }
    }

    @Test
    void aPrefixedIoMapsListedKeysBackUnderItsLogicalRoot() throws Exception {
        Files.createDirectories(tempDir.resolve("ns1/tableA/metadata"));
        Files.writeString(tempDir.resolve("ns1/tableA/metadata/v1.metadata.json"), "{}");

        try (Storage storage = StorageFactory.open(tempDir.toUri())) {
            StorageIcebergFileIO io = StorageIcebergFileIO.over(storage, "file:///logical/tableA", "ns1/tableA");

            assertThat(io.listMetadataFiles("file:///logical/tableA"))
                    .containsExactly("file:///logical/tableA/metadata/v1.metadata.json");
            assertThat(io.list("file:///logical/tableA/metadata"))
                    .containsExactly("file:///logical/tableA/metadata/v1.metadata.json");
        }
    }

    @Test
    void aDeeperRootPrefixStillMapsHitsUnderTheLogicalRoot() throws Exception {
        Files.createDirectories(tempDir.resolve("ns1/tableA/metadata"));
        Files.writeString(tempDir.resolve("ns1/tableA/metadata/v1.metadata.json"), "{}");

        try (Storage storage = StorageFactory.open(tempDir.toUri())) {
            StorageIcebergFileIO io = StorageIcebergFileIO.over(storage, "wh://root");

            assertThat(io.listMetadataFiles("wh://root/ns1"))
                    .containsExactly("wh://root/ns1/tableA/metadata/v1.metadata.json");
        }
    }

    @Test
    void theMetadataScanExcludesLooseAndNonMetadataJsonFiles() throws Exception {
        Files.createDirectories(tempDir.resolve("tableB/metadata"));
        Files.writeString(tempDir.resolve("tableB/metadata/v1.metadata.json"), "{}");
        Files.writeString(tempDir.resolve("tableB/metadata/snap-1.avro"), "x");
        Files.writeString(tempDir.resolve("loose.metadata.json"), "{}");

        try (Storage storage = StorageFactory.open(tempDir.toUri())) {
            StorageIcebergFileIO io = StorageIcebergFileIO.over(storage, "wh://root");

            assertThat(io.listMetadataFiles("wh://root")).containsExactly("wh://root/tableB/metadata/v1.metadata.json");
        }
    }

    @Test
    void anEmptyRootFindsNothing() throws Exception {
        try (Storage storage = StorageFactory.open(tempDir.toUri())) {
            StorageIcebergFileIO io = StorageIcebergFileIO.over(storage, "wh://root");

            assertThat(io.listMetadataFiles("wh://root")).isEmpty();
        }
    }

    @Test
    void listMetadataFilesRejectsARootOutsideTheLogicalRoot() throws Exception {
        try (Storage storage = StorageFactory.open(tempDir.toUri())) {
            StorageIcebergFileIO io = StorageIcebergFileIO.over(storage, "file:///warehouse");

            assertThatThrownBy(() -> io.listMetadataFiles("file:///elsewhere"))
                    .isInstanceOf(IcebergFormatException.class);
        }
    }
}
