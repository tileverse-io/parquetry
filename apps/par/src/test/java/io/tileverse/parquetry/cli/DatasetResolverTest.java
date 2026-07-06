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
package io.tileverse.parquetry.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.testkit.TestCorpus;

class DatasetResolverTest {

    @Test
    void singleFileResolvesToOneDataset(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        try (DatasetResolver.OpenDataset open = DatasetResolver.open(file.toString(), new Properties())) {
            ParquetDataset dataset = open.dataset();
            assertThat(dataset.schema().leafColumns()).isNotEmpty();
        }
    }

    @Test
    void directoryOfParquetMergesIntoOneDataset(@TempDir Path dir) throws Exception {
        Fixtures.writeCities(dir.resolve("a.parquet"));
        Fixtures.writeCities(dir.resolve("b.parquet"));
        try (DatasetResolver.OpenDataset open = DatasetResolver.open(dir.toString(), new Properties())) {
            long oneFile;
            Path single = dir.resolve("a.parquet");
            try (DatasetResolver.OpenDataset solo = DatasetResolver.open(single.toString(), new Properties())) {
                oneFile = solo.dataset()
                        .count(
                                io.tileverse.parquetry.filter.Predicate.ALWAYS_TRUE,
                                io.tileverse.parquetry.data.ReadOptions.DEFAULTS);
            }
            long merged = open.dataset()
                    .count(
                            io.tileverse.parquetry.filter.Predicate.ALWAYS_TRUE,
                            io.tileverse.parquetry.data.ReadOptions.DEFAULTS);
            assertThat(merged).isEqualTo(oneFile * 2);
        }
    }

    @Test
    void globResolvesToFileset(@TempDir Path dir) throws Exception {
        Fixtures.writeCities(dir.resolve("a.parquet"));
        Fixtures.writeCities(dir.resolve("b.parquet"));
        // Build the glob as a forward-slash URI string: Path.resolve rejects '*' as an illegal character on Windows.
        String glob = dir.toUri() + "*.parquet";
        try (DatasetResolver.OpenDataset open = DatasetResolver.open(glob, new Properties())) {
            assertThat(open.dataset().schema().leafColumns()).isNotEmpty();
        }
    }

    @Test
    void localIcebergTableResolvesToIcebergDataset(@TempDir Path tmp) {
        Path tableDir = TestCorpus.extractDirectory("iceberg-geo-testbed/v3_geometry", tmp);
        try (DatasetResolver.OpenDataset open = DatasetResolver.open(tableDir.toString(), new Properties())) {
            assertThat(open.dataset().snapshot()).isPresent();
        }
    }

    @Test
    void detectsIcebergTableByMetadataMarker(@TempDir Path tmp) throws Exception {
        Path tableDir = TestCorpus.extractDirectory("iceberg-geo-testbed/v3_geometry", tmp);
        try (Storage storage = StorageFactory.open(tableDir.toUri())) {
            assertThat(DatasetResolver.hasIcebergMetadata(storage)).isTrue();
        }
    }

    @Test
    void plainParquetDirectoryIsNotDetectedAsIceberg(@TempDir Path dir) throws Exception {
        Fixtures.writeCities(dir.resolve("a.parquet"));
        try (Storage storage = StorageFactory.open(dir.toUri())) {
            assertThat(DatasetResolver.hasIcebergMetadata(storage)).isFalse();
        }
    }

    @Test
    void singleFileWithGlobCharsInNameOpensLiterally(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("data[0].parquet");
        Fixtures.writeCities(file);
        try (DatasetResolver.OpenDataset open = DatasetResolver.open(file.toString(), new Properties())) {
            assertThat(open.dataset().schema().leafColumns()).isNotEmpty();
        }
    }

    @Test
    void missingPathFailsClearly(@TempDir Path dir) {
        Path missing = dir.resolve("nope.parquet");
        org.assertj.core.api.Assertions.assertThatThrownBy(
                        () -> DatasetResolver.open(missing.toString(), new Properties()))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
