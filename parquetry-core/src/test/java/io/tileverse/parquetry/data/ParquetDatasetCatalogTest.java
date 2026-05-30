/*
 * Copyright (c) 2026 Multivers.io
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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.LocalFileSource;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

class ParquetDatasetCatalogTest {

    private static final Path FILE = CorpusFixtures.parquetTestingData().resolve("alltypes_plain.parquet");

    private long singleFileRowCount() {
        try (ByteRangeSource src = ByteRangeSource.ofFile(FILE)) {
            return ParquetDataset.open(src).count();
        }
    }

    @Test
    void singleFileDatasetReadsSchemaAndCount(@TempDir Path dir) throws Exception {
        Path only = dir.resolve("alltypes_plain.parquet");
        Files.copy(FILE, only);

        try (ParquetDatasetCatalog catalog =
                ParquetDatasetCatalog.open(LocalFileSource.file(only), CatalogOptions.defaults())) {
            assertThat(catalog.datasetNames()).containsExactly("alltypes_plain");
            ParquetDataset ds = catalog.dataset("alltypes_plain");
            assertThat(ds.count()).isEqualTo(singleFileRowCount());
        }
    }

    @Test
    void flatDirectoryConcatenatesSameSchemaFiles(@TempDir Path dir) throws Exception {
        Files.copy(FILE, dir.resolve("a.parquet"));
        Files.copy(FILE, dir.resolve("b.parquet"));

        try (ParquetDatasetCatalog catalog = ParquetDatasetCatalog.open(
                LocalFileSource.directory(dir, "*.parquet"),
                CatalogOptions.builder().datasetName("places").build())) {

            assertThat(catalog.datasetNames()).containsExactly("places");
            ParquetDataset ds = catalog.dataset("places");
            assertThat(ds.count()).isEqualTo(2 * singleFileRowCount());
        }
    }

    @Test
    void unknownDatasetNameRejected(@TempDir Path dir) throws Exception {
        Files.copy(FILE, dir.resolve("a.parquet"));
        try (ParquetDatasetCatalog catalog =
                ParquetDatasetCatalog.open(LocalFileSource.directory(dir, "*.parquet"), CatalogOptions.defaults())) {
            assertThatThrownBy(() -> catalog.dataset("nope")).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void emptySourceRejected(@TempDir Path dir) {
        LocalFileSource emptySource = LocalFileSource.directory(dir, "*.parquet");
        CatalogOptions options = CatalogOptions.defaults();
        assertThatThrownBy(() -> ParquetDatasetCatalog.open(emptySource, options))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void multiFileNameDerivedFromDirectory(@TempDir Path parent) throws Exception {
        Path datasetDir = Files.createDirectory(parent.resolve("places"));
        Files.copy(FILE, datasetDir.resolve("a.parquet"));
        Files.copy(FILE, datasetDir.resolve("b.parquet"));

        try (ParquetDatasetCatalog catalog = ParquetDatasetCatalog.open(
                LocalFileSource.directory(datasetDir, "*.parquet"), CatalogOptions.defaults())) {
            assertThat(catalog.datasetNames()).containsExactly("places");
            assertThat(catalog.dataset("places").count()).isEqualTo(2 * singleFileRowCount());
        }
    }
}
