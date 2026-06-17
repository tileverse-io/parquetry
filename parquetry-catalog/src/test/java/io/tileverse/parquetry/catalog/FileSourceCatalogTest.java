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
package io.tileverse.parquetry.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.Dataset;
import io.tileverse.parquetry.dataset.DatasetCapabilities.PartitionModel;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.io.FileSource;
import io.tileverse.parquetry.io.LocalFileSource;
import io.tileverse.parquetry.testkit.TestCorpus;

class FileSourceCatalogTest {

    @TempDir
    Path tempDir;

    @Test
    void directoryOfSiblingFilesIsLayerPerDataset() throws Exception {
        Path corpus = TestCorpus.extractDirectory("geoparquet/test_data", tempDir.resolve("corpus"));
        Path source = corpus.resolveSibling("layers");
        Files.createDirectories(source);
        List<Path> picked = pickTwoParquet(corpus);
        Files.copy(picked.get(0), source.resolve("pois.parquet"));
        Files.copy(picked.get(1), source.resolve("buildings.parquet"));

        try (FileSource fileSource = LocalFileSource.directory(source, "*.parquet");
                FileSourceCatalog catalog = FileSourceCatalog.open(
                        fileSource, CatalogOptions.builder().build())) {
            assertThat(catalog.capabilities().enumeratesDatasets()).isTrue();
            assertThat(catalog.datasets()).containsExactlyInAnyOrder("pois", "buildings");
            Dataset pois = catalog.dataset("pois");
            assertThat(pois.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS)).isGreaterThan(0L);
        }
    }

    @Test
    void unitsCollidingOnSanitizedNameFailFast() throws Exception {
        Path corpus = TestCorpus.extractDirectory("geoparquet/test_data", tempDir.resolve("corpus3"));
        Path source = corpus.resolveSibling("colliding");
        Files.createDirectories(source);
        List<Path> picked = pickTwoParquet(corpus);
        Files.copy(picked.get(0), source.resolve("2024.parquet"));
        Files.copy(picked.get(1), source.resolve("_2024.parquet"));

        FileSource fileSource = LocalFileSource.directory(source, "*.parquet");
        CatalogOptions options = CatalogOptions.builder().build();
        assertThatThrownBy(() -> FileSourceCatalog.open(fileSource, options))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("_2024");
    }

    @Test
    void singleFileIsOneDataset() throws Exception {
        Path corpus = TestCorpus.extractDirectory("geoparquet/test_data", tempDir.resolve("corpus2"));
        Path one = pickTwoParquet(corpus).get(0);
        try (FileSource fileSource = LocalFileSource.file(one);
                FileSourceCatalog catalog = FileSourceCatalog.open(
                        fileSource, CatalogOptions.builder().build())) {
            assertThat(catalog.capabilities().enumeratesDatasets()).isFalse();
            assertThat(catalog.datasets()).hasSize(1);
            String name = catalog.datasets().get(0);
            assertThat(catalog.dataset(name).capabilities().partitionModel()).isEqualTo(PartitionModel.NONE);
        }
    }

    @Test
    void datasetNameWithMultipleUnitsFailsFast() throws Exception {
        Path corpus = TestCorpus.extractDirectory("geoparquet/test_data", tempDir.resolve("corpus4"));
        Path source = corpus.resolveSibling("named");
        Files.createDirectories(source);
        List<Path> picked = pickTwoParquet(corpus);
        Files.copy(picked.get(0), source.resolve("a.parquet"));
        Files.copy(picked.get(1), source.resolve("b.parquet"));

        FileSource fileSource = LocalFileSource.directory(source, "*.parquet");
        CatalogOptions options = CatalogOptions.builder().datasetName("x").build();
        assertThatThrownBy(() -> FileSourceCatalog.open(fileSource, options))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("datasetName");
    }

    private static List<Path> pickTwoParquet(Path dir) throws Exception {
        try (Stream<Path> walk = Files.walk(dir)) {
            List<Path> parquet = walk.filter(p -> p.toString().endsWith(".parquet"))
                    .sorted()
                    .limit(2)
                    .toList();
            if (parquet.size() < 2) {
                throw new IllegalStateException("need >=2 parquet fixtures under " + dir);
            }
            return parquet;
        }
    }
}
