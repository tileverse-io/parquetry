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
package io.tileverse.parquetry.geotools.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.GeoParquetDataset;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Smoke test for the DuckDB-generated nested GeoParquet fixture: it is written, has bytes, opens through the catalog
 * with the expected top-level columns and {@code geo} metadata, and at least one row reads back.
 */
class NestedFixturesTest {

    @Test
    void writesReadableNestedGeoParquet(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("nested.parquet");
        NestedFixtures.writeSample(file);

        assertThat(file).exists();
        assertThat(Files.size(file)).isPositive();

        try (FilesetCatalog catalog = NestedFixtures.openCatalog(file)) {
            GeoParquetDataset dataset = (GeoParquetDataset) catalog.dataset("nested");
            assertTopLevelColumns(dataset);
            assertGeoMetadataPresent(dataset);
            assertFirstRowReadable(dataset);
        }
    }

    private void assertTopLevelColumns(GeoParquetDataset dataset) {
        List<String> columns = dataset.schema().root().children().stream()
                .map(SchemaNode::name)
                .toList();
        assertThat(columns).containsExactlyInAnyOrder("id", "geometry", "brand", "addresses", "tags");
    }

    private void assertGeoMetadataPresent(GeoParquetDataset dataset) {
        assertThat(dataset.geoMetadata())
                .as("DuckDB writes the GeoParquet 'geo' metadata for the geometry column")
                .isPresent();
        assertThat(dataset.geoMetadata().orElseThrow().primaryColumn()).isEqualTo("geometry");
    }

    private void assertFirstRowReadable(GeoParquetDataset dataset) {
        try (Stream<ParquetRecord> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            ParquetRecord first = rows.findFirst().orElseThrow();
            assertThat(first).isNotNull();
        }
    }
}
