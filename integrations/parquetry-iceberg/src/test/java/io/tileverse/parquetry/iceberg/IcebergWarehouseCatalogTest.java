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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.SequencedMap;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * A warehouse directory of Iceberg tables reads as one multi-dataset catalog: tables discovered at any depth, named by
 * their dotted root-relative path, each opened lazily on first access. A corrupt table fails alone; discovery itself
 * never reads metadata.
 */
class IcebergWarehouseCatalogTest {

    @TempDir
    Path tempDir;

    @Test
    void discoversTablesAtAnyDepthWithDottedNames() {
        Path warehouse = warehouseWithTwoTables();

        try (IcebergWarehouseCatalog catalog = IcebergWarehouseCatalog.openLocal(warehouse)) {
            assertThat(catalog.datasets()).containsExactly("ns1.tableA", "tableB");
            assertThat(catalog.capabilities().enumeratesDatasets()).isTrue();

            ParquetDataset tableA = catalog.dataset("ns1.tableA");
            assertThat(tableA.name()).isEqualTo("ns1.tableA");
            assertThat(tableA.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isEqualTo(15L);
            assertThat(catalog.dataset("tableB").count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isEqualTo(10_000L);
        }
    }

    @Test
    void aCorruptTableConstructsTheCatalogAndFailsAloneOnAccess() throws Exception {
        Path warehouse = warehouseWithTwoTables();
        corruptEveryMetadataJson(warehouse.resolve("tableB/metadata"));

        try (IcebergWarehouseCatalog catalog = IcebergWarehouseCatalog.openLocal(warehouse)) {
            assertThat(catalog.datasets()).containsExactly("ns1.tableA", "tableB");
            assertThat(catalog.dataset("ns1.tableA").count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isEqualTo(15L);
            assertThatThrownBy(() -> catalog.dataset("tableB")).isInstanceOf(IcebergFormatException.class);
            assertThatThrownBy(() -> catalog.dataset("tableB")).isInstanceOf(IcebergFormatException.class);
        }
    }

    @Test
    void anUnknownNameNamesTheAvailableDatasets() {
        try (IcebergWarehouseCatalog catalog = IcebergWarehouseCatalog.openLocal(warehouseWithTwoTables())) {
            assertThatThrownBy(() -> catalog.dataset("nope"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("ns1.tableA");
        }
    }

    @Test
    void anEmptyWarehouseFailsLoudly() throws Exception {
        Path empty = Files.createDirectories(tempDir.resolve("empty"));

        assertThatThrownBy(() -> IcebergWarehouseCatalog.openLocal(empty))
                .isInstanceOf(IcebergFormatException.class)
                .hasMessageContaining("empty");
    }

    @Test
    void theLocalRegistryKeepsVerbatimNamesInMapOrder() {
        Path warehouse = warehouseWithTwoTables();
        SequencedMap<String, Path> tables = new LinkedHashMap<>();
        tables.put("zeta", warehouse.resolve("tableB"));
        tables.put("alpha", warehouse.resolve("ns1/tableA"));

        try (IcebergWarehouseCatalog catalog = IcebergWarehouseCatalog.ofLocalTables(tables)) {
            assertThat(catalog.datasets()).containsExactly("zeta", "alpha");
            assertThat(catalog.dataset("alpha").count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isEqualTo(15L);
        }
    }

    @Test
    void anEmptyRegistryIsRejected() {
        assertThatThrownBy(() -> IcebergWarehouseCatalog.ofLocalTables(new LinkedHashMap<>()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void closeIsIdempotent() {
        IcebergWarehouseCatalog catalog = IcebergWarehouseCatalog.openLocal(warehouseWithTwoTables());
        catalog.dataset("tableB");
        catalog.close();
        catalog.close();
    }

    @Test
    void aClosedCatalogRejectsDatasetAccess() {
        IcebergWarehouseCatalog catalog = IcebergWarehouseCatalog.openLocal(warehouseWithTwoTables());
        catalog.close();

        assertThatThrownBy(() -> catalog.dataset("tableB")).isInstanceOf(IllegalStateException.class);
    }

    private Path warehouseWithTwoTables() {
        Path warehouse = tempDir.resolve("wh");
        TestCorpus.extractDirectory("iceberg-row-lineage/fresh", warehouse.resolve("ns1/tableA"));
        TestCorpus.extractDirectory("iceberg-geo-testbed/v2_flat_columns", warehouse.resolve("tableB"));
        return warehouse;
    }

    private static void corruptEveryMetadataJson(Path metadataDir) throws IOException {
        try (Stream<Path> entries = Files.list(metadataDir)) {
            for (Path entry : (Iterable<Path>) entries::iterator) {
                if (entry.getFileName().toString().endsWith(".metadata.json")) {
                    Files.writeString(entry, "not json");
                }
            }
        }
    }
}
