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

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.SequencedMap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * The warehouse catalog over a tileverse Storage: discovery through the recursive metadata scan and every table served
 * by ONE shared Storage through prefixed per-table IOs, relocating each table's baked logical root onto its warehouse
 * subtree.
 */
class IcebergWarehouseCatalogStorageTest {

    @TempDir
    Path tempDir;

    @Test
    void discoversAndReadsThroughASharedStorage() {
        Path warehouse = warehouseWithTwoTables();
        String warehouseLocation = warehouse.toUri().toString().replaceAll("/$", "");
        Storage storage = StorageFactory.open(warehouse.toUri());

        try (IcebergWarehouseCatalog catalog = IcebergWarehouseCatalog.open(warehouseLocation, storage)) {
            assertThat(catalog.datasets()).containsExactly("ns1.tableA", "tableB");
            assertThat(catalog.dataset("ns1.tableA").count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isEqualTo(15L);
            assertThat(catalog.dataset("tableB").count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isEqualTo(10_000L);
        }
    }

    @Test
    void theRegistrySkipsDiscovery() {
        Path warehouse = warehouseWithTwoTables();
        String warehouseLocation = warehouse.toUri().toString().replaceAll("/$", "");
        Storage storage = StorageFactory.open(warehouse.toUri());
        SequencedMap<String, String> tables = new LinkedHashMap<>();
        tables.put("lineage", "ns1/tableA");

        try (IcebergWarehouseCatalog catalog = IcebergWarehouseCatalog.ofTables(tables, warehouseLocation, storage)) {
            assertThat(catalog.datasets()).containsExactly("lineage");
            assertThat(catalog.dataset("lineage").count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isEqualTo(15L);
        }
    }

    private Path warehouseWithTwoTables() {
        Path warehouse = tempDir.resolve("wh");
        TestCorpus.extractDirectory("iceberg-row-lineage/fresh", warehouse.resolve("ns1/tableA"));
        TestCorpus.extractDirectory("iceberg-geo-testbed/v2_flat_columns", warehouse.resolve("tableB"));
        return warehouse;
    }
}
