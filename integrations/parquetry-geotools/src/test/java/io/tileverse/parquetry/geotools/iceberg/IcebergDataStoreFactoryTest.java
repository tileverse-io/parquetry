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
package io.tileverse.parquetry.geotools.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.geotools.api.data.DataStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.geotools.data.CatalogDataStore;
import io.tileverse.parquetry.testkit.TestCorpus;

class IcebergDataStoreFactoryTest {

    @TempDir
    Path tempDir;

    @Test
    void canProcessRequiresIcebergKey() {
        IcebergDataStoreFactory factory = new IcebergDataStoreFactory();
        assertThat(factory.canProcess(Map.of("iceberg", "file:///tmp/wh"))).isTrue();
        assertThat(factory.canProcess(Map.of("geoparquet", "file:///tmp/x.parquet")))
                .isFalse();
    }

    @Test
    void singleTableDirOpensAsOneLayer() throws IOException {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve("corpus"));
        Path tableDir = root.resolve("v3_geometry");
        try (CatalogDataStore store = openStore(tableDir)) {
            assertThat(store.getTypeNames()).containsExactly("v3_geometry");
        }
    }

    @Test
    void warehouseDirOpensOneLayerPerTableWithDottedNames() throws IOException {
        Path warehouse = tempDir.resolve("wh");
        Path corpus = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve("corpus"));
        copyTable(corpus.resolve("v3_geometry"), warehouse.resolve("ns1/geo"));
        copyTable(corpus.resolve("v3_minimal"), warehouse.resolve("plain"));
        try (CatalogDataStore store = openStore(warehouse)) {
            assertThat(store.getTypeNames()).containsExactlyInAnyOrder("ns1.geo", "plain");
        }
    }

    @Test
    void neitherTableNorWarehouseFailsWithBothInterpretations() throws IOException {
        Path empty = Files.createDirectories(tempDir.resolve("empty"));
        IcebergDataStoreFactory factory = new IcebergDataStoreFactory();
        assertThatThrownBy(() ->
                        factory.createDataStore(Map.of("iceberg", empty.toUri().toString())))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("table")
                .hasMessageContaining("warehouse");
    }

    private static CatalogDataStore openStore(Path dir) throws IOException {
        DataStore store = new IcebergDataStoreFactory()
                .createDataStore(Map.of("iceberg", dir.toUri().toString()));
        return (CatalogDataStore) store;
    }

    private static void copyTable(Path from, Path to) throws IOException {
        try (Stream<Path> tree = Files.walk(from)) {
            List<Path> sources = tree.toList();
            for (Path source : sources) {
                Path target = to.resolve(from.relativize(source).toString());
                if (Files.isDirectory(source)) {
                    Files.createDirectories(target);
                } else {
                    Files.createDirectories(target.getParent());
                    Files.copy(source, target);
                }
            }
        }
    }
}
