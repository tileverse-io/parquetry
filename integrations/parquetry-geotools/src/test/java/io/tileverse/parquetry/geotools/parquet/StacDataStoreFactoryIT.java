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
package io.tileverse.parquetry.geotools.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFactorySpi;
import org.geotools.api.data.DataStoreFinder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StacDataStoreFactoryIT {

    @Test
    void isDiscoverableThroughDataStoreFinder() {
        assertThat(availableFactories()).hasAtLeastOneElementOfType(StacDataStoreFactory.class);
    }

    @Test
    void canProcessRequiresOwnUriKey() {
        StacDataStoreFactory factory = new StacDataStoreFactory();
        assertThat(factory.canProcess(Map.of("stac-geoparquet", "file:///tmp/catalog.json")))
                .isTrue();
        assertThat(factory.canProcess(Map.of("uri", "file:///tmp/catalog.json")))
                .isFalse();
        assertThat(factory.canProcess(Map.of("geoparquet", "file:///tmp/x.parquet")))
                .isFalse();
        assertThat(factory.canProcess(Map.of("iceberg", "file:///tmp/wh"))).isFalse();
    }

    private static Iterable<DataStoreFactorySpi> availableFactories() {
        Iterator<DataStoreFactorySpi> factories = DataStoreFinder.getAvailableDataStores();
        return () -> factories;
    }

    @Test
    void exposesOneFeatureTypePerCollection(@TempDir Path tempDir) throws Exception {
        Path catalogJson = StacFixtures.writeOvertureMini(tempDir);

        StacDataStoreFactory factory = new StacDataStoreFactory();
        Map<String, Object> params =
                Map.of(StacDataStoreFactory.STAC_URI.key, catalogJson.toUri().toString());

        assertThat(factory.canProcess(params)).isTrue();

        // DataStore exposes dispose(), not AutoCloseable; release the store in a finally block.
        DataStore store = factory.createDataStore(params);
        try {
            assertThat(store.getTypeNames()).containsExactly("building");
        } finally {
            store.dispose();
        }
    }

    @Test
    void appendsCatalogJsonToAContainerUrl(@TempDir Path tempDir) throws Exception {
        StacFixtures.writeOvertureMini(tempDir);

        StacDataStoreFactory factory = new StacDataStoreFactory();
        // A directory URI ends in '/'; the factory must append catalog.json to reach the document.
        Map<String, Object> params =
                Map.of(StacDataStoreFactory.STAC_URI.key, tempDir.toUri().toString());

        DataStore store = factory.createDataStore(params);
        try {
            assertThat(store.getTypeNames()).contains("building");
        } finally {
            store.dispose();
        }
    }

    @Test
    void parquetUriOpensTheStacGeoParquetReader(@TempDir Path tempDir) throws Exception {
        Path itemTable = StacFixtures.writeItemTable(tempDir);

        StacDataStoreFactory factory = new StacDataStoreFactory();
        Map<String, Object> params =
                Map.of(StacDataStoreFactory.STAC_URI.key, itemTable.toUri().toString());

        DataStore store = factory.createDataStore(params);
        try {
            assertThat(store.getTypeNames()).containsExactly("building");
        } finally {
            store.dispose();
        }
    }
}
