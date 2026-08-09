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
package io.tileverse.parquetry.geotools.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFinder;
import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.geotools.parquet.GeoParquetDataStoreFactory;

/**
 * End-to-end proof of the {@code layer-grouping=file} opt-in: a flat directory of heterogeneous GeoParquet files reads
 * through the GeoTools DataStore as one feature type per file, while the same directory without the parameter keeps
 * today's merged-schema failure. Fixtures are written with DuckDB exactly as {@link MultiFileDataStoreIT} does.
 */
class MultiLayerDataStoreIT {

    @Test
    void fileLayersPublishOneLayerPerFile(@TempDir Path dir) throws Exception {
        writeCities(dir.resolve("ne_cities.parquet"), 3);
        writeRivers(dir.resolve("ne_rivers.parquet"), 4);

        DataStore store = openStore(dir, "file");
        try {
            assertThat(store.getTypeNames()).containsExactly("ne_cities", "ne_rivers");

            SimpleFeatureSource cities = store.getFeatureSource("ne_cities");
            assertThat(cities.getCount(Query.ALL)).isEqualTo(3);
            assertThat(cities.getSchema().getDescriptor("name")).isNotNull();
            assertThat(cities.getSchema().getDescriptor("length_km")).isNull();
            assertThat(cities.getSchema().getGeometryDescriptor()).isNotNull();
            assertThat(readCount(store, "ne_cities")).isEqualTo(3);

            SimpleFeatureSource rivers = store.getFeatureSource("ne_rivers");
            assertThat(rivers.getCount(Query.ALL)).isEqualTo(4);
            assertThat(rivers.getSchema().getDescriptor("length_km")).isNotNull();
            assertThat(rivers.getSchema().getGeometryDescriptor()).isNotNull();
            assertThat(readCount(store, "ne_rivers")).isEqualTo(4);
        } finally {
            store.dispose();
        }
    }

    @Test
    void heterogeneousDirectoryWithoutLayersParamFailsOnAccess(@TempDir Path dir) throws Exception {
        writeCities(dir.resolve("ne_cities.parquet"), 3);
        writeRivers(dir.resolve("ne_rivers.parquet"), 4);

        Map<String, Object> params = storeParams(dir, null);
        GeoParquetDataStoreFactory factory = new GeoParquetDataStoreFactory();
        // The merged catalog resolves its dataset lazily: the store opens on the listing alone and the
        // schema mismatch reports on the first layer access, naming the files and the parameter that
        // publishes each file as its own layer.
        DataStore store = factory.createDataStore(params);
        try {
            String typeName = store.getTypeNames()[0];
            assertThatThrownBy(() -> store.getSchema(typeName))
                    .hasStackTraceContaining("do not share a schema")
                    .hasStackTraceContaining("ne_cities.parquet")
                    .hasStackTraceContaining("ne_rivers.parquet")
                    .hasStackTraceContaining("layer-grouping");
        } finally {
            store.dispose();
        }
    }

    @Test
    void fileLayersOverHiveTreeFindsNoFiles(@TempDir Path dir) throws Exception {
        Path partition = Files.createDirectories(dir.resolve("year=2024"));
        writeCities(partition.resolve("part-0.parquet"), 2);

        Map<String, Object> params = storeParams(dir, "file");
        GeoParquetDataStoreFactory factory = new GeoParquetDataStoreFactory();
        assertThatThrownBy(() -> factory.createDataStore(params))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no files found");
    }

    @Test
    void plainParquetFileBecomesGeometrylessLayer(@TempDir Path dir) throws Exception {
        writeCities(dir.resolve("cities.parquet"), 2);
        writePlain(dir.resolve("lookup.parquet"), 5);

        DataStore store = openStore(dir, "file");
        try {
            assertThat(store.getTypeNames()).containsExactly("cities", "lookup");
            assertThat(store.getSchema("lookup").getGeometryDescriptor()).isNull();
            assertThat(store.getFeatureSource("lookup").getCount(Query.ALL)).isEqualTo(5);
        } finally {
            store.dispose();
        }
    }

    private static DataStore openStore(Path dir, String layerGrouping) throws IOException {
        DataStore store = DataStoreFinder.getDataStore(storeParams(dir, layerGrouping));
        assertThat(store).isNotNull();
        return store;
    }

    private static Map<String, Object> storeParams(Path dir, String layerGrouping) {
        Map<String, Object> params = new HashMap<>();
        params.put("geoparquet", dir.toUri().toString());
        if (layerGrouping != null) {
            params.put("layer-grouping", layerGrouping);
        }
        return params;
    }

    private static int readCount(DataStore store, String typeName) throws IOException {
        int count = 0;
        try (FeatureReader<SimpleFeatureType, SimpleFeature> reader =
                store.getFeatureReader(new Query(typeName), Transaction.AUTO_COMMIT)) {
            while (reader.hasNext()) {
                reader.next();
                count++;
            }
        }
        return count;
    }

    private static void writeCities(Path target, int rowCount) throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSTALL spatial");
            stmt.execute("LOAD spatial");
            stmt.execute("COPY (SELECT id, 'city-' || id AS name, ST_Point(id, id) AS geometry FROM range(" + rowCount
                    + ") t(id)) TO '" + NestedFixtures.sqlPath(target) + "' (FORMAT PARQUET)");
        }
    }

    private static void writeRivers(Path target, int rowCount) throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSTALL spatial");
            stmt.execute("LOAD spatial");
            stmt.execute("COPY (SELECT id, 2.5 * id AS length_km, ST_Point(id, -id) AS geometry FROM range(" + rowCount
                    + ") t(id)) TO '" + NestedFixtures.sqlPath(target) + "' (FORMAT PARQUET)");
        }
    }

    private static void writePlain(Path target, int rowCount) throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            stmt.execute("COPY (SELECT id, 'name-' || id AS name FROM range(" + rowCount + ") t(id)) TO '"
                    + NestedFixtures.sqlPath(target) + "' (FORMAT PARQUET)");
        }
    }
}
