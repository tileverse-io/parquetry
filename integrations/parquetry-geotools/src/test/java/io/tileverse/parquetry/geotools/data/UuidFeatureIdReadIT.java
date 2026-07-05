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
package io.tileverse.parquetry.geotools.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.FilterFactory;
import org.geotools.factory.CommonFactoryFinder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.geotools.parquet.GeoParquetDataStore;
import io.tileverse.parquetry.io.LocalFileSource;

/**
 * Reads a GeoParquet file whose feature id column is a {@code UUID} logical type and asserts the reader materializes
 * the canonical {@link java.util.UUID} object as the attribute value and its 8-4-4-4-12 string as the feature id.
 *
 * <p>DuckDB writes the fixture: a {@code UUID} column lands as {@code FIXED_LEN_BYTE_ARRAY(16)} with the Parquet UUID
 * logical type, the geometry column auto-emits the GeoParquet {@code geo} key-value metadata, and the schema mapper
 * binds the UUID column to {@link java.util.UUID} and auto-detects it as the feature id (it is named {@code id}).
 */
class UuidFeatureIdReadIT {

    private static final FilterFactory FF = CommonFactoryFinder.getFilterFactory();

    private static final UUID ROW_UUID = UUID.fromString("0123456f-89ab-cdef-fedc-ba9876543210");

    private static final List<UUID> ROW_UUIDS = List.of(
            UUID.fromString("11111111-1111-1111-1111-111111111111"),
            UUID.fromString("22222222-2222-2222-2222-222222222222"),
            UUID.fromString("33333333-3333-3333-3333-333333333333"),
            UUID.fromString("44444444-4444-4444-4444-444444444444"));

    @Test
    void materializesUuidColumnAndCanonicalFeatureId(@TempDir Path dir) throws Exception {
        Path file = writeUuidGeoParquet(dir.resolve("uuid.parquet"));
        FilesetCatalog catalog = FilesetCatalog.open(
                LocalFileSource.file(file),
                CatalogOptions.builder().datasetName("uuid").build());
        try (CatalogDataStore store = new GeoParquetDataStore(catalog)) {
            CatalogFeatureSource fs = (CatalogFeatureSource) store.getFeatureSource("uuid");
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = fs.getReader(Query.ALL)) {
                assertThat(reader.hasNext()).isTrue();
                SimpleFeature feature = reader.next();

                assertThat(feature.getAttribute("id"))
                        .as("a UUID column materializes as a java.util.UUID, not raw bytes")
                        .isEqualTo(ROW_UUID);
                assertThat(feature.getID())
                        .as("the UUID feature id stringifies to its canonical 8-4-4-4-12 form")
                        .isEqualTo(ROW_UUID.toString());
            }
        }
    }

    @Test
    void uuidColumnBindsToJavaUuid(@TempDir Path dir) throws Exception {
        Path file = writeMultiRowUuidGeoParquet(dir.resolve("uuids.parquet"));
        try (CatalogDataStore store = open(file)) {
            CatalogFeatureSource fs = (CatalogFeatureSource) store.getFeatureSource("uuid");
            SimpleFeatureType featureType = fs.getSchema();

            assertThat(featureType.getDescriptor("id").getType().getBinding())
                    .as("a UUID logical-type column binds to java.util.UUID")
                    .isEqualTo(UUID.class);
        }
    }

    @Test
    void readsEveryRowWithCanonicalUuidFeatureId(@TempDir Path dir) throws Exception {
        Path file = writeMultiRowUuidGeoParquet(dir.resolve("uuids.parquet"));
        try (CatalogDataStore store = open(file)) {
            CatalogFeatureSource fs = (CatalogFeatureSource) store.getFeatureSource("uuid");

            List<UUID> readIds = new ArrayList<>();
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = fs.getReader(new Query("uuid"))) {
                while (reader.hasNext()) {
                    SimpleFeature feature = reader.next();
                    UUID id = (UUID) feature.getAttribute("id");
                    assertThat(feature.getID())
                            .as("the feature id is the canonical UUID string of its row")
                            .isEqualTo(id.toString());
                    readIds.add(id);
                }
            }
            assertThat(readIds).containsExactlyInAnyOrderElementsOf(ROW_UUIDS);
        }
    }

    @Test
    void idQueryForKnownUuidsReturnsExactlyThoseFeatures(@TempDir Path dir) throws Exception {
        Path file = writeMultiRowUuidGeoParquet(dir.resolve("uuids.parquet"));
        try (CatalogDataStore store = open(file)) {
            CatalogFeatureSource fs = (CatalogFeatureSource) store.getFeatureSource("uuid");

            UUID first = ROW_UUIDS.get(0);
            UUID second = ROW_UUIDS.get(2);
            Query byIds =
                    new Query("uuid", FF.id(Set.of(FF.featureId(first.toString()), FF.featureId(second.toString()))));

            List<UUID> matched = new ArrayList<>();
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = fs.getReader(byIds)) {
                while (reader.hasNext()) {
                    matched.add((UUID) reader.next().getAttribute("id"));
                }
            }
            assertThat(matched).containsExactlyInAnyOrder(first, second);
        }
    }

    private static CatalogDataStore open(Path file) {
        FilesetCatalog catalog = FilesetCatalog.open(
                LocalFileSource.file(file),
                CatalogOptions.builder().datasetName("uuid").build());
        return new GeoParquetDataStore(catalog);
    }

    private static Path writeMultiRowUuidGeoParquet(Path target) throws Exception {
        Files.deleteIfExists(target);
        StringBuilder rows = new StringBuilder();
        for (int i = 0; i < ROW_UUIDS.size(); i++) {
            if (i > 0) {
                rows.append(" UNION ALL ");
            }
            rows.append("SELECT CAST('")
                    .append(ROW_UUIDS.get(i))
                    .append("' AS UUID) AS id, ST_Point(")
                    .append(i)
                    .append(", ")
                    .append(i)
                    .append(") AS geometry, 'feature-")
                    .append(i)
                    .append("' AS name");
        }
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSTALL spatial");
            stmt.execute("LOAD spatial");
            stmt.execute("COPY (" + rows + ") TO '" + sqlPath(target) + "' (FORMAT PARQUET)");
        }
        return target;
    }

    private static Path writeUuidGeoParquet(Path target) throws Exception {
        Files.deleteIfExists(target);
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSTALL spatial");
            stmt.execute("LOAD spatial");
            stmt.execute("COPY (SELECT CAST('" + ROW_UUID + "' AS UUID) AS id, ST_Point(1, 2) AS geometry, "
                    + "'feature' AS name) TO '" + sqlPath(target) + "' (FORMAT PARQUET)");
        }
        return target;
    }

    private static String sqlPath(Path file) {
        return file.toAbsolutePath().toString().replace("'", "''");
    }
}
