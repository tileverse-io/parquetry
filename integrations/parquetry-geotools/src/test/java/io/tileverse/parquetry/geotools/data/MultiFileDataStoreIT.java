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

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.geotools.api.data.Query;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.geotools.parquet.GeoParquetDataStore;
import io.tileverse.parquetry.io.LocalFileSource;

/**
 * End-to-end proof that a directory of same-schema GeoParquet part files reads through the GeoTools DataStore as ONE
 * feature type, with the part counts summed and the part bounds unioned, and that a plain Parquet file without geometry
 * yields a geometryless {@link SimpleFeatureType}.
 *
 * <p>Fixtures are written with DuckDB exactly as {@link NestedFixtures} does: a {@code COPY (SELECT ... ST_Point(...)
 * AS geometry ...) TO ... (FORMAT PARQUET)} over a {@code GEOMETRY} column auto-writes the GeoParquet {@code geo}
 * key-value metadata. The store is opened over a directory source via {@link LocalFileSource#directory}, mirroring
 * {@code FilesetCatalogTest}.
 */
class MultiFileDataStoreIT {

    @Test
    void directoryOfPartsIsOneTypeWithUnionedBounds(@TempDir Path dir) throws Exception {
        int rowsA = 4;
        int rowsB = 6;
        writeGeoPart(dir.resolve("part-a.parquet"), 0, 10, rowsA);
        writeGeoPart(dir.resolve("part-b.parquet"), 20, 30, rowsB);

        try (FilesetCatalog catalog = FilesetCatalog.open(
                        LocalFileSource.directory(dir, "*.parquet"),
                        CatalogOptions.builder().datasetName("places").build());
                CatalogDataStore store = new GeoParquetDataStore(catalog)) {

            assertThat(store.getTypeNames()).containsExactly("places");

            SimpleFeatureSource fs = store.getFeatureSource("places");
            assertThat(fs.getCount(Query.ALL)).isEqualTo(rowsA + rowsB);

            ReferencedEnvelope bounds = fs.getBounds();
            assertThat(bounds.getMinX()).isLessThanOrEqualTo(0.0);
            assertThat(bounds.getMaxX()).isGreaterThanOrEqualTo(30.0);
        }
    }

    @Test
    void plainParquetWithoutGeometryIsGeometryless(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("attributes.parquet");
        writePlainPart(file, 3);

        try (FilesetCatalog catalog = FilesetCatalog.open(
                        LocalFileSource.file(file),
                        CatalogOptions.builder().datasetName("attributes").build());
                CatalogDataStore store = new GeoParquetDataStore(catalog)) {

            SimpleFeatureType schema = store.getSchema("attributes");
            assertThat(schema.getGeometryDescriptor()).isNull();
            assertThat(schema.getDescriptor("id")).isNotNull();
            assertThat(schema.getDescriptor("name")).isNotNull();
        }
    }

    /**
     * Writes a GeoParquet part whose points all fall within the square {@code (lo lo, hi hi)}, pinning the part's bbox
     * to exactly that square: row 0 sits at the {@code (lo, lo)} corner and every other row at the {@code (hi, hi)}
     * corner. With {@code rowCount >= 2} both corners are present, giving the part the full {@code lo..hi} extent.
     */
    private static void writeGeoPart(Path target, int lo, int hi, int rowCount) throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSTALL spatial");
            stmt.execute("LOAD spatial");
            String coord = "(CASE WHEN id = 0 THEN " + lo + " ELSE " + hi + " END)";
            stmt.execute("COPY (SELECT id, ST_Point(" + coord + ", " + coord + ") AS geometry " + "FROM range("
                    + rowCount + ") t(id)) TO '" + NestedFixtures.sqlPath(target) + "' (FORMAT PARQUET)");
        }
    }

    private static void writePlainPart(Path target, int rowCount) throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            stmt.execute("COPY (SELECT id, 'name-' || id AS name FROM range(" + rowCount + ") t(id)) TO '"
                    + NestedFixtures.sqlPath(target) + "' (FORMAT PARQUET)");
        }
    }
}
