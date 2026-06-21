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
package io.tileverse.parquetry.geotools;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.geotools.api.data.Query;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.factory.CommonFactoryFinder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.io.LocalFileSource;

/**
 * End-to-end proof that a Hive-partitioned tree whose partition column is PHYSICALLY present in each file reads through
 * the GeoTools DataStore as ONE feature type, and that a filter on the partition column prunes non-matching files while
 * preserving exact results.
 *
 * <p>Each part is written with DuckDB (a {@code year} int column plus a {@code geometry} column, the latter making the
 * file GeoParquet) into a temp path, then moved under a {@code year=YYYY/} segment. The store is opened over the tree
 * root with the recursive {@code **.parquet} glob, mirroring {@code FilesetCatalogTest}.
 */
class HivePruningDataStoreIT {

    private static final FilterFactory FF = CommonFactoryFinder.getFilterFactory();

    @Test
    void filterOnPhysicalPartitionColumnReturnsOnlyMatchingRows(@TempDir Path root) throws Exception {
        int rows2023 = 5;
        int rows2024 = 3;
        writePartUnder(root, 2023, rows2023);
        writePartUnder(root, 2024, rows2024);

        try (FilesetCatalog catalog =
                        FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults());
                GeoParquetDataStore store = new GeoParquetDataStore(catalog)) {

            assertThat(store.getTypeNames()).hasSize(1);
            String typeName = store.getTypeNames()[0];
            SimpleFeatureSource fs = store.getFeatureSource(typeName);

            assertThat(fs.getCount(Query.ALL)).isEqualTo(rows2023 + rows2024);

            Filter only2024 = FF.equals(FF.property("year"), FF.literal(2024));
            assertThat(fs.getCount(new Query(typeName, only2024))).isEqualTo(rows2024);
        }
    }

    @Test
    void pathOnlyPartitionColumnIsSynthesizedIntoTheType(@TempDir Path root) throws Exception {
        int rowCount = 3;
        writePathOnlyPartitionedTree(root, 2024, rowCount);

        try (FilesetCatalog catalog =
                        FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults());
                GeoParquetDataStore store = new GeoParquetDataStore(catalog)) {

            String typeName = store.getTypeNames()[0];
            SimpleFeatureType schema = store.getSchema(typeName);
            assertThat(schema.getDescriptor("year")).isNotNull();

            SimpleFeatureSource fs = store.getFeatureSource(typeName);
            assertThat(fs.getCount(Query.ALL)).isEqualTo(rowCount);
        }
    }

    /**
     * Writes a Hive tree where {@code year} is the partition column but is NOT written into the files. DuckDB's
     * {@code PARTITION_BY} drops the partitioning column from the data, leaving a path-only {@code year=YYYY/} segment.
     */
    private static void writePathOnlyPartitionedTree(Path root, int year, int rowCount) throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSTALL spatial");
            stmt.execute("LOAD spatial");
            stmt.execute("COPY (SELECT " + year + " AS year, ST_Point(id, id) AS geometry FROM range(" + rowCount
                    + ") t(id)) TO '" + NestedFixtures.sqlPath(root) + "' (FORMAT PARQUET, PARTITION_BY (year))");
        }
    }

    /**
     * Writes a GeoParquet part with a physical {@code year} column and a {@code geometry} column, then moves it under a
     * {@code year=YYYY/} partition segment beneath {@code root}.
     */
    private static void writePartUnder(Path root, int year, int rowCount) throws SQLException, IOException {
        Path scratch = root.resolve("scratch-" + year + ".parquet");
        writeYearGeoFile(scratch, year, rowCount);
        Path partition = Files.createDirectories(root.resolve("year=" + year));
        Files.move(scratch, partition.resolve("data.parquet"));
    }

    private static void writeYearGeoFile(Path target, int year, int rowCount) throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSTALL spatial");
            stmt.execute("LOAD spatial");
            stmt.execute("COPY (SELECT " + year + " AS year, ST_Point(id, id) AS geometry FROM range(" + rowCount
                    + ") t(id)) TO '" + NestedFixtures.sqlPath(target) + "' (FORMAT PARQUET)");
        }
    }
}
