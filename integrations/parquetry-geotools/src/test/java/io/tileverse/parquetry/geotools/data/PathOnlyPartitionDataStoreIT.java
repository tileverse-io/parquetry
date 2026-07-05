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

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;

import org.geotools.api.data.Query;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.FeatureIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.io.LocalFileSource;

/**
 * End-to-end proof that the GeoTools DataStore reads a Hive tree whose partition column is NOT physically present in
 * the data files. DuckDB's {@code PARTITION_BY} drops the {@code year} column from each file, leaving it only in the
 * {@code year=YYYY/} path segment. The catalog synthesizes that column back into the dataset schema, and this test
 * proves it reaches GeoTools as a feature attribute: the {@link SimpleFeatureType} exposes a {@code year} attribute
 * bound to {@link Long}, a filter on {@code year} returns the right features, and each read feature has its partition
 * value.
 *
 * <p>The same DuckDB-partitioned tree doubles as the oracle: {@code read_parquet('<dir>/**}{@code /*.parquet',
 * hive_partitioning=true)} reconstructs the {@code year} column from the path, and its filtered count must equal the
 * GeoTools count.
 */
class PathOnlyPartitionDataStoreIT {

    private static final FilterFactory FF = CommonFactoryFinder.getFilterFactory();

    private static final int ROWS_2023 = 5;
    private static final int ROWS_2024 = 3;

    private static final int ROWS_JAN = 4;
    private static final int ROWS_FEB = 2;

    @Test
    void synthesizedPartitionColumnReadsThroughTheDataStore(@TempDir Path root) throws Exception {
        writePathOnlyTree(root);

        try (FilesetCatalog catalog =
                        FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults());
                CatalogDataStore store = new CatalogDataStore(catalog)) {

            assertThat(store.getTypeNames()).hasSize(1);
            String typeName = store.getTypeNames()[0];

            assertYearAttributeIsLong(store.getSchema(typeName));

            SimpleFeatureSource fs = store.getFeatureSource(typeName);
            assertThat(fs.getCount(Query.ALL)).isEqualTo(ROWS_2023 + ROWS_2024);

            assertFilterOnYearMatchesPartition(fs, typeName, root);
        }
    }

    @Test
    void synthesizedDatePartitionColumnBindsToDate(@TempDir Path root) throws Exception {
        writeDatePartitionedTree(root);

        try (FilesetCatalog catalog =
                        FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults());
                CatalogDataStore store = new CatalogDataStore(catalog)) {

            String typeName = store.getTypeNames()[0];

            SimpleFeatureType schema = store.getSchema(typeName);
            assertThat(schema.getDescriptor("dt")).isNotNull();
            assertThat(schema.getDescriptor("dt").getType().getBinding()).isEqualTo(Date.class);

            SimpleFeatureSource fs = store.getFeatureSource(typeName);
            Filter onlyJan = FF.equals(FF.property("dt"), FF.literal(dateAtUtcMidnight("2024-01-01")));
            Query query = new Query(typeName, onlyJan);

            int seen = 0;
            try (FeatureIterator<SimpleFeature> features = fs.getFeatures(query).features()) {
                while (features.hasNext()) {
                    SimpleFeature feature = features.next();
                    assertThat(feature.getAttribute("dt")).isEqualTo(dateAtUtcMidnight("2024-01-01"));
                    seen++;
                }
            }
            assertThat(seen).isEqualTo(ROWS_JAN);
        }
    }

    private static Date dateAtUtcMidnight(String isoDate) {
        return Date.from(LocalDate.parse(isoDate).atStartOfDay(ZoneOffset.UTC).toInstant());
    }

    private static void writeDatePartitionedTree(Path root) throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSTALL spatial");
            stmt.execute("LOAD spatial");
            stmt.execute("COPY (SELECT CASE WHEN id < " + ROWS_JAN
                    + " THEN DATE '2024-01-01' ELSE DATE '2024-02-01' END AS dt, "
                    + "ST_Point(id, id) AS geometry FROM range(" + (ROWS_JAN + ROWS_FEB)
                    + ") t(id)) TO '" + NestedFixtures.sqlPath(root) + "' (FORMAT PARQUET, PARTITION_BY (dt))");
        }
    }

    private static void assertYearAttributeIsLong(SimpleFeatureType schema) {
        assertThat(schema.getDescriptor("year")).isNotNull();
        assertThat(schema.getDescriptor("year").getType().getBinding()).isEqualTo(Long.class);
    }

    private static void assertFilterOnYearMatchesPartition(SimpleFeatureSource fs, String typeName, Path root)
            throws Exception {
        Filter only2024 = FF.equals(FF.property("year"), FF.literal(2024L));
        Query query = new Query(typeName, only2024);

        assertThat(fs.getCount(query)).isEqualTo(ROWS_2024);
        assertEveryFeatureHasYear(fs, query, 2024L);
        assertThat(fs.getCount(query)).isEqualTo(duckDbCountForYear(root, 2024));
    }

    private static void assertEveryFeatureHasYear(SimpleFeatureSource fs, Query query, long expectedYear)
            throws Exception {
        int seen = 0;
        try (FeatureIterator<SimpleFeature> features = fs.getFeatures(query).features()) {
            while (features.hasNext()) {
                SimpleFeature feature = features.next();
                assertThat(feature.getAttribute("year")).isEqualTo(expectedYear);
                seen++;
            }
        }
        assertThat(seen).isEqualTo(ROWS_2024);
    }

    /**
     * Writes a Hive tree where {@code year} is the partition column but is NOT written into the files: DuckDB's
     * {@code PARTITION_BY} drops the partitioning column from the data, leaving a path-only {@code year=YYYY/} segment.
     * Both partitions are written in one {@code COPY} that derives {@code year} from {@code id}.
     */
    private static void writePathOnlyTree(Path root) throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSTALL spatial");
            stmt.execute("LOAD spatial");
            stmt.execute("COPY (SELECT CASE WHEN id < " + ROWS_2023 + " THEN 2023 ELSE 2024 END AS year, "
                    + "ST_Point(id, id) AS geometry FROM range(" + (ROWS_2023 + ROWS_2024)
                    + ") t(id)) TO '" + NestedFixtures.sqlPath(root) + "' (FORMAT PARQUET, PARTITION_BY (year))");
        }
    }

    /**
     * The oracle count: DuckDB reconstructs the path-only {@code year} column from the {@code year=YYYY/} segment via
     * {@code hive_partitioning=true} and counts the rows matching {@code year}.
     */
    private static long duckDbCountForYear(Path root, int year) throws SQLException {
        // Build the glob as a string, not via Path.resolve: Windows rejects '*' as an illegal path char.
        String glob = NestedFixtures.sqlPath(root) + "/**/*.parquet";
        String sql = "SELECT count(*) FROM read_parquet('" + glob + "', hive_partitioning=true) WHERE year = " + year;
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
