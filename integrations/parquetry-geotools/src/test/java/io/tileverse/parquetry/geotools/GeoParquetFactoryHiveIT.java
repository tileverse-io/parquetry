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
import java.util.Map;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.Query;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.factory.CommonFactoryFinder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end proof that the production path - {@link GeoParquetDataStoreFactory#createDataStore(Map)} over a directory
 * URI - discovers Hive-partitioned and flat directories alike, using the recursive {@code **}{@code /*.parquet} glob
 * the factory emits. A non-recursive glob would leave a {@code year=YYYY/} tree unreachable through the factory, which
 * is the bug this guards against.
 */
class GeoParquetFactoryHiveIT {

    private static final FilterFactory FF = CommonFactoryFinder.getFilterFactory();

    @Test
    void factoryOverHiveDirectoryResolvesOneTypeAndPrunes(@TempDir Path root) throws Exception {
        int rows2023 = 5;
        int rows2024 = 3;
        writePartUnder(root, 2023, rows2023);
        writePartUnder(root, 2024, rows2024);

        DataStore store = openFactoryStore(root);
        try {
            assertThat(store.getTypeNames()).hasSize(1);
            String typeName = store.getTypeNames()[0];
            SimpleFeatureSource fs = store.getFeatureSource(typeName);

            assertThat(fs.getCount(Query.ALL)).isEqualTo(rows2023 + rows2024);

            Filter only2024 = FF.equals(FF.property("year"), FF.literal(2024));
            assertThat(fs.getCount(new Query(typeName, only2024))).isEqualTo(rows2024);
        } finally {
            store.dispose();
        }
    }

    @Test
    void factoryOverFlatDirectoryOfPartsResolvesOneType(@TempDir Path root) throws Exception {
        int rowsA = 4;
        int rowsB = 6;
        writeYearGeoFile(root.resolve("part-a.parquet"), 2023, rowsA);
        writeYearGeoFile(root.resolve("part-b.parquet"), 2024, rowsB);

        DataStore store = openFactoryStore(root);
        try {
            assertThat(store.getTypeNames()).hasSize(1);
            String typeName = store.getTypeNames()[0];
            SimpleFeatureSource fs = store.getFeatureSource(typeName);

            assertThat(fs.getCount(Query.ALL)).isEqualTo(rowsA + rowsB);
        } finally {
            store.dispose();
        }
    }

    private static DataStore openFactoryStore(Path root) throws IOException {
        Map<String, Object> params =
                Map.of("filetype", "geoparquet", "uri", root.toUri().toString());
        return new GeoParquetDataStoreFactory().createDataStore(params);
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
