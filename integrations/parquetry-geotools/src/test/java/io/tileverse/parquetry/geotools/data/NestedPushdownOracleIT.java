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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.TreeSet;

import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.geotools.parquet.GeoParquetDataStore;

/**
 * Cross-checks the GeoTools DataStore's nested-path filter push-down against DuckDB's {@code list_filter} semantics
 * over the same file. A CQL filter on the nested leaf {@code addresses.locality} is translated to a parquetry
 * quantified predicate, pushed into the read, and the returned feature ids must equal DuckDB's answer for the
 * equivalent existential query on the same {@code LIST<STRUCT>} column.
 */
class NestedPushdownOracleIT {

    @Test
    void anyNestedEqualsMatchesDuckDbOverDataStore(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("nested.parquet");
        NestedFixtures.writeSample(file);

        Set<Integer> pushed = dataStoreMatches(file, ECQL.toFilter("addresses.locality = 'NYC'"));
        Set<Integer> oracle = duckDbAnyMatches(file);

        assertThat(pushed)
                .as("nested ANY push-down matched ids equal DuckDB list_filter ids")
                .isEqualTo(oracle)
                .containsExactly(1, 3);
    }

    private Set<Integer> dataStoreMatches(Path file, Filter filter) throws Exception {
        Set<Integer> ids = new TreeSet<>();
        Query query = new Query("nested", filter);
        try (FilesetCatalog catalog = NestedFixtures.openCatalog(file);
                CatalogDataStore store = new GeoParquetDataStore(catalog)) {
            CatalogFeatureSource fs = (CatalogFeatureSource) store.getFeatureSource("nested");
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = fs.getReader(query)) {
                while (reader.hasNext()) {
                    ids.add((Integer) reader.next().getAttribute("id"));
                }
            }
        }
        return ids;
    }

    private Set<Integer> duckDbAnyMatches(Path file) throws SQLException {
        String sql = "SELECT id FROM read_parquet('" + sqlPath(file) + "') "
                + "WHERE len(list_filter(addresses, a -> a.locality = 'NYC')) > 0";
        Set<Integer> ids = new TreeSet<>();
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                ids.add(rs.getInt("id"));
            }
        }
        return ids;
    }

    private static String sqlPath(Path file) {
        return file.toAbsolutePath().toString().replace("'", "''");
    }
}
