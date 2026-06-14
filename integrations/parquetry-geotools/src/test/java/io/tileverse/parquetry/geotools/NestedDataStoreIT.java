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

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.catalog.ParquetDatasetCatalog;
import io.tileverse.parquetry.record.ParquetRecord;

/**
 * End-to-end coverage of the nested-attribute stack driven entirely through the public GeoTools DataStore API: a nested
 * ECQL filter pushed into the read, the matched features materialized into owned {@code List}/{@code Map} value
 * objects, and a projected query that retypes away the unselected nested attributes.
 *
 * <p>The fixture (see {@link NestedFixtures}) has three rows. {@code addresses.locality = 'NYC'} parses to the slash
 * PropertyName {@code addresses/locality} and translates to an existential quantified predicate over the
 * {@code LIST<STRUCT>} column: rows 1 and 3 hold an NYC address, row 2's list is empty. DuckDB's {@code list_filter}
 * answer over the same file is the oracle the pushed result set is checked against.
 */
class NestedDataStoreIT {

    private static final String NESTED_FILTER = "addresses.locality = 'NYC'";

    @Test
    void nestedFilterPushdownMatchesDuckDbOracle(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("nested.parquet");
        NestedFixtures.writeSample(file);

        Set<Integer> pushed = readMatchedIds(file, new Query("nested", ECQL.toFilter(NESTED_FILTER)));
        Set<Integer> oracle = duckDbNycMatches(file);

        assertThat(pushed)
                .as("nested ANY push-down matched ids equal DuckDB list_filter ids")
                .isEqualTo(oracle)
                .containsExactly(1, 3);
    }

    @Test
    void matchedFeaturesMaterializeAsOwnedValues(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("nested.parquet");
        NestedFixtures.writeSample(file);

        Map<Integer, SimpleFeature> byId = readMatchedFeatures(file, new Query("nested", ECQL.toFilter(NESTED_FILTER)));
        assertThat(byId.keySet()).containsExactlyInAnyOrder(1, 3);

        for (SimpleFeature feature : byId.values()) {
            assertThat(feature.getDefaultGeometry()).isInstanceOf(Geometry.class);
            assertNoLiveParquetRecord(feature);
        }

        assertFeatureOneHasNycAddress(byId.get(1));
    }

    @Test
    void projectionRetypesAwayUnselectedNestedAttributes(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("nested.parquet");
        NestedFixtures.writeSample(file);

        Query query = new Query("nested", ECQL.toFilter(NESTED_FILTER));
        query.setPropertyNames(new String[] {"id", "addresses"});

        Map<Integer, SimpleFeature> byId = readMatchedFeatures(file, query);
        assertThat(byId.keySet()).containsExactlyInAnyOrder(1, 3);

        SimpleFeature featureOne = byId.get(1);
        assertThat(featureOne.getAttribute("id")).isEqualTo(1);
        assertFeatureOneHasNycAddress(featureOne);

        SimpleFeatureType projected = featureOne.getFeatureType();
        assertThat(projected.getDescriptor("tags")).isNull();
        assertThat(projected.getDescriptor("brand")).isNull();
    }

    private Set<Integer> readMatchedIds(Path file, Query query) throws Exception {
        return new TreeSet<>(readMatchedFeatures(file, query).keySet());
    }

    private Map<Integer, SimpleFeature> readMatchedFeatures(Path file, Query query) throws Exception {
        List<SimpleFeature> features = new ArrayList<>();
        try (ParquetDatasetCatalog catalog = NestedFixtures.openCatalog(file);
                GeoParquetDataStore store = new GeoParquetDataStore(catalog)) {
            GeoParquetFeatureSource fs = (GeoParquetFeatureSource) store.getFeatureSource("nested");
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = fs.getReader(query)) {
                while (reader.hasNext()) {
                    features.add(reader.next());
                }
            }
        }
        return indexById(features);
    }

    private Map<Integer, SimpleFeature> indexById(List<SimpleFeature> features) {
        Map<Integer, SimpleFeature> byId = new LinkedHashMap<>();
        for (SimpleFeature feature : features) {
            byId.put((Integer) feature.getAttribute("id"), feature);
        }
        return byId;
    }

    private void assertFeatureOneHasNycAddress(SimpleFeature feature) {
        Object addresses = feature.getAttribute("addresses");
        assertThat(addresses).isInstanceOf(List.class);

        List<?> addressList = (List<?>) addresses;
        assertThat(addressList.get(0)).isInstanceOf(Map.class);

        Map<?, ?> firstAddress = (Map<?, ?>) addressList.get(0);
        assertThat(firstAddress.get("locality")).isEqualTo("NYC");
        assertThat(firstAddress.get("postcode")).isEqualTo("10001");
    }

    private void assertNoLiveParquetRecord(SimpleFeature feature) {
        for (Object attribute : feature.getAttributes()) {
            assertNoParquetRecord(attribute);
        }
    }

    private void assertNoParquetRecord(Object value) {
        if (value == null) {
            return;
        }
        assertThat(value).isNotInstanceOf(ParquetRecord.class);
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                assertNoParquetRecord(entry.getKey());
                assertNoParquetRecord(entry.getValue());
            }
        } else if (value instanceof List<?> list) {
            for (Object element : list) {
                assertNoParquetRecord(element);
            }
        }
    }

    private Set<Integer> duckDbNycMatches(Path file) throws SQLException {
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
