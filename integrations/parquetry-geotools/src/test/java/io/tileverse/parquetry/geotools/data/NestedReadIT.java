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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.geotools.parquet.GeoParquetDataStore;
import io.tileverse.parquetry.record.ParquetRecord;

/**
 * Verifies that nested {@code STRUCT}/{@code LIST}/{@code MAP} attributes read back as owned, batch-independent values.
 *
 * <p>The reader is fully drained into a list before any assertion runs. Draining forces parquetry to recycle batch
 * buffers, which proves the feature values do not retain a live {@link ParquetRecord} view.
 */
class NestedReadIT {

    @Test
    void readsNestedAttributesAsOwnedValues(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("nested.parquet");
        NestedFixtures.writeSample(file);

        List<SimpleFeature> features = drainAllFeatures(file);

        Map<Integer, SimpleFeature> byId = indexById(features);
        assertThat(byId.keySet()).containsExactlyInAnyOrder(1, 2, 3);

        assertEveryFeatureHasGeometry(features);
        assertRowOne(byId.get(1));
        assertRowTwo(byId.get(2));
        assertRowThree(byId.get(3));
        assertNoLiveParquetRecord(features);
    }

    private List<SimpleFeature> drainAllFeatures(Path file) throws Exception {
        List<SimpleFeature> features = new ArrayList<>();
        try (FilesetCatalog catalog = NestedFixtures.openCatalog(file);
                CatalogDataStore store = new GeoParquetDataStore(catalog)) {
            CatalogFeatureSource fs = (CatalogFeatureSource) store.getFeatureSource("nested");
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = fs.getReader(Query.ALL)) {
                while (reader.hasNext()) {
                    features.add(reader.next());
                }
            }
        }
        return features;
    }

    private Map<Integer, SimpleFeature> indexById(List<SimpleFeature> features) {
        Map<Integer, SimpleFeature> byId = new LinkedHashMap<>();
        for (SimpleFeature feature : features) {
            byId.put((Integer) feature.getAttribute("id"), feature);
        }
        return byId;
    }

    private void assertEveryFeatureHasGeometry(List<SimpleFeature> features) {
        for (SimpleFeature feature : features) {
            assertThat(feature.getDefaultGeometry()).isInstanceOf(Geometry.class);
        }
    }

    private void assertRowOne(SimpleFeature feature) {
        assertThat(feature.getAttribute("id")).isEqualTo(1);

        Object addresses = feature.getAttribute("addresses");
        assertThat(addresses).isInstanceOf(List.class);
        List<?> addressList = (List<?>) addresses;
        assertThat(addressList).hasSize(2);

        assertThat(addressList.get(0)).isInstanceOf(Map.class);
        Map<?, ?> firstAddress = (Map<?, ?>) addressList.get(0);
        assertThat(firstAddress.get("locality")).isEqualTo("NYC");
        assertThat(firstAddress.get("postcode")).isEqualTo("10001");

        Object brand = feature.getAttribute("brand");
        assertThat(brand).isInstanceOf(Map.class);
        assertThat(((Map<?, ?>) brand).get("name")).isEqualTo("Acme");

        Object tags = feature.getAttribute("tags");
        assertThat(tags).isInstanceOf(Map.class);
        assertThat(((Map<?, ?>) tags).get("k1")).isEqualTo("v1");
    }

    private void assertRowTwo(SimpleFeature feature) {
        assertThat(feature.getAttribute("id")).isEqualTo(2);

        Object addresses = feature.getAttribute("addresses");
        assertThat(addresses).isInstanceOf(List.class);
        assertThat((List<?>) addresses).isEmpty();
    }

    private void assertRowThree(SimpleFeature feature) {
        assertThat(feature.getAttribute("id")).isEqualTo(3);

        Object addresses = feature.getAttribute("addresses");
        assertThat(addresses).isInstanceOf(List.class);
        List<?> addressList = (List<?>) addresses;
        assertThat(addressList).hasSize(2);

        assertThat(addressList.get(1)).isInstanceOf(Map.class);
        Map<?, ?> secondAddress = (Map<?, ?>) addressList.get(1);
        assertThat(secondAddress.get("locality")).isNull();
        assertThat(secondAddress.get("postcode")).isEqualTo("00000");
    }

    private void assertNoLiveParquetRecord(List<SimpleFeature> features) {
        for (SimpleFeature feature : features) {
            for (Object attribute : feature.getAttributes()) {
                assertNoParquetRecord(attribute);
            }
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
}
