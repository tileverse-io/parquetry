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

import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.geotools.parquet.GeoParquetDataStore;
import io.tileverse.parquetry.io.LocalFileSource;
import io.tileverse.parquetry.testkit.TestCorpus;

class GeoParquetReadIT {

    @Test
    void readsEveryFeatureWithGeometryAndAttributes(@TempDir Path dir) throws Exception {
        Path file = TestCorpus.extractFile("geoparquet/examples/example.parquet", dir);
        FilesetCatalog catalog = FilesetCatalog.open(
                LocalFileSource.file(file),
                CatalogOptions.builder().datasetName("example").build());
        try (CatalogDataStore store = new GeoParquetDataStore(catalog)) {
            CatalogFeatureSource fs = (CatalogFeatureSource) store.getFeatureSource("example");
            int expected = fs.getCount(Query.ALL);

            int seen = 0;
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = fs.getReader(Query.ALL)) {
                while (reader.hasNext()) {
                    SimpleFeature f = reader.next();
                    assertThat(f.getDefaultGeometry()).isInstanceOf(Geometry.class);
                    assertThat(f.getAttribute("name")).isInstanceOf(String.class);
                    seen++;
                }
            }
            assertThat(seen).isEqualTo(expected);
        }
    }
}
