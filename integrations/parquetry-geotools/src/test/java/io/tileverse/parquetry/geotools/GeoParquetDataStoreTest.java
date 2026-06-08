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

import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.ParquetDatasetCatalog;
import io.tileverse.parquetry.io.LocalFileSource;
import io.tileverse.parquetry.testkit.TestCorpus;

class GeoParquetDataStoreTest {

    private ParquetDatasetCatalog openExample(Path dir) throws Exception {
        Path file = TestCorpus.extractFile("geoparquet/examples/example.parquet", dir);
        return ParquetDatasetCatalog.open(
                LocalFileSource.file(file),
                CatalogOptions.builder().datasetName("example").build());
    }

    @Test
    void exposesTypeNameSchemaCountAndBounds(@TempDir Path dir) throws Exception {
        try (GeoParquetDataStore store = new GeoParquetDataStore(openExample(dir))) {
            assertThat(store.getTypeNames()).containsExactly("example");

            SimpleFeatureType ft = store.getSchema("example");
            assertThat(ft.getGeometryDescriptor()).isNotNull();

            GeoParquetFeatureSource fs = (GeoParquetFeatureSource) store.getFeatureSource("example");
            int count = fs.getCount(Query.ALL);
            assertThat(count).isGreaterThan(0);

            ReferencedEnvelope bounds = fs.getBounds();
            assertThat(bounds).isNotNull();
            assertThat(bounds.getCoordinateReferenceSystem()).isNotNull();
        }
    }
}
