/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
import java.util.List;

import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.Name;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.DatasetCatalog;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.io.LocalFileSource;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Exercises the abstract {@link CatalogDataStore} directly through an anonymous subclass that adds no dataset
 * validation, proving the base store publishes any catalog dataset - geo or attribute-only - as a feature type.
 */
class CatalogDataStoreTest {

    @Test
    void exposesTypeNameSchemaCountAndBounds(@TempDir Path dir) throws Exception {
        try (CatalogDataStore store = openBaseStore(geoCatalog(dir))) {
            assertThat(store.getTypeNames()).containsExactly("example");

            SimpleFeatureType ft = store.getSchema("example");
            assertThat(ft.getGeometryDescriptor()).isNotNull();

            CatalogFeatureSource fs = (CatalogFeatureSource) store.getFeatureSource("example");
            int count = fs.getCount(Query.ALL);
            assertThat(count).isGreaterThan(0);

            ReferencedEnvelope bounds = fs.getBounds();
            assertThat(bounds).isNotNull();
            assertThat(bounds.getCoordinateReferenceSystem()).isNotNull();
        }
    }

    @Test
    void acceptsNonGeoCatalogAndPublishesAttributeOnlyType(@TempDir Path dir) throws Exception {
        try (CatalogDataStore store = openBaseStore(nonGeoCatalog(dir))) {
            List<Name> names = store.getNames();
            assertThat(names).hasSize(1);
            assertThat(store.getSchema(names.get(0)).getGeometryDescriptor())
                    .as("non-geo dataset maps to an attribute-only feature type")
                    .isNull();
        }
    }

    /** A bare subclass with no dataset validation, standing in for the abstract base store. */
    private static CatalogDataStore openBaseStore(DatasetCatalog catalog) {
        return new CatalogDataStore(catalog) {};
    }

    private static FilesetCatalog geoCatalog(Path dir) throws Exception {
        Path file = TestCorpus.extractFile("geoparquet/examples/example.parquet", dir);
        return FilesetCatalog.open(
                LocalFileSource.file(file),
                CatalogOptions.builder().datasetName("example").build());
    }

    private static FilesetCatalog nonGeoCatalog(Path dir) throws Exception {
        Path file = TestCorpus.extractFile("parquet-testing/data/alltypes_plain.parquet", dir);
        return FilesetCatalog.open(
                LocalFileSource.file(file),
                CatalogOptions.builder().datasetName("plain").build());
    }
}
