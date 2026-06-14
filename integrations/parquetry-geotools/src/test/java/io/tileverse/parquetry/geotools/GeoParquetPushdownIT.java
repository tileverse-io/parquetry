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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.api.filter.FilterFactory;
import org.geotools.factory.CommonFactoryFinder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.ParquetDatasetCatalog;
import io.tileverse.parquetry.io.LocalFileSource;
import io.tileverse.parquetry.testkit.TestCorpus;

class GeoParquetPushdownIT {

    private static final FilterFactory FF = CommonFactoryFinder.getFilterFactory();

    private static GeoParquetDataStore store(Path dir) throws Exception {
        Path file = TestCorpus.extractFile("geoparquet/examples/example.parquet", dir);
        ParquetDatasetCatalog catalog = ParquetDatasetCatalog.open(
                LocalFileSource.file(file),
                CatalogOptions.builder().datasetName("example").build());
        return new GeoParquetDataStore(catalog);
    }

    private static int count(FeatureReader<SimpleFeatureType, SimpleFeature> reader) throws Exception {
        int n = 0;
        try (reader) {
            while (reader.hasNext()) {
                reader.next();
                n++;
            }
        }
        return n;
    }

    private static int bruteForceCount(GeoParquetFeatureSource fs, Filter filter) throws Exception {
        int n = 0;
        try (FeatureReader<SimpleFeatureType, SimpleFeature> all = fs.getReader(new Query("example"))) {
            while (all.hasNext()) {
                if (filter.evaluate(all.next())) {
                    n++;
                }
            }
        }
        return n;
    }

    @Test
    void attributeFilterMatchesBruteForceCount(@TempDir Path dir) throws Exception {
        try (GeoParquetDataStore store = store(dir)) {
            GeoParquetFeatureSource fs = (GeoParquetFeatureSource) store.getFeatureSource("example");
            Filter attribute = FF.equals(FF.property("continent"), FF.literal("Africa"));
            int pushed = count(fs.getReader(new Query("example", attribute)));
            assertThat(pushed).isEqualTo(bruteForceCount(fs, attribute)).isPositive();
        }
    }

    @Test
    void countForPushableFilterMatchesReaderCount(@TempDir Path dir) throws Exception {
        try (GeoParquetDataStore store = store(dir)) {
            GeoParquetFeatureSource fs = (GeoParquetFeatureSource) store.getFeatureSource("example");
            Query q = new Query("example", FF.equals(FF.property("continent"), FF.literal("Africa")));
            int counted = fs.getCount(q);
            int read = count(fs.getReader(q));
            assertThat(counted).isEqualTo(read).isPositive();
        }
    }

    @Test
    void bboxFilterMatchesBruteForce(@TempDir Path dir) throws Exception {
        try (GeoParquetDataStore store = store(dir)) {
            GeoParquetFeatureSource fs = (GeoParquetFeatureSource) store.getFeatureSource("example");
            String geom = fs.getSchema().getGeometryDescriptor().getLocalName();
            Filter bbox = FF.bbox(geom, -20, 0, 60, 40, null);

            int pushed = count(fs.getReader(new Query("example", bbox)));
            assertThat(pushed).isEqualTo(bruteForceCount(fs, bbox));
        }
    }

    @Test
    void intersectsFilterMatchesBruteForce(@TempDir Path dir) throws Exception {
        try (GeoParquetDataStore store = store(dir)) {
            GeoParquetFeatureSource fs = (GeoParquetFeatureSource) store.getFeatureSource("example");
            String geom = fs.getSchema().getGeometryDescriptor().getLocalName();
            GeometryFactory gf = new GeometryFactory();
            Polygon q = gf.createPolygon(new Coordinate[] {
                new Coordinate(-20, 0),
                new Coordinate(60, 0),
                new Coordinate(60, 40),
                new Coordinate(-20, 40),
                new Coordinate(-20, 0)
            });
            Filter intersects = FF.intersects(FF.property(geom), FF.literal(q));

            int pushed = count(fs.getReader(new Query("example", intersects)));
            assertThat(pushed).isEqualTo(bruteForceCount(fs, intersects)).isPositive();
        }
    }

    @Test
    void retypeReturnsOnlyRequestedAttributes(@TempDir Path dir) throws Exception {
        try (GeoParquetDataStore store = store(dir)) {
            GeoParquetFeatureSource fs = (GeoParquetFeatureSource) store.getFeatureSource("example");
            String geom = fs.getSchema().getGeometryDescriptor().getLocalName();
            Query q = new Query("example");
            q.setPropertyNames(new String[] {geom, "name"});
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = fs.getReader(q)) {
                assertThat(reader.getFeatureType().getAttributeCount()).isEqualTo(2);
                assertThat(reader.hasNext()).isTrue();
                SimpleFeature f = reader.next();
                assertThat(f.getAttribute("continent")).isNull();
                assertThat(f.getAttribute("name")).isNotNull();
            }
        }
    }

    @Test
    void mixedSpatialAndLikeMatchesBruteForce(@TempDir Path dir) throws Exception {
        try (GeoParquetDataStore store = store(dir)) {
            GeoParquetFeatureSource fs = (GeoParquetFeatureSource) store.getFeatureSource("example");
            String geom = fs.getSchema().getGeometryDescriptor().getLocalName();
            Filter mixed = FF.and(FF.bbox(geom, -180, -90, 180, 90, null), FF.like(FF.property("name"), "*a*"));

            int pushed = count(fs.getReader(new Query("example", mixed)));
            assertThat(pushed).isEqualTo(bruteForceCount(fs, mixed)).isPositive();
        }
    }

    @Test
    void maxFeaturesCapsWithResidual(@TempDir Path dir) throws Exception {
        try (GeoParquetDataStore store = store(dir)) {
            GeoParquetFeatureSource fs = (GeoParquetFeatureSource) store.getFeatureSource("example");
            Query q = new Query("example", FF.like(FF.property("name"), "*a*"));
            q.setMaxFeatures(2);
            assertThat(count(fs.getReader(q))).isEqualTo(2);
        }
    }

    @Test
    void countWithMaxFeaturesEqualsReaderCount(@TempDir Path dir) throws Exception {
        try (GeoParquetDataStore store = store(dir)) {
            GeoParquetFeatureSource fs = (GeoParquetFeatureSource) store.getFeatureSource("example");
            Query q = new Query("example", FF.equals(FF.property("continent"), FF.literal("Africa")));
            q.setMaxFeatures(1);
            assertThat(fs.getCount(q)).isEqualTo(count(fs.getReader(q))).isEqualTo(1);
        }
    }

    @Test
    void unlimitedMaxFeaturesCountsAll(@TempDir Path dir) throws Exception {
        try (GeoParquetDataStore store = store(dir)) {
            GeoParquetFeatureSource fs = (GeoParquetFeatureSource) store.getFeatureSource("example");
            Query q = new Query("example", FF.equals(FF.property("continent"), FF.literal("Africa")));
            q.setMaxFeatures(-1); // GeoTools "unlimited" sentinel
            assertThat(fs.getCount(q)).isEqualTo(count(fs.getReader(q))).isEqualTo(2);
        }
    }

    @Test
    void pagingReturnsCorrectWindow(@TempDir Path dir) throws Exception {
        try (GeoParquetDataStore store = store(dir)) {
            GeoParquetFeatureSource fs = (GeoParquetFeatureSource) store.getFeatureSource("example");
            // *a* matches 4 of 5; offset 1, limit 2 -> the 2nd and 3rd matches.
            Query q = new Query("example", FF.like(FF.property("name"), "*a*"));
            // GeoTools applies offset via a sort-based pager that requires serializable attribute bindings; restrict
            // to scalar properties (the nested bbox struct binds to a non-serializable Map and is not paged here).
            q.setPropertyNames("name", "geometry");
            q.setStartIndex(1);
            q.setMaxFeatures(2);
            assertThat(count(fs.getReader(q))).isEqualTo(2);
        }
    }

    @Test
    void featureIdComesFromConfiguredColumn(@TempDir Path dir) throws Exception {
        try (GeoParquetDataStore store = store(dir)) {
            store.setFidColumn("name");
            GeoParquetFeatureSource fs = (GeoParquetFeatureSource) store.getFeatureSource("example");

            String someName;
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = fs.getReader(new Query("example"))) {
                assertThat(reader.hasNext()).isTrue();
                SimpleFeature first = reader.next();
                someName = (String) first.getAttribute("name");
                assertThat(first.getID()).isEqualTo(someName);
            }

            Query byId = new Query("example", FF.id(Set.of(FF.featureId(someName))));
            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = fs.getReader(byId)) {
                assertThat(reader.hasNext()).isTrue();
                SimpleFeature match = reader.next();
                assertThat(match.getID()).isEqualTo(someName);
                assertThat(match.getAttribute("name")).isEqualTo(someName);
                assertThat(reader.hasNext()).isFalse();
            }
        }
    }

    @Test
    void idFilterWithoutFeatureIdColumnIsRejected(@TempDir Path dir) throws Exception {
        try (GeoParquetDataStore store = store(dir)) {
            GeoParquetFeatureSource fs = (GeoParquetFeatureSource) store.getFeatureSource("example");
            Query byId = new Query("example", FF.id(Set.of(FF.featureId("0"))));
            assertThatThrownBy(() -> fs.getReader(byId)).isInstanceOf(IOException.class);
        }
    }

    @Test
    void configuredFeatureIdColumnMustExist(@TempDir Path dir) throws Exception {
        try (GeoParquetDataStore store = store(dir)) {
            store.setFidColumn("no_such_column");
            GeoParquetFeatureSource fs = (GeoParquetFeatureSource) store.getFeatureSource("example");
            Query all = new Query("example");
            // ContentFeatureSource resolves the schema eagerly and wraps the build failure in a RuntimeException.
            assertThatThrownBy(() -> fs.getReader(all))
                    .hasMessageContaining("no_such_column")
                    .hasCauseInstanceOf(IOException.class);
        }
    }

    @Test
    void pagingCountEqualsReaderCount(@TempDir Path dir) throws Exception {
        try (GeoParquetDataStore store = store(dir)) {
            GeoParquetFeatureSource fs = (GeoParquetFeatureSource) store.getFeatureSource("example");
            Query q = new Query("example", FF.equals(FF.property("continent"), FF.literal("Africa")));
            // GeoTools applies offset via a sort-based pager that requires serializable attribute bindings; restrict
            // to scalar properties (the nested bbox struct binds to a non-serializable Map and is not paged here).
            q.setPropertyNames("continent", "geometry");
            q.setStartIndex(1); // Africa matches 2; offset 1 -> 1 remaining
            assertThat(fs.getCount(q)).isEqualTo(count(fs.getReader(q))).isEqualTo(1);
        }
    }
}
