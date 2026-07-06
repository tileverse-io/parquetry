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
package io.tileverse.parquetry.geotools.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.Query;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.FilterFactory;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Reads the vendored Iceberg geo testbed through the {@link IcebergDataStore} to exercise the full read path: geometry
 * schema and CRS, unfiltered count, bbox pushdown, record-level spatial refinement, projection, an attribute-only
 * table, and merge-on-read position deletes. Spatial filters are built in the layer's native CRS, matching the store's
 * contract that a spatial predicate arrives in the dataset CRS.
 */
class IcebergDataStoreIT {

    private static final FilterFactory FF = CommonFactoryFinder.getFilterFactory();

    @TempDir
    Path tempDir;

    private DataStore openV3Geometry() throws IOException {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve("corpus"));
        return new IcebergDataStoreFactory()
                .createDataStore(
                        Map.of("iceberg", root.resolve("v3_geometry").toUri().toString()));
    }

    @Test
    void schemaHasGeometryAttributeWithWgs84() throws IOException {
        DataStore store = openV3Geometry();
        try {
            SimpleFeatureType schema = store.getSchema("v3_geometry");
            assertThat(schema.getGeometryDescriptor()).isNotNull();
            assertThat(schema.getGeometryDescriptor().getType().getBinding()).isEqualTo(Geometry.class);
            CoordinateReferenceSystem crs = schema.getCoordinateReferenceSystem();
            assertThat(CRS.equalsIgnoreMetadata(crs, DefaultGeographicCRS.WGS84))
                    .isTrue();
        } finally {
            store.dispose();
        }
    }

    @Test
    void unfilteredCountReadsEveryRow() throws IOException {
        DataStore store = openV3Geometry();
        try {
            SimpleFeatureSource source = store.getFeatureSource("v3_geometry");
            assertThat(source.getCount(Query.ALL)).isEqualTo(10_000);
        } finally {
            store.dispose();
        }
    }

    @Test
    void bboxOverCaliforniaExtentReadsThePrunedFilesRows() throws IOException {
        DataStore store = openV3Geometry();
        try {
            SimpleFeatureSource source = store.getFeatureSource("v3_geometry");
            // The window equals the california file's bbox. Manifest pruning keeps exactly that one file (1000 of
            // 10000 rows), and every surviving row falls inside the window.
            Query query = bboxQuery(source, window(source, -125, 32, -115, 42));
            assertThat(source.getFeatures(query).size()).isEqualTo(1000);
        } finally {
            store.dispose();
        }
    }

    @Test
    void recordLevelFilterDropsRowsInsideTheSurvivingFile() throws IOException {
        DataStore store = openV3Geometry();
        try {
            SimpleFeatureSource source = store.getFeatureSource("v3_geometry");
            ReferencedEnvelope insideCalifornia = window(source, -123, 34, -119, 38);
            long bruteForce = countGeometriesIntersecting(source, insideCalifornia);
            int pushed = source.getFeatures(bboxQuery(source, insideCalifornia)).size();
            assertThat(pushed).isEqualTo((int) bruteForce);
            // The window is strictly inside the california file's bbox and the corpus is byte-reproducible, making 156
            // external ground truth that also catches a whole-store systematic under-read depressing both counts alike.
            assertThat(pushed).isEqualTo(156);
            assertThat(pushed).isLessThan(1000);
        } finally {
            store.dispose();
        }
    }

    @Test
    void projectionNarrowsTheFeatureType() throws IOException {
        DataStore store = openV3Geometry();
        try {
            Query query = new Query("v3_geometry");
            query.setPropertyNames("id");
            SimpleFeatureCollection features =
                    store.getFeatureSource("v3_geometry").getFeatures(query);
            assertThat(features.getSchema().getAttributeCount()).isEqualTo(1);
            assertThat(features.getSchema().getDescriptor("id")).isNotNull();
        } finally {
            store.dispose();
        }
    }

    @Test
    void attributeOnlyTableIsServedWithoutGeometry() throws IOException {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve("corpus"));
        DataStore store = new IcebergDataStoreFactory()
                .createDataStore(
                        Map.of("iceberg", root.resolve("v3_minimal").toUri().toString()));
        try {
            assertThat(store.getSchema("v3_minimal").getGeometryDescriptor()).isNull();
        } finally {
            store.dispose();
        }
    }

    @Test
    void mergeOnReadPositionDeletesHideDeletedRows() throws IOException {
        Path table = TestCorpus.extractDirectory("iceberg-deletes/positional", tempDir.resolve("positional"));
        DataStore store = new IcebergDataStoreFactory()
                .createDataStore(Map.of("iceberg", table.toUri().toString()));
        try {
            String typeName = store.getTypeNames()[0];
            assertThat(store.getFeatureSource(typeName).getCount(Query.ALL)).isEqualTo(890);
        } finally {
            store.dispose();
        }
    }

    private static ReferencedEnvelope window(
            SimpleFeatureSource source, double minX, double minY, double maxX, double maxY) {
        CoordinateReferenceSystem nativeCrs = source.getSchema().getCoordinateReferenceSystem();
        return new ReferencedEnvelope(minX, maxX, minY, maxY, nativeCrs);
    }

    private static Query bboxQuery(SimpleFeatureSource source, ReferencedEnvelope window) {
        SimpleFeatureType schema = source.getSchema();
        String geomName = schema.getGeometryDescriptor().getLocalName();
        return new Query(schema.getTypeName(), FF.bbox(FF.property(geomName), window));
    }

    /**
     * Brute-force oracle: reads every feature unfiltered and counts geometries whose envelope intersects the query
     * window, mirroring the pushed-down BboxIntersects intent independent of the pushdown path being tested.
     */
    private static long countGeometriesIntersecting(SimpleFeatureSource source, ReferencedEnvelope window)
            throws IOException {
        long matches = 0;
        try (SimpleFeatureIterator features = source.getFeatures(Query.ALL).features()) {
            while (features.hasNext()) {
                Geometry geometry = (Geometry) features.next().getDefaultGeometry();
                if (geometry != null && window.intersects(geometry.getEnvelopeInternal())) {
                    matches++;
                }
            }
        }
        return matches;
    }
}
