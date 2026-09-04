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
package io.tileverse.parquetry.geotools.export;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.stream.Stream;

import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.geo.MemorySegmentWkbReader;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystems;

/**
 * Parquet stores geometry coordinates easting-first whatever the CRS declares. A latitude-first collection is
 * reprojected before it is encoded. These tests pin the stored ordinates for both source orders.
 */
class EastingFirstFeaturesTest {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    private static final double LONGITUDE = 10;
    private static final double LATITUDE = 50;

    @Test
    void latitudeFirstSourceIsStoredLongitudeFirst() throws Exception {
        CoordinateReferenceSystem authorityOrder = CRS.decode("EPSG:4326");
        assertThat(CRS.getAxisOrder(authorityOrder)).isEqualTo(CRS.AxisOrder.NORTH_EAST);

        Point stored = exportSinglePoint(authorityOrder, new Coordinate(LATITUDE, LONGITUDE));

        assertThat(stored.getX()).isEqualTo(LONGITUDE);
        assertThat(stored.getY()).isEqualTo(LATITUDE);
    }

    @Test
    void longitudeFirstSourceIsStoredUnchanged() throws Exception {
        CoordinateReferenceSystem longitudeFirst = CRS.decode("EPSG:4326", true);
        assertThat(CRS.getAxisOrder(longitudeFirst)).isEqualTo(CRS.AxisOrder.EAST_NORTH);

        Point stored = exportSinglePoint(longitudeFirst, new Coordinate(LONGITUDE, LATITUDE));

        assertThat(stored.getX()).isEqualTo(LONGITUDE);
        assertThat(stored.getY()).isEqualTo(LATITUDE);
    }

    @Test
    void collectionWithoutCrsIsStoredUnchanged() {
        Point stored = exportSinglePoint(null, new Coordinate(LONGITUDE, LATITUDE));

        assertThat(stored.getX()).isEqualTo(LONGITUDE);
        assertThat(stored.getY()).isEqualTo(LATITUDE);
    }

    @Test
    void latitudeFirstSourceCollectionIsNotMutated() throws Exception {
        SimpleFeatureType featureType = pointType(CRS.decode("EPSG:4326"));
        Coordinate authored = new Coordinate(LATITUDE, LONGITUDE);
        FeatureCollection<SimpleFeatureType, SimpleFeature> features = singlePointCollection(featureType, authored);

        readStoredPoint(features, featureType);

        try (FeatureIterator<SimpleFeature> iterator = features.features()) {
            Point source = (Point) iterator.next().getDefaultGeometry();
            assertThat(source.getX()).isEqualTo(LATITUDE);
            assertThat(source.getY()).isEqualTo(LONGITUDE);
        }
    }

    @Test
    void declaredCrsIsLongitudeFirstForBothSourceOrders() throws Exception {
        assertDeclaresCrs84(CRS.decode("EPSG:4326"));
        assertDeclaresCrs84(CRS.decode("EPSG:4326", true));
    }

    /**
     * The geo metadata and the write options must name the same CRS: a reader takes the column's CRS from the geo
     * metadata, while the footer takes it from the write options.
     */
    private static void assertDeclaresCrs84(CoordinateReferenceSystem sourceCrs) {
        FeatureRecordBatches recordBatches = FeatureRecordBatches.forType(pointType(sourceCrs));

        io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem declared = recordBatches
                .geoMetadata()
                .orElseThrow()
                .columns()
                .get("geom")
                .crs()
                .orElseThrow();
        io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem inWriteOptions = recordBatches
                .withGeometryCrs(WriteOptions.builder().build())
                .crs()
                .get("geom");

        assertThat(declared).isEqualTo(CoordinateReferenceSystems.ogcCrs84());
        assertThat(inWriteOptions).isEqualTo(declared);
    }

    /** Exports one point authored at {@code authored} in {@code crs} and returns the point as stored in the WKB. */
    private static Point exportSinglePoint(CoordinateReferenceSystem crs, Coordinate authored) {
        SimpleFeatureType featureType = pointType(crs);
        return readStoredPoint(singlePointCollection(featureType, authored), featureType);
    }

    private static Point readStoredPoint(
            FeatureCollection<SimpleFeatureType, SimpleFeature> features, SimpleFeatureType featureType) {
        FeatureRecordBatches recordBatches = FeatureRecordBatches.forType(featureType);
        try (Stream<ParquetRecordBatch> batches = recordBatches.batches(features, 10)) {
            List<ParquetRecordBatch> collected = batches.toList();
            ParquetRecord record = collected.get(0).materialize(0);
            byte[] wkb = record.getBinary(ColumnPath.of("geom"));
            return (Point) new MemorySegmentWkbReader().read(MemorySegment.ofArray(wkb));
        }
    }

    private static SimpleFeatureType pointType(CoordinateReferenceSystem crs) {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("t");
        typeBuilder.setCRS(crs);
        typeBuilder.add("geom", Point.class);
        return typeBuilder.buildFeatureType();
    }

    private static FeatureCollection<SimpleFeatureType, SimpleFeature> singlePointCollection(
            SimpleFeatureType featureType, Coordinate authored) {
        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);
        featureBuilder.add(GEOMETRY_FACTORY.createPoint(authored));
        SimpleFeature feature = featureBuilder.buildFeature("fid-0");
        return new ListFeatureCollection(featureType, List.of(feature));
    }
}
