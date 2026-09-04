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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.collection.DecoratingFeatureCollection;
import org.geotools.feature.collection.DecoratingFeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.coordinatesequence.CoordinateSequences;
import org.geotools.referencing.CRS;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.geo.MemorySegmentWkbReader;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystems;

class FeatureRecordBatchesTest {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    @Test
    void freezesOnRowTarget() throws Exception {
        SimpleFeatureType featureType = buildFeatureType();
        FeatureCollection<SimpleFeatureType, SimpleFeature> features = buildFeatureCollection(featureType, 25);

        List<ParquetRecordBatch> batches = collectBatches(FeatureRecordBatches.forType(featureType), features, 10);

        assertThat(batches).extracting(ParquetRecordBatch::rowCount).containsExactly(10, 10, 5);
    }

    @Test
    void freezesOnByteBudget() throws Exception {
        SimpleFeatureType featureType = buildFeatureType();
        FeatureCollection<SimpleFeatureType, SimpleFeature> features = buildFeatureCollection(featureType, 5);

        List<ParquetRecordBatch> batches = collectBatches(FeatureRecordBatches.forType(featureType), features, 10, 1L);

        assertThat(batches).hasSize(5);
        assertThat(batches).extracting(ParquetRecordBatch::rowCount).containsOnly(1);
    }

    @Test
    void emptyCollectionYieldsNoBatches() throws Exception {
        SimpleFeatureType featureType = buildFeatureType();
        FeatureCollection<SimpleFeatureType, SimpleFeature> features = buildFeatureCollection(featureType, 0);

        FeatureRecordBatches recordBatches = FeatureRecordBatches.forType(featureType);
        List<ParquetRecordBatch> batches = collectBatches(recordBatches, features, 10);

        assertThat(batches).isEmpty();
        assertThat(recordBatches.parquetSchema()).isNotNull();
        assertThat(recordBatches.geoMetadata()).isPresent();
    }

    @Test
    void nullValuesAuthorAsNulls() throws Exception {
        SimpleFeatureType featureType = buildFeatureType();
        List<SimpleFeature> featureList = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);
            builder.add(i % 3 == 0 ? null : point(i));
            builder.add(i % 2 == 0 ? null : "name-" + i);
            builder.add(i);
            featureList.add(builder.buildFeature("fid-" + i));
        }
        FeatureCollection<SimpleFeatureType, SimpleFeature> features =
                new ListFeatureCollection(featureType, featureList);

        List<ParquetRecordBatch> batches = collectBatches(FeatureRecordBatches.forType(featureType), features, 100);

        assertThat(batches).hasSize(1);
        ParquetRecordBatch batch = batches.get(0);
        assertThat(batch.rowCount()).isEqualTo(6);
        for (int i = 0; i < 6; i++) {
            ParquetRecord record = batch.materialize(i);
            assertThat(record.isNull(ColumnPath.of("geom"))).isEqualTo(i % 3 == 0);
            assertThat(record.isNull(ColumnPath.of("name"))).isEqualTo(i % 2 == 0);
            assertThat(record.getInt(ColumnPath.of("count"))).isEqualTo(i);
        }
    }

    @Test
    void closingTheStreamClosesTheFeatureIterator() throws Exception {
        SimpleFeatureType featureType = buildFeatureType();
        FeatureCollection<SimpleFeatureType, SimpleFeature> baseCollection = buildFeatureCollection(featureType, 3);
        CloseTrackingFeatureCollection features = new CloseTrackingFeatureCollection(baseCollection);

        try (Stream<ParquetRecordBatch> stream =
                FeatureRecordBatches.forType(featureType).batches(features, 10)) {
            stream.count();
            assertThat(features.iteratorClosed).isFalse();
        }

        assertThat(features.iteratorClosed).isTrue();
    }

    @Test
    void rejectsNonPositiveBatchRows() throws Exception {
        SimpleFeatureType featureType = buildFeatureType();
        FeatureCollection<SimpleFeatureType, SimpleFeature> features = buildFeatureCollection(featureType, 1);
        FeatureRecordBatches recordBatches = FeatureRecordBatches.forType(featureType);

        assertThatThrownBy(() -> recordBatches.batches(features, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchRows");
        assertThatThrownBy(() -> recordBatches.batches(features, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("batchRows");
    }

    @Test
    void geometryRoundTripsThroughWkb() throws Exception {
        SimpleFeatureType featureType = buildFeatureType();
        Point sourcePoint = point(7);
        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);
        featureBuilder.add(sourcePoint);
        featureBuilder.add("name-7");
        featureBuilder.add(7);
        SimpleFeature feature = featureBuilder.buildFeature("fid-7");
        FeatureCollection<SimpleFeatureType, SimpleFeature> features =
                new ListFeatureCollection(featureType, List.of(feature));

        List<ParquetRecordBatch> batches = collectBatches(FeatureRecordBatches.forType(featureType), features, 10);

        ParquetRecordBatch batch = batches.get(0);
        ParquetRecord record = batch.materialize(0);
        byte[] wkb = record.getBinary(ColumnPath.of("geom"));
        Point decoded = (Point) new MemorySegmentWkbReader().read(java.lang.foreign.MemorySegment.ofArray(wkb));

        assertThat(decoded.equalsExact(sourcePoint)).isTrue();
    }

    @Test
    void packedTwoDimensionalGeometryEncodesAsTwoDimensionalWkb() throws Exception {
        SimpleFeatureType featureType = buildFeatureType();
        GeometryFactory packedFactory = new GeometryFactory(new PackedCoordinateSequenceFactory());
        Point sourcePoint = packedFactory.createPoint(
                PackedCoordinateSequenceFactory.DOUBLE_FACTORY.create(new double[] {3, 4}, 2));
        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);
        featureBuilder.add(sourcePoint);
        featureBuilder.add("packed");
        featureBuilder.add(1);
        SimpleFeature feature = featureBuilder.buildFeature("fid-packed");
        FeatureCollection<SimpleFeatureType, SimpleFeature> features =
                new ListFeatureCollection(featureType, List.of(feature));

        List<ParquetRecordBatch> batches = collectBatches(FeatureRecordBatches.forType(featureType), features, 10);

        byte[] wkb = batches.get(0).materialize(0).getBinary(ColumnPath.of("geom"));
        Point decoded = (Point) new MemorySegmentWkbReader().read(java.lang.foreign.MemorySegment.ofArray(wkb));
        assertThat(decoded.equalsExact(sourcePoint)).isTrue();
        assertThat(CoordinateSequences.coordinateDimension(decoded))
                .as("a 2D geometry must encode as 2D WKB, not as 3D with a fabricated Z")
                .isEqualTo(2);
    }

    @Test
    void geometryWithZKeepsItsZOrdinate() throws Exception {
        SimpleFeatureType featureType = buildFeatureType();
        GeometryFactory packedFactory = new GeometryFactory(new PackedCoordinateSequenceFactory());
        Point sourcePoint = packedFactory.createPoint(new Coordinate(3, 4, 5));
        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);
        featureBuilder.add(sourcePoint);
        featureBuilder.add("xyz");
        featureBuilder.add(1);
        SimpleFeature feature = featureBuilder.buildFeature("fid-xyz");
        FeatureCollection<SimpleFeatureType, SimpleFeature> features =
                new ListFeatureCollection(featureType, List.of(feature));

        List<ParquetRecordBatch> batches = collectBatches(FeatureRecordBatches.forType(featureType), features, 10);

        byte[] wkb = batches.get(0).materialize(0).getBinary(ColumnPath.of("geom"));
        Point decoded = (Point) new MemorySegmentWkbReader().read(java.lang.foreign.MemorySegment.ofArray(wkb));
        assertThat(decoded.getCoordinate().getZ()).isEqualTo(5.0);
    }

    @Test
    void withGeometryCrsAddsResolvedEpsgForMissingColumn() throws Exception {
        SimpleFeatureType featureType = buildFeatureType();
        FeatureRecordBatches recordBatches = FeatureRecordBatches.forType(featureType);

        WriteOptions rebuilt = recordBatches.withGeometryCrs(WriteOptions.defaults());

        assertThat(rebuilt.crs())
                .containsEntry("geom", CoordinateReferenceSystems.forEpsg(4326).orElseThrow());
    }

    @Test
    void withGeometryCrsFallsBackToCrs84WhenEpsgUnresolved() throws Exception {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("t");
        typeBuilder.add("geom", Point.class);
        SimpleFeatureType featureType = typeBuilder.buildFeatureType();
        FeatureRecordBatches recordBatches = FeatureRecordBatches.forType(featureType);

        WriteOptions rebuilt = recordBatches.withGeometryCrs(WriteOptions.defaults());

        assertThat(rebuilt.crs()).containsEntry("geom", CoordinateReferenceSystems.ogcCrs84());
    }

    @Test
    void withGeometryCrsLeavesOptionsUntouchedWhenAllColumnsCovered() throws Exception {
        SimpleFeatureType featureType = buildFeatureType();
        FeatureRecordBatches recordBatches = FeatureRecordBatches.forType(featureType);
        WriteOptions base = WriteOptions.builder().crsEpsg("geom", 3857).build();

        WriteOptions rebuilt = recordBatches.withGeometryCrs(base);

        assertThat(rebuilt).isSameAs(base);
    }

    private static SimpleFeatureType buildFeatureType() throws Exception {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("t");
        typeBuilder.setCRS(CRS.decode("EPSG:4326", true));
        typeBuilder.add("geom", Point.class);
        typeBuilder.add("name", String.class);
        typeBuilder.add("count", Integer.class);
        return typeBuilder.buildFeatureType();
    }

    private static FeatureCollection<SimpleFeatureType, SimpleFeature> buildFeatureCollection(
            SimpleFeatureType featureType, int count) {
        List<SimpleFeature> featureList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);
            builder.add(point(i));
            builder.add("name-" + i);
            builder.add(i);
            featureList.add(builder.buildFeature("fid-" + i));
        }
        return new ListFeatureCollection(featureType, featureList);
    }

    private static Point point(int ordinate) {
        return GEOMETRY_FACTORY.createPoint(new Coordinate(ordinate, ordinate));
    }

    /** Collects {@code recordBatches.batches(features, batchRows)} into a list, closing the stream afterward. */
    private static List<ParquetRecordBatch> collectBatches(
            FeatureRecordBatches recordBatches,
            FeatureCollection<SimpleFeatureType, SimpleFeature> features,
            int batchRows) {
        try (Stream<ParquetRecordBatch> stream = recordBatches.batches(features, batchRows)) {
            return stream.collect(Collectors.toList());
        }
    }

    /**
     * Collects {@code recordBatches.batches(features, batchRows, maxBatchBytes)} into a list, closing the stream
     * afterward.
     */
    private static List<ParquetRecordBatch> collectBatches(
            FeatureRecordBatches recordBatches,
            FeatureCollection<SimpleFeatureType, SimpleFeature> features,
            int batchRows,
            long maxBatchBytes) {
        try (Stream<ParquetRecordBatch> stream = recordBatches.batches(features, batchRows, maxBatchBytes)) {
            return stream.collect(Collectors.toList());
        }
    }

    /**
     * Wraps a delegate {@link FeatureCollection}, flipping {@link #iteratorClosed} once the iterator returned by
     * {@link #features()} is closed. Used to assert that closing the stream returned by
     * {@link FeatureRecordBatches#batches(FeatureCollection, int)} closes the underlying feature iterator.
     */
    private static final class CloseTrackingFeatureCollection
            extends DecoratingFeatureCollection<SimpleFeatureType, SimpleFeature> {

        private boolean iteratorClosed;

        CloseTrackingFeatureCollection(FeatureCollection<SimpleFeatureType, SimpleFeature> delegate) {
            super(delegate);
        }

        @Override
        public FeatureIterator<SimpleFeature> features() {
            FeatureIterator<SimpleFeature> delegateIterator = super.features();
            return new DecoratingFeatureIterator<>(delegateIterator) {
                @Override
                public void close() {
                    super.close();
                    iteratorClosed = true;
                }
            };
        }
    }
}
