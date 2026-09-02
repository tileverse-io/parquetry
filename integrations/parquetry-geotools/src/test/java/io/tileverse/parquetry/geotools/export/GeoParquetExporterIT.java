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

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.api.referencing.crs.CoordinateReferenceSystem;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.referencing.CRS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.geotools.data.CatalogDataStore;
import io.tileverse.parquetry.geotools.parquet.GeoParquetDataStore;
import io.tileverse.parquetry.io.LocalFileSource;

/**
 * Round-trips a {@link FeatureCollection} through {@link GeoParquetExporter} and back through
 * {@link GeoParquetDataStore}, over the flat (non-nested) attribute bindings the reader table supports.
 */
class GeoParquetExporterIT {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    @Test
    void flatTypesRoundTrip(@TempDir Path dir) throws Exception {
        SimpleFeatureType featureType = flatFeatureType();
        List<SimpleFeature> sourceFeatures = flatFeatures(featureType);
        Path file = writeToFile(dir, featureType, sourceFeatures);

        try (CatalogDataStore store = openStore(file)) {
            ContentFeatureSource fs = (ContentFeatureSource) store.getFeatureSource("features");
            SimpleFeatureType readType = fs.getSchema();

            assertBindingsMatch(featureType, readType);
            assertThat(bindingOf(readType, "geom"))
                    .as("a geometry attribute always reads back bound to the generic Geometry type")
                    .isEqualTo(Geometry.class);
            assertThat(CRS.equalsIgnoreMetadata(readType.getCoordinateReferenceSystem(), CRS.decode("EPSG:4326", true)))
                    .as("the geometry attribute CRS decodes as EPSG:4326")
                    .isTrue();

            Map<Integer, SimpleFeature> readByIntegerKey = readAllByIntegerKey(fs);
            assertThat(readByIntegerKey).hasSize(sourceFeatures.size());
            for (SimpleFeature sourceFeature : sourceFeatures) {
                Integer key = (Integer) sourceFeature.getAttribute("integer");
                SimpleFeature readFeature = readByIntegerKey.get(key);
                assertThat(readFeature).as("row %d was read back", key).isNotNull();
                assertFlatFeatureEquals(sourceFeature, readFeature);
            }

            SimpleFeature nullRowSource = sourceFeatures.get(1);
            SimpleFeature nullRowRead = readByIntegerKey.get((Integer) nullRowSource.getAttribute("integer"));
            assertThat(nullRowRead.getAttribute("string"))
                    .as("a null attribute round-trips as null, not a default value")
                    .isNull();
            assertThat(nullRowRead.getAttribute("uuid")).isNull();
            assertThat(nullRowRead.getAttribute("instant")).isNull();
            assertThat(nullRowRead.getAttribute("localDateTime")).isNull();
        }
    }

    /** Compares every attribute of {@code expected} against {@code actual}; the geometry compares via equalsExact. */
    private static void assertFlatFeatureEquals(SimpleFeature expected, SimpleFeature actual) {
        Point expectedGeometry = (Point) expected.getDefaultGeometry();
        Point actualGeometry = (Point) actual.getDefaultGeometry();
        assertThat(actualGeometry.equalsExact(expectedGeometry))
                .as("geometry for row %s", expected.getID())
                .isTrue();

        assertThat(actual.getAttribute("string")).isEqualTo(expected.getAttribute("string"));
        assertThat(actual.getAttribute("longVal")).isEqualTo(expected.getAttribute("longVal"));
        assertThat(actual.getAttribute("floatVal")).isEqualTo(expected.getAttribute("floatVal"));
        assertThat(actual.getAttribute("doubleVal")).isEqualTo(expected.getAttribute("doubleVal"));
        assertThat(actual.getAttribute("boolVal")).isEqualTo(expected.getAttribute("boolVal"));
        assertThat(actual.getAttribute("blob")).isEqualTo(expected.getAttribute("blob"));
        assertThat(actual.getAttribute("uuid")).isEqualTo(expected.getAttribute("uuid"));
        assertThat(actual.getAttribute("date")).isEqualTo(expected.getAttribute("date"));
        assertThat(actual.getAttribute("instant")).isEqualTo(expected.getAttribute("instant"));
        assertThat(actual.getAttribute("localDateTime")).isEqualTo(expected.getAttribute("localDateTime"));
    }

    @Test
    void widenedBindingsRoundTripAsWidenedTypes(@TempDir Path dir) throws Exception {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("widened");
        typeBuilder.setCRS(CRS.decode("EPSG:4326", true));
        typeBuilder.add("geom", Point.class);
        typeBuilder.add("shortVal", Short.class);
        typeBuilder.add("utilDate", java.util.Date.class);
        typeBuilder.add("sqlDate", java.sql.Date.class);
        typeBuilder.add("decimalVal", BigDecimal.class);
        SimpleFeatureType featureType = typeBuilder.buildFeatureType();

        Instant utilDateInstant = Instant.parse("2024-03-10T08:15:30.123Z");
        LocalDate sqlDateValue = LocalDate.of(2023, 11, 5);

        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);
        featureBuilder.add(point(1, 1));
        featureBuilder.add((short) 7);
        featureBuilder.add(java.util.Date.from(utilDateInstant));
        featureBuilder.add(java.sql.Date.valueOf(sqlDateValue));
        featureBuilder.add(new BigDecimal("2.5"));
        SimpleFeature sourceFeature = featureBuilder.buildFeature("f-0");

        Path file = writeToFile(dir, featureType, List.of(sourceFeature));

        try (CatalogDataStore store = openStore(file)) {
            ContentFeatureSource fs = (ContentFeatureSource) store.getFeatureSource("features");
            SimpleFeatureType readType = fs.getSchema();

            assertThat(bindingOf(readType, "shortVal")).isEqualTo(Integer.class);
            assertThat(bindingOf(readType, "utilDate")).isEqualTo(Instant.class);
            assertThat(bindingOf(readType, "sqlDate")).isEqualTo(LocalDate.class);
            assertThat(bindingOf(readType, "decimalVal")).isEqualTo(Double.class);

            SimpleFeature readFeature = readOnlyFeature(fs);
            assertThat(readFeature.getAttribute("shortVal")).isEqualTo(7);
            assertThat(readFeature.getAttribute("utilDate")).isEqualTo(utilDateInstant);
            assertThat(readFeature.getAttribute("sqlDate")).isEqualTo(sqlDateValue);
            assertThat(readFeature.getAttribute("decimalVal")).isEqualTo(2.5);
        }
    }

    @Test
    void nullCrsWritesAndReadsWithDefault(@TempDir Path dir) throws Exception {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("noCrs");
        typeBuilder.add("geom", Point.class);
        typeBuilder.add("label", String.class);
        SimpleFeatureType featureType = typeBuilder.buildFeatureType();

        SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);
        featureBuilder.add(point(3, 4));
        featureBuilder.add("no-crs-feature");
        SimpleFeature sourceFeature = featureBuilder.buildFeature("f-0");

        Path file = writeToFile(dir, featureType, List.of(sourceFeature));

        try (CatalogDataStore store = openStore(file)) {
            ContentFeatureSource fs = (ContentFeatureSource) store.getFeatureSource("features");
            CoordinateReferenceSystem readCrs = fs.getSchema().getCoordinateReferenceSystem();

            assertThat(readCrs).isNotNull();
            assertThat(CRS.equalsIgnoreMetadata(readCrs, CRS.decode("EPSG:4326", true)))
                    .as("a geometry attribute with no source CRS defaults to WGS84 on read")
                    .isTrue();
        }
    }

    @Test
    void emptyCollectionWritesValidEmptyFile(@TempDir Path dir) throws Exception {
        SimpleFeatureType featureType = flatFeatureType();
        Path file = writeToFile(dir, featureType, List.of());

        try (CatalogDataStore store = openStore(file)) {
            ContentFeatureSource fs = (ContentFeatureSource) store.getFeatureSource("features");
            SimpleFeatureType readType = fs.getSchema();

            assertBindingsMatch(featureType, readType);
            assertThat(fs.getCount(Query.ALL)).isZero();

            try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = fs.getReader(Query.ALL)) {
                assertThat(reader.hasNext()).isFalse();
            }
        }
    }

    @Test
    void doesNotCloseTheOutputStream() throws Exception {
        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        CloseTrackingOutputStream out = new CloseTrackingOutputStream(sink);
        SimpleFeatureType featureType = flatFeatureType();
        FeatureCollection<SimpleFeatureType, SimpleFeature> features =
                new ListFeatureCollection(featureType, flatFeatures(featureType));

        GeoParquetExporter.export(features, out, WriteOptions.defaults());

        assertThat(out.closeCount)
                .as("the writer must not close the caller-owned output stream")
                .isZero();
        assertThat(sink.size()).as("the writer still produced output").isGreaterThan(0);
    }

    /**
     * The WFS output-format flow: features READ from a parquetry store (whose WKB decoding produces 2D packed
     * coordinate sequences) export again with their geometries intact.
     */
    @Test
    void reExportsFeaturesReadFromAStore(@TempDir Path dir) throws Exception {
        SimpleFeatureType featureType = flatFeatureType();
        List<SimpleFeature> sourceFeatures = flatFeatures(featureType);
        Path first = writeToFile(dir, featureType, sourceFeatures);

        Path second = dir.resolve("re-export.parquet");
        try (CatalogDataStore store = openStore(first)) {
            ContentFeatureSource fs = (ContentFeatureSource) store.getFeatureSource("features");
            try (OutputStream out = Files.newOutputStream(second)) {
                GeoParquetExporter.export(fs.getFeatures(), out, WriteOptions.defaults());
            }
        }

        try (CatalogDataStore store = openStore(second)) {
            ContentFeatureSource fs = (ContentFeatureSource) store.getFeatureSource("features");
            Map<Integer, SimpleFeature> readByIntegerKey = readAllByIntegerKey(fs);
            assertThat(readByIntegerKey).hasSize(sourceFeatures.size());
            for (SimpleFeature sourceFeature : sourceFeatures) {
                Point sourceGeometry = (Point) sourceFeature.getDefaultGeometry();
                SimpleFeature readFeature = readByIntegerKey.get((Integer) sourceFeature.getAttribute("integer"));
                assertThat(readFeature)
                        .as("row %s was re-exported", sourceFeature.getID())
                        .isNotNull();
                Point reReadGeometry = (Point) readFeature.getDefaultGeometry();
                assertThat(reReadGeometry.equalsExact(sourceGeometry))
                        .as("geometry for row %s survives the store-read re-export", sourceFeature.getID())
                        .isTrue();
            }
        }
    }

    private static Path writeToFile(Path dir, SimpleFeatureType featureType, List<SimpleFeature> sourceFeatures)
            throws IOException {
        FeatureCollection<SimpleFeatureType, SimpleFeature> features =
                new ListFeatureCollection(featureType, sourceFeatures);
        Path file = dir.resolve("features.parquet");
        try (OutputStream out = Files.newOutputStream(file)) {
            GeoParquetExporter.export(features, out, WriteOptions.defaults());
        }
        return file;
    }

    private static CatalogDataStore openStore(Path file) {
        FilesetCatalog catalog = FilesetCatalog.open(
                LocalFileSource.file(file),
                CatalogOptions.builder().datasetName("features").build());
        return new GeoParquetDataStore(catalog);
    }

    private static SimpleFeature readOnlyFeature(ContentFeatureSource fs) throws IOException {
        try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = fs.getReader(Query.ALL)) {
            assertThat(reader.hasNext()).isTrue();
            SimpleFeature feature = reader.next();
            assertThat(reader.hasNext()).isFalse();
            return feature;
        }
    }

    private static Map<Integer, SimpleFeature> readAllByIntegerKey(ContentFeatureSource fs) throws IOException {
        Map<Integer, SimpleFeature> byKey = new HashMap<>();
        try (FeatureReader<SimpleFeatureType, SimpleFeature> reader = fs.getReader(Query.ALL)) {
            while (reader.hasNext()) {
                SimpleFeature feature = reader.next();
                byKey.put((Integer) feature.getAttribute("integer"), feature);
            }
        }
        return byKey;
    }

    /**
     * Asserts every non-geometry attribute reads back with the exact same Java binding it was written with. A geometry
     * attribute is skipped: the reader always widens it to {@link Geometry}, regardless of the concrete JTS type it was
     * written with.
     */
    private static void assertBindingsMatch(SimpleFeatureType sourceType, SimpleFeatureType readType) {
        for (AttributeDescriptor sourceDescriptor : sourceType.getAttributeDescriptors()) {
            Class<?> sourceBinding = sourceDescriptor.getType().getBinding();
            if (Geometry.class.isAssignableFrom(sourceBinding)) {
                continue;
            }
            String name = sourceDescriptor.getLocalName();
            assertThat(bindingOf(readType, name))
                    .as("attribute '%s' binding round-trips", name)
                    .isEqualTo(sourceBinding);
        }
    }

    private static Class<?> bindingOf(SimpleFeatureType featureType, String attributeName) {
        return featureType.getDescriptor(attributeName).getType().getBinding();
    }

    private static SimpleFeatureType flatFeatureType() throws Exception {
        SimpleFeatureTypeBuilder typeBuilder = new SimpleFeatureTypeBuilder();
        typeBuilder.setName("flat");
        typeBuilder.setCRS(CRS.decode("EPSG:4326", true));
        typeBuilder.add("geom", Point.class);
        typeBuilder.add("string", String.class);
        typeBuilder.add("integer", Integer.class);
        typeBuilder.add("longVal", Long.class);
        typeBuilder.add("floatVal", Float.class);
        typeBuilder.add("doubleVal", Double.class);
        typeBuilder.add("boolVal", Boolean.class);
        typeBuilder.add("blob", byte[].class);
        typeBuilder.add("uuid", UUID.class);
        typeBuilder.add("date", LocalDate.class);
        typeBuilder.add("instant", Instant.class);
        typeBuilder.add("localDateTime", LocalDateTime.class);
        return typeBuilder.buildFeatureType();
    }

    /** Three features; the middle row (index 1) leaves every attribute but {@code geom} and {@code integer} null. */
    private static List<SimpleFeature> flatFeatures(SimpleFeatureType featureType) {
        List<SimpleFeature> features = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            SimpleFeatureBuilder builder = new SimpleFeatureBuilder(featureType);
            boolean nullRow = i == 1;
            builder.add(point(i, i));
            builder.add(nullRow ? null : "name-" + i);
            builder.add(i);
            builder.add(nullRow ? null : 1000L + i);
            builder.add(nullRow ? null : 1.5f + i);
            builder.add(nullRow ? null : 2.5 + i);
            builder.add(nullRow ? null : i % 2 == 0);
            builder.add(nullRow ? null : new byte[] {(byte) i, (byte) (i + 1)});
            builder.add(nullRow ? null : new UUID(0L, i));
            builder.add(nullRow ? null : LocalDate.of(2020, 1, i + 1));
            builder.add(nullRow ? null : Instant.ofEpochSecond(1_700_000_000L + i, i * 1000L));
            builder.add(nullRow ? null : LocalDateTime.of(2021, 6, 15, 10, 30, i, i * 1000));
            features.add(builder.buildFeature("f-" + i));
        }
        return features;
    }

    private static Point point(double x, double y) {
        return GEOMETRY_FACTORY.createPoint(new Coordinate(x, y));
    }

    /** Records how many times {@link #close()} is called, without ever closing the delegate. */
    private static final class CloseTrackingOutputStream extends FilterOutputStream {

        private int closeCount;

        CloseTrackingOutputStream(OutputStream delegate) {
            super(delegate);
        }

        @Override
        public void close() throws IOException {
            closeCount++;
        }
    }
}
