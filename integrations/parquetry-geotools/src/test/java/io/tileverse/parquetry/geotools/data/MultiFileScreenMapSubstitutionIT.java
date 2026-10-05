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
import static org.assertj.core.api.Assertions.within;

import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.geotools.api.data.FeatureReader;
import org.geotools.api.data.Query;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.data.util.ScreenMap;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.geotools.util.factory.Hints;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.geotools.parquet.GeoParquetDataStore;
import io.tileverse.parquetry.io.FileSource;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * End-to-end proof that a dataset of several GeoParquet files decimates whole files: a geometry-only query with the
 * renderer's {@link Hints#SCREENMAP} over a directory of two point files reads the sub-pixel file as one pixel-sized
 * substitute and the other file row by row.
 *
 * <p>Both files hold points on one latitude. The query reads them through an identity world-to-screen transform at one
 * world unit per pixel. File {@code a} holds four points within a third of a unit: its footer box is sub-pixel and
 * every row inside it satisfies the unfiltered query, hence one substitute centred on that box stands for the whole
 * file, which is never opened. File {@code b} spreads twenty points over five world units: neither its footer box nor
 * its single page is sub-pixel, and the per-row gate keeps the first point of each unit.
 *
 * <p>The visit order is ascending by the files' box minimum corner, which puts file {@code a} first, and the reader
 * emits a waiting substitute ahead of the next row: the substitute leads the sequence, followed by the surviving rows
 * of file {@code b} in row order.
 */
class MultiFileScreenMapSubstitutionIT {

    private static final int CELLS = 5;
    private static final int POINTS_PER_CELL = 4;

    @Test
    void aSubPixelFileIsStoodInForWithoutOpeningIt(@TempDir Path dir) throws Exception {
        writePointFile(dir.resolve("a.parquet"), pointsInOneCell(0.0, 10.0));
        writePointFile(dir.resolve("b.parquet"), pointsOverFiveCells(0.0, 15.0));

        try (FilesetCatalog catalog = FilesetCatalog.open(
                        FileSource.directory(dir, "*.parquet"),
                        CatalogOptions.builder().datasetName("points").build());
                CatalogDataStore store = new GeoParquetDataStore(catalog)) {
            CatalogFeatureSource fs = (CatalogFeatureSource) store.getFeatureSource("points");
            Query query = new Query("points");
            query.setPropertyNames(List.of("geometry"));
            query.getHints().put(Hints.SCREENMAP, oneWorldUnitPerPixelScreenMap());

            List<SimpleFeature> features = collect(fs.getReader(query));

            List<Double> xs = features.stream()
                    .map(feature -> ((Geometry) feature.getDefaultGeometry()).getCoordinate().x)
                    .toList();
            assertThat(xs)
                    .as("one substitute for file a, five real rows for file b")
                    .hasSize(6);
            assertThat(xs.get(0))
                    .as("the midpoint of file a's box, where no row lies")
                    .isCloseTo(0.15, within(1e-6));
            assertThat(xs.subList(1, 6))
                    .as("the first row of each cell of file b")
                    .usingElementComparator((left, right) -> Math.abs(left - right) < 1e-6 ? 0 : 1)
                    .containsExactly(0.0, 1.0, 2.0, 3.0, 4.0);
        }
    }

    private static List<SimpleFeature> collect(FeatureReader<SimpleFeatureType, SimpleFeature> reader)
            throws Exception {
        List<SimpleFeature> features = new ArrayList<>();
        try (reader) {
            while (reader.hasNext()) {
                features.add(reader.next());
            }
        }
        return features;
    }

    /**
     * A coarse ScreenMap over the fixture's extent: identity world-to-screen transform and one world unit per pixel. A
     * sub-pixel point at {@code (x, y)} paints the pixel at the truncated coordinates.
     */
    private static ScreenMap oneWorldUnitPerPixelScreenMap() {
        ScreenMap screenMap = new ScreenMap(0, 0, 256, 256);
        AffineTransform2D identity = new AffineTransform2D(1, 0, 0, 1, 0, 0);
        screenMap.setTransform(identity);
        screenMap.setSpans(1.0, 1.0);
        return screenMap;
    }

    /** The points of one world unit, a tenth of a unit apart: a file of these has a sub-pixel footer box. */
    private static List<double[]> pointsInOneCell(double x0, double y) {
        List<double[]> points = new ArrayList<>();
        for (int withinCell = 0; withinCell < POINTS_PER_CELL; withinCell++) {
            points.add(new double[] {x0 + 0.1 * withinCell, y});
        }
        return points;
    }

    /** The points of five adjacent world units: a file of these has a footer box several pixels wide. */
    private static List<double[]> pointsOverFiveCells(double x0, double y) {
        List<double[]> points = new ArrayList<>();
        for (int cell = 0; cell < CELLS; cell++) {
            for (int withinCell = 0; withinCell < POINTS_PER_CELL; withinCell++) {
                points.add(new double[] {x0 + cell + 0.1 * withinCell, y});
            }
        }
        return points;
    }

    /** Writes {@code points} as one GeoParquet file, ids numbered from zero in list order. */
    private static void writePointFile(Path target, List<double[]> points) throws Exception {
        ParquetSchema schema = geometrySchema();
        WriteOptions options = WriteOptions.builder().crsEpsg("geometry", 4326).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(target), schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(points.size());
            long id = 0;
            for (double[] point : points) {
                appendPoint(appender, id, point[0], point[1]);
                id++;
            }
            appender.flush();
        }
    }

    private static void appendPoint(ParquetRecordBatchBuilder appender, long id, double x, double y) {
        appender.setLong(0, id);
        appender.setBinary(1, pointWkb(x, y));
        appender.endRow();
    }

    private static MemorySegment pointWkb(double x, double y) {
        GeometryFactory factory = new GeometryFactory();
        Point point = factory.createPoint(new Coordinate(x, y));
        WKBWriter writer = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
        byte[] bytes = writer.write(point);
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    private static ParquetSchema geometrySchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                "geometry", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, geometry), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
