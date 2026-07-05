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
import io.tileverse.parquetry.io.LocalFileSource;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Proves the feature source honors the renderer's {@link Hints#SCREENMAP}: a query that sets the hint reads through a
 * spatial decimation probe and collapses the many points in each painted pixel down to one, while a query without the
 * hint returns every point unchanged.
 *
 * <p>The fixture is a single GeoParquet point file of {@value #CELLS} cells with {@value #POINTS_PER_CELL} points each.
 * Every point in cell {@code c} lies in the world unit {@code [c, c + 1)} on X. With an identity world-to-screen
 * transform and one world unit per pixel ({@code setSpans(1.0, 1.0)}), each cell's points fall into the same pixel and
 * different cells fall into different pixels, hence the decimated read returns exactly one feature per cell.
 */
class ScreenMapDecimationIT {

    private static final int CELLS = 5;
    private static final int POINTS_PER_CELL = 4;
    private static final int TOTAL_POINTS = CELLS * POINTS_PER_CELL;

    @Test
    void readWithoutTheHintReturnsEveryPoint(@TempDir Path dir) throws Exception {
        try (CatalogDataStore store = pointStore(dir)) {
            CatalogFeatureSource fs = (CatalogFeatureSource) store.getFeatureSource("points");

            int seen = count(fs.getReader(new Query("points")));

            assertThat(seen).isEqualTo(TOTAL_POINTS);
        }
    }

    @Test
    void readWithTheScreenMapHintCollapsesToOnePointPerPaintedPixel(@TempDir Path dir) throws Exception {
        try (CatalogDataStore store = pointStore(dir)) {
            CatalogFeatureSource fs = (CatalogFeatureSource) store.getFeatureSource("points");

            Query query = new Query("points");
            query.getHints().put(Hints.SCREENMAP, oneWorldUnitPerPixelScreenMap());

            int seen = count(fs.getReader(query));

            assertThat(seen).isLessThan(TOTAL_POINTS).isEqualTo(CELLS);
        }
    }

    private static CatalogDataStore pointStore(Path dir) throws Exception {
        Path file = writePointFile(dir);
        FilesetCatalog catalog = FilesetCatalog.open(
                LocalFileSource.file(file),
                CatalogOptions.builder().datasetName("points").build());
        return new CatalogDataStore(catalog);
    }

    /**
     * A coarse ScreenMap over the fixture's extent: identity world-to-screen transform and one world unit per pixel.
     * Mirrors {@code ScreenMapReadProbeTest}; a sub-pixel point in cell {@code c} paints pixel {@code c}.
     */
    private static ScreenMap oneWorldUnitPerPixelScreenMap() {
        ScreenMap screenMap = new ScreenMap(0, 0, 256, 256);
        AffineTransform2D identity = new AffineTransform2D(1, 0, 0, 1, 0, 0);
        screenMap.setTransform(identity);
        screenMap.setSpans(1.0, 1.0);
        return screenMap;
    }

    private static int count(FeatureReader<SimpleFeatureType, SimpleFeature> reader) throws Exception {
        int n = 0;
        try (reader) {
            while (reader.hasNext()) {
                SimpleFeature feature = reader.next();
                assertThat(feature.getDefaultGeometry()).isInstanceOf(Geometry.class);
                n++;
            }
        }
        return n;
    }

    private static Path writePointFile(Path dir) throws Exception {
        ParquetSchema schema = geometrySchema();
        WriteOptions options =
                WriteOptions.builder().tempDir(dir).crsEpsg("geometry", 4326).build();
        Path file = dir.resolve("points.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(TOTAL_POINTS);
            for (double[] point : pointsClusteredByCell()) {
                appendPoint(appender, point[0], point[1]);
            }
            appender.flush();
        }
        return file;
    }

    /** Several points per cell across a handful of cells, all in one row group. */
    private static List<double[]> pointsClusteredByCell() {
        List<double[]> points = new ArrayList<>();
        for (int cell = 0; cell < CELLS; cell++) {
            for (int withinCell = 0; withinCell < POINTS_PER_CELL; withinCell++) {
                double x = cell + 0.1 * withinCell;
                double y = 10.0 + cell;
                points.add(new double[] {x, y});
            }
        }
        return points;
    }

    private static void appendPoint(ParquetRecordBatchBuilder appender, double x, double y) {
        appender.setBinary(0, pointWkb(x, y));
        appender.endRow();
    }

    private static MemorySegment pointWkb(double x, double y) {
        GeometryFactory factory = new GeometryFactory();
        Point point = factory.createPoint(new org.locationtech.jts.geom.Coordinate(x, y));
        WKBWriter writer = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
        byte[] bytes = writer.write(point);
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    private static ParquetSchema geometrySchema() {
        SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                "geometry", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(geometry), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
