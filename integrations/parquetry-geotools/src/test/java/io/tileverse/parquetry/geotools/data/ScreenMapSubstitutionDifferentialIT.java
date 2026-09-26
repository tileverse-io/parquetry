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

import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.geotools.data.util.ScreenMap;
import org.geotools.referencing.operation.transform.AffineTransform2D;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.geo.JtsGeometryFilter;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * A substituting ScreenMap read against a read through a probe that never substitutes, over two point files and two
 * screen resolutions: every pixel painted by either lies within one pixel of a pixel painted by the other, the
 * substituting read emits one row or substitute per pixel painted by it, and it reads fewer bytes when it substituted
 * anything.
 *
 * <p>One fixture holds a geometry in every row, the other leaves every {@value #NULL_GEOMETRY_EVERY}th geometry null,
 * which keeps the acceptance proof honest: a unit is substituted only where every non-null geometry satisfies the query
 * and at least one geometry is there to stand for.
 *
 * <p>Each query shape runs on a serial runtime and on the default one, whose prefetcher plans ahead of the paint state.
 * A case that can prove a unit inside its query also asserts that a substitute was emitted, which keeps the comparison
 * from agreeing over an unused path.
 */
class ScreenMapSubstitutionDifferentialIT {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");

    private static final int ROWS = 8192;
    private static final int ROWS_PER_GROUP = 64;
    private static final double EXTENT = 16.0;
    private static final int SCREEN_SIZE = 256;

    /** Every row whose id is a multiple of this has a null geometry in the nullable fixture. */
    private static final int NULL_GEOMETRY_EVERY = 3;

    @TempDir
    static Path tempDir;

    private static Path points;
    private static Path pointsWithNulls;

    @BeforeAll
    static void writePointFiles() throws Exception {
        points = writeFixture("points.parquet", Repetition.REQUIRED);
        pointsWithNulls = writeFixture("points-with-nulls.parquet", Repetition.OPTIONAL);
    }

    @ParameterizedTest(name = "{0}, {1}, {2}")
    @MethodSource("screensPredicatesAndFiles")
    void aSubstitutingReadPaintsWithinOnePixelOfAReadWithoutSubstitution(Screen screen, Case queryCase, Path file) {
        assertPaintWithinOnePixel(screen, queryCase, file, serialRuntime(), true);
        assertPaintWithinOnePixel(screen, queryCase, file, ParquetRuntime.defaultRuntime(), false);
    }

    static Stream<Arguments> screensPredicatesAndFiles() {
        List<Screen> screens = List.of(
                new Screen("one world unit per pixel", ScreenMapSubstitutionDifferentialIT::oneWorldUnitPerPixel),
                new Screen("four world units per pixel", ScreenMapSubstitutionDifferentialIT::fourWorldUnitsPerPixel));
        List<Arguments> vectors = new ArrayList<>();
        for (Path file : List.of(points, pointsWithNulls)) {
            for (Case queryCase : queryShapes()) {
                for (Screen screen : screens) {
                    vectors.add(Arguments.of(screen, queryCase, file));
                }
            }
        }
        return vectors.stream();
    }

    private static List<Case> queryShapes() {
        Predicate viewport = viewport();
        Predicate polygon = Predicate.geometryFilter(JtsGeometryFilter.intersects(GEOMETRY, lowerLeftTriangle()));
        Predicate attribute = Pred.col("id").gt(100L);
        return List.of(
                new Case("whole extent", wholeExtent(), true),
                new Case("edge-straddling viewport", viewport, true),
                new Case("polygon", polygon, true),
                new Case("polygon and viewport", new Predicate.And(List.of(viewport, polygon)), true),
                new Case("viewport and attribute", new Predicate.And(List.of(viewport, attribute)), false));
    }

    /**
     * Reads the fixture twice over the same screen geometry, once through a substituting probe and once through one
     * that never substitutes, and compares what the two reads left painted.
     *
     * @param compareBytes whether to assert that the substituting read fetched fewer bytes, which only a runtime
     *     without coalescing or prefetching answers reproducibly
     */
    private void assertPaintWithinOnePixel(
            Screen screen, Case queryCase, Path file, ParquetRuntime runtime, boolean compareBytes) {
        ScreenMap substitutingScreen = screen.build();
        ScreenMap referenceScreen = screen.build();
        ScreenMapReadProbe substituting = new ScreenMapReadProbe(substitutingScreen, Point.class);
        ScreenMapReadProbe reference = new ScreenMapReadProbe(referenceScreen);

        AtomicLong substitutingBytes = new AtomicLong();
        AtomicLong referenceBytes = new AtomicLong();
        List<Long> substitutingIds = readIds(file, runtime, queryCase.predicate(), substituting, substitutingBytes);
        List<Long> referenceIds = readIds(file, runtime, queryCase.predicate(), reference, referenceBytes);

        Set<Pixel> substitutingPixels = paintedPixels(substitutingScreen);
        Set<Pixel> referencePixels = paintedPixels(referenceScreen);
        assertThat(referenceIds).as("the case keeps features to compare").isNotEmpty();
        assertThat(substitutingIds.size() + substituting.substitutesEmitted())
                .as("one row or substitute per painted pixel")
                .isEqualTo(substitutingPixels.size());
        assertWithinOnePixel(
                referencePixels, substitutingPixels, "a reference pixel has no substituting pixel near it");
        assertWithinOnePixel(
                substitutingPixels, referencePixels, "a substituting pixel has no reference pixel near it");
        if (!queryCase.acceptable()) {
            assertThat(substituting.substitutesEmitted()).isZero();
            return;
        }
        assertThat(substituting.substitutesEmitted())
                .as("the accepted path ran")
                .isPositive();
        if (compareBytes) {
            assertThat(substitutingBytes.get())
                    .as("substituted units are not fetched")
                    .isLessThan(referenceBytes.get());
        }
    }

    private static void assertWithinOnePixel(Set<Pixel> pixels, Set<Pixel> near, String message) {
        for (Pixel pixel : pixels) {
            boolean covered = near.stream()
                    .anyMatch(other -> Math.abs(other.x() - pixel.x()) <= 1 && Math.abs(other.y() - pixel.y()) <= 1);
            assertThat(covered).as(message + ": " + pixel).isTrue();
        }
    }

    private static List<Long> readIds(
            Path file, ParquetRuntime runtime, Predicate predicate, SpatialReadProbe probe, AtomicLong bytesRead) {
        ReadOptions options = ReadOptions.builder().spatialReadProbe(probe).build();
        List<Long> ids = new ArrayList<>();
        try (ByteRangeSource source = new CountingByteRangeSource(ByteRangeSource.ofFile(file), bytesRead)) {
            ParquetFileReader reader = ParquetFileReader.open(source, runtime, Optional.empty());
            try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, options)) {
                rows.forEach(row -> ids.add((Long) row.get(ID)));
            }
        }
        return ids;
    }

    /** Every painted pixel of the screen, which is the render produced by a decimated read. */
    private static Set<Pixel> paintedPixels(ScreenMap screen) {
        Set<Pixel> painted = new HashSet<>();
        for (int x = 0; x < SCREEN_SIZE; x++) {
            for (int y = 0; y < SCREEN_SIZE; y++) {
                if (screen.get(x, y)) {
                    painted.add(new Pixel(x, y));
                }
            }
        }
        return painted;
    }

    /** An identity world-to-screen transform at one world unit per pixel: a point in world cell {@code c} paints it. */
    private static ScreenMap oneWorldUnitPerPixel() {
        return screenMap(new AffineTransform2D(1, 0, 0, 1, 0, 0), 1.0);
    }

    /** Four world units per pixel: the whole fixture extent falls into a four by four block of pixels. */
    private static ScreenMap fourWorldUnitsPerPixel() {
        return screenMap(new AffineTransform2D(0.25, 0, 0, 0.25, 0, 0), 4.0);
    }

    private static ScreenMap screenMap(AffineTransform2D worldToScreen, double span) {
        ScreenMap screen = new ScreenMap(0, 0, SCREEN_SIZE, SCREEN_SIZE);
        screen.setTransform(worldToScreen);
        screen.setSpans(span, span);
        return screen;
    }

    /**
     * A runtime that fetches exactly the ranges requested by the plan, one row group at a time: the byte comparison
     * needs a read without coalescing, prefetching, or decode-ahead.
     */
    private static ParquetRuntime serialRuntime() {
        return ParquetRuntime.builder()
                .maxCoalesceGap(0)
                .maxDecodeAhead(0)
                .prefetchDepth(0)
                .build();
    }

    private static Predicate wholeExtent() {
        return new Predicate.Spatial.BboxIntersects(GEOMETRY, Bbox.of2d(0, 0, EXTENT, EXTENT));
    }

    /** A box whose edges fall inside the fixture, cutting through pixels and pages at both resolutions. */
    private static Predicate viewport() {
        return new Predicate.Spatial.BboxIntersects(GEOMETRY, Bbox.of2d(3.3, 2.2, 9.7, 11.1));
    }

    private static Geometry lowerLeftTriangle() {
        GeometryFactory factory = new GeometryFactory();
        Coordinate[] corners = {
            new Coordinate(0, 0), new Coordinate(EXTENT, 0), new Coordinate(0, EXTENT), new Coordinate(0, 0)
        };
        return factory.createPolygon(corners);
    }

    /**
     * Writes a fixture: {@value #ROWS} points over {@value #ROWS_PER_GROUP} rows per row group, with a page limit that
     * cuts the geometry column into pages of a few rows while a covering page still holds eight of them. An
     * {@link Repetition#OPTIONAL} geometry column leaves every {@value #NULL_GEOMETRY_EVERY}th geometry null.
     */
    private static Path writeFixture(String fileName, Repetition geometryRepetition) throws Exception {
        ParquetSchema schema = idAndGeometrySchema(geometryRepetition);
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .crsEpsg("geometry", 4326)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .pageValueLimit(8)
                .pageByteLimit(96)
                .build();
        Path target = tempDir.resolve(fileName);
        boolean nullable = geometryRepetition == Repetition.OPTIONAL;
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(target), schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(ROWS_PER_GROUP);
            long id = 0;
            for (double[] point : pointsInPixelOrder()) {
                appendPoint(appender, id, point, nullable);
                id++;
            }
            appender.flush();
        }
        return target;
    }

    private static void appendPoint(ParquetRecordBatchBuilder appender, long id, double[] point, boolean nullable) {
        appender.setLong(0, id);
        if (nullable && id % NULL_GEOMETRY_EVERY == 0) {
            appender.setNull(1);
        } else {
            appender.setBinary(1, pointWkb(point[0], point[1]));
        }
        appender.endRow();
    }

    /**
     * Points drawn from a fixed seed over the fixture extent, laid out in the order of a write sorted on the finer of
     * the two pixel grids. The coarser grid nests in that one, hence a page of consecutive rows usually falls in a
     * single pixel of either screen while some pages still straddle a pixel boundary.
     */
    private static List<double[]> pointsInPixelOrder() {
        Random random = new Random(42);
        List<double[]> points = new ArrayList<>(ROWS);
        for (int i = 0; i < ROWS; i++) {
            points.add(new double[] {random.nextDouble() * EXTENT, random.nextDouble() * EXTENT});
        }
        Comparator<double[]> byPixelThenX = Comparator.comparingInt((double[] point) -> (int) Math.floor(point[1]))
                .thenComparingInt(point -> (int) Math.floor(point[0]))
                .thenComparingDouble(point -> point[0]);
        points.sort(byPixelThenX);
        return points;
    }

    private static MemorySegment pointWkb(double x, double y) {
        GeometryFactory factory = new GeometryFactory();
        Point point = factory.createPoint(new Coordinate(x, y));
        WKBWriter writer = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN);
        byte[] bytes = writer.write(point);
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    private static ParquetSchema idAndGeometrySchema(Repetition geometryRepetition) {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                "geometry", geometryRepetition, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, geometry), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /** A byte range source that adds up how many bytes its delegate returned. */
    private record CountingByteRangeSource(ByteRangeSource delegate, AtomicLong bytesRead) implements ByteRangeSource {

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public int read(long offset, MemorySegment dst) {
            int read = delegate.read(offset, dst);
            bytesRead.addAndGet(read);
            return read;
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    /** One pixel of a screen, in screen coordinates. */
    private record Pixel(int x, int y) {}

    /** One named screen resolution, able to hand out a fresh ScreenMap of that geometry for each probe. */
    private record Screen(String name, Supplier<ScreenMap> maps) {

        ScreenMap build() {
            return maps.get();
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * One named query shape. {@code acceptable} says whether the planner can prove a unit of this fixture fully inside
     * the query: a conjunction with an attribute comparison never can, and its read exercises the substitution contract
     * by leaving it unused.
     */
    private record Case(String name, Predicate predicate, boolean acceptable) {

        @Override
        public String toString() {
            return name;
        }
    }
}
