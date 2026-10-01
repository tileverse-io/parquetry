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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

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
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.geo.JtsGeometryFilter;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.RecordingByteRangeSource;
import io.tileverse.parquetry.testsupport.SpanCellProbe;
import io.tileverse.parquetry.testsupport.Wkb;

/**
 * Pins the promise of the page-level substitution: a read through a probe that substitutes an accepted sub-pixel unit
 * paints within one cell of a read through a probe that leaves the accepted-region consultation at its interface
 * default, emits one row or substitute per painted cell, and fetches fewer geometry bytes. The two probes share one
 * cell grid and differ only in whether they substitute.
 *
 * <p>Every case runs twice: once on a serial runtime and once on the default one, where the prefetcher plans several
 * row groups ahead against a paint state that may lag behind what has been emitted. Each case also asserts that the
 * substituting read did substitute, which is what keeps the comparison from agreeing because nothing happened on either
 * side.
 *
 * <p>The fixture is one GeoParquet point file whose geometry pages are smaller than its covering pages, and dense
 * enough that a covering page usually fits in one cell of every span under test.
 */
class PageLevelSkipDifferentialTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");

    private static final int ROWS = 8192;
    private static final int ROWS_PER_GROUP = 64;
    private static final double EXTENT = 16.0;
    private static final double FINEST_SPAN = 0.5;
    private static final long FIRST_KEPT_ID = 100L;

    @TempDir
    static Path tempDir;

    private static Path file;

    @BeforeAll
    static void writePointFile() throws Exception {
        file = writeFixture();
    }

    @ParameterizedTest(name = "span {0}, {1}")
    @MethodSource("spansAndPredicates")
    void aSubstitutingReadPaintsWithinOneCellOfAReadWithoutSubstitution(double span, Case queryCase) {
        assertPaintWithinOneCell(span, queryCase, serialRuntime());
        assertPaintWithinOneCell(span, queryCase, ParquetRuntime.defaultRuntime());
    }

    @Test
    void aSubstitutingReadFetchesFewerGeometryBytes() {
        List<ByteSpan> geometryChunks = geometryColumnChunks();
        RecordingByteRangeSource spy = new RecordingByteRangeSource(ByteRangeSource.ofFile(file));
        try (spy) {
            ParquetFileReader reader = ParquetFileReader.open(spy, serialRuntime(), Optional.empty());

            SpanCellProbe substituting = new SpanCellProbe(4.0);
            spy.reset();
            List<Long> substitutingIds = readIds(reader, wholeExtent(), substituting);
            long substitutingBytes = bytesReadIn(spy, geometryChunks);

            SpanCellProbe reference = new SpanCellProbe(4.0);
            spy.reset();
            List<Long> referenceIds = readIds(reader, wholeExtent(), SpanCellProbe.withoutSubstitution(reference));
            long referenceBytes = bytesReadIn(spy, geometryChunks);

            assertThat(referenceIds).isNotEmpty();
            assertThat(substituting.substitutes()).isPositive();
            assertThat(substitutingIds.size() + substituting.substitutes())
                    .as("one row or substitute per painted cell")
                    .isEqualTo(substituting.painted().size());
            assertThat(substitutingBytes)
                    .as("substituted units are never fetched")
                    .isLessThan(referenceBytes);
        }
    }

    /**
     * A read that filters on the geometry but does not project it. Only the per-row gate paints, from the geometry of
     * the batch that it sees, and that batch is shaped by the read's output columns: with no geometry among them
     * nothing paints, hence no plan runs and every matching row is emitted.
     */
    @Test
    void aProbeReadThatDoesNotProjectTheGeometryEmitsEveryMatchingRow() {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source, serialRuntime(), Optional.empty());
            SpanCellProbe substituting = new SpanCellProbe(4.0);
            SpanCellProbe reference = new SpanCellProbe(4.0);
            Projection idOnly = Projection.ofPhysical(List.of(ID));

            List<Long> substitutingIds = readIds(reader, wholeExtent(), substituting, idOnly);
            List<Long> referenceIds =
                    readIds(reader, wholeExtent(), SpanCellProbe.withoutSubstitution(reference), idOnly);

            assertThat(substitutingIds)
                    .as("every row of the fixture matches the whole extent")
                    .hasSize(ROWS);
            assertThat(substitutingIds).as("the same rows in the same order").isEqualTo(referenceIds);
            assertThat(substituting.substitutes())
                    .as("without the geometry in the output nothing paints and no plan runs")
                    .isZero();
        }
    }

    static Stream<Arguments> spansAndPredicates() {
        List<Double> spans = List.of(FINEST_SPAN, 1.0, 2.0, 4.0);
        List<Case> cases = queryShapes();
        List<Arguments> vectors = new ArrayList<>();
        for (double span : spans) {
            for (Case queryCase : cases) {
                vectors.add(Arguments.of(span, queryCase));
            }
        }
        return vectors.stream();
    }

    /**
     * The query shapes that reach the planner differently: a box covering the whole fixture, a box whose edges cut
     * through cells and pages, an exact polygon filter, that polygon narrowed by a box, and a box narrowed by an
     * attribute comparison.
     */
    private static List<Case> queryShapes() {
        Predicate viewport = viewport();
        Predicate polygon = Predicate.geometryFilter(JtsGeometryFilter.intersects(GEOMETRY, lowerLeftTriangle()));
        Predicate attribute = Pred.col("id").gt(FIRST_KEPT_ID);
        return List.of(
                new Case("whole extent", wholeExtent(), true),
                new Case("edge-straddling viewport", viewport, true),
                new Case("polygon", polygon, true),
                new Case("polygon and viewport", new Predicate.And(List.of(viewport, polygon)), true),
                new Case("viewport and attribute", new Predicate.And(List.of(viewport, attribute)), false));
    }

    private void assertPaintWithinOneCell(double span, Case queryCase, ParquetRuntime runtime) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source, runtime, Optional.empty());
            SpanCellProbe substituting = new SpanCellProbe(span);
            SpanCellProbe reference = new SpanCellProbe(span);

            List<Long> substitutingIds = readIds(reader, queryCase.predicate(), substituting);
            List<Long> referenceIds =
                    readIds(reader, queryCase.predicate(), SpanCellProbe.withoutSubstitution(reference));

            assertThat(referenceIds).as("the case keeps rows to compare").isNotEmpty();
            assertThat(substitutingIds.size() + substituting.substitutes())
                    .as("one row or substitute per painted cell")
                    .isEqualTo(substituting.painted().size());
            assertWithinOneCell(
                    reference.painted(), substituting.painted(), "a reference cell has no substituting cell near it");
            assertWithinOneCell(
                    substituting.painted(), reference.painted(), "a substituting cell has no reference cell near it");
            assertSubstitutesMatchTheShape(queryCase, substituting);
        }
    }

    /** Every cell of {@code cells} lies within one cell, on both axes, of some cell of {@code near}. */
    private static void assertWithinOneCell(
            Set<SpanCellProbe.Cell> cells, Set<SpanCellProbe.Cell> near, String message) {
        for (SpanCellProbe.Cell cell : cells) {
            boolean covered = near.stream()
                    .anyMatch(other -> Math.abs(other.x() - cell.x()) <= 1 && Math.abs(other.y() - cell.y()) <= 1);
            assertThat(covered).as(message + ": " + cell).isTrue();
        }
    }

    /** A shape whose units can be proven inside the query must have substituted; one that cannot must not have. */
    private static void assertSubstitutesMatchTheShape(Case queryCase, SpanCellProbe substituting) {
        if (queryCase.acceptable()) {
            assertThat(substituting.substitutes()).as("the accepted path ran").isPositive();
            return;
        }
        assertThat(substituting.substitutes())
                .as("an attribute comparison leaves no unit proven inside the query")
                .isZero();
    }

    private static List<Long> readIds(ParquetFileReader reader, Predicate predicate, SpatialReadProbe probe) {
        return readIds(reader, predicate, probe, Projection.ALL);
    }

    private static List<Long> readIds(
            ParquetFileReader reader, Predicate predicate, SpatialReadProbe probe, Projection projection) {
        ReadOptions options = ReadOptions.builder().spatialReadProbe(probe).build();
        List<Long> ids = new ArrayList<>();
        try (Stream<ParquetRecord> rows = reader.read(predicate, projection, options)) {
            rows.forEach(row -> ids.add((Long) row.get(ID)));
        }
        return ids;
    }

    private static ParquetRuntime serialRuntime() {
        return ParquetRuntime.defaultRuntime().withMaxDecodeAhead(0).withPrefetchDepth(0);
    }

    private static Predicate wholeExtent() {
        return new Predicate.Spatial.BboxIntersects(GEOMETRY, Bbox.of2d(0, 0, EXTENT, EXTENT));
    }

    /** A box whose edges fall inside the fixture, cutting through cells and pages at every span under test. */
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

    private static long bytesReadIn(RecordingByteRangeSource spy, List<ByteSpan> spans) {
        long total = 0;
        for (ByteSpan span : spans) {
            total += spy.bytesReadIn(span.start(), span.end());
        }
        return total;
    }

    /** The on-disk byte span of the geometry column chunk of each row group, in file order. */
    private static List<ByteSpan> geometryColumnChunks() {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            List<ByteSpan> spans = new ArrayList<>();
            for (RowGroup rowGroup : footer.rowGroups()) {
                spans.add(geometryChunkSpan(rowGroup));
            }
            return spans;
        }
    }

    private static ByteSpan geometryChunkSpan(RowGroup rowGroup) {
        for (ColumnChunk chunk : rowGroup.columns()) {
            ColumnMetaData meta = chunk.metaData().orElseThrow();
            if (!GEOMETRY.equals(ColumnPath.of(meta.pathInSchema()))) {
                continue;
            }
            long start = meta.dictionaryPageOffset().orElse(meta.dataPageOffset());
            return new ByteSpan(start, start + meta.totalCompressedSize());
        }
        throw new IllegalStateException("the fixture row group has no geometry column chunk");
    }

    /**
     * Writes the fixture: {@value #ROWS} points over {@value #ROWS_PER_GROUP} rows per row group, with a page limit
     * that cuts the geometry column into pages of a few rows while a covering page still holds eight of them.
     */
    private static Path writeFixture() throws Exception {
        ParquetSchema schema = idAndGeometrySchema();
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .crsEpsg("geometry", 4326)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .pageValueLimit(8)
                .pageByteLimit(96)
                .build();
        Path target = tempDir.resolve("points.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(target), schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(ROWS_PER_GROUP);
            long id = 0;
            for (double[] point : pointsInCellOrder()) {
                appendPoint(appender, schema, id, point[0], point[1]);
                id++;
            }
            appender.flush();
        }
        return target;
    }

    /**
     * Points drawn from a fixed seed over the fixture extent, laid out in the order of a write sorted on the finest
     * grid under test. The coarser grids nest in that one, hence a page of consecutive rows usually falls in a single
     * cell of every span under test while some pages still straddle a cell boundary.
     */
    private static List<double[]> pointsInCellOrder() {
        Random random = new Random(42);
        List<double[]> points = new ArrayList<>(ROWS);
        for (int i = 0; i < ROWS; i++) {
            points.add(new double[] {random.nextDouble() * EXTENT, random.nextDouble() * EXTENT});
        }
        Comparator<double[]> byCellThenX = Comparator.comparingInt(
                        (double[] point) -> (int) Math.floor(point[1] / FINEST_SPAN))
                .thenComparingInt(point -> (int) Math.floor(point[0] / FINEST_SPAN))
                .thenComparingDouble(point -> point[0]);
        points.sort(byCellThenX);
        return points;
    }

    private static void appendPoint(
            ParquetRecordBatchBuilder appender, ParquetSchema schema, long id, double x, double y) {
        Map<ColumnPath, Object> values = new HashMap<>(2);
        values.put(ID, id);
        values.put(GEOMETRY, Wkb.fromWkt("POINT (" + x + " " + y + ")"));
        WriteFixtures.appendRow(appender, schema, values);
    }

    private static ParquetSchema idAndGeometrySchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                "geometry", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, geometry), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /** A half-open byte span of the file under test. */
    private record ByteSpan(long start, long end) {}

    /**
     * One named query shape. {@code acceptable} says whether the planner can prove a unit of this fixture inside the
     * query: a conjunction with an attribute comparison never can.
     */
    private record Case(String name, Predicate predicate, boolean acceptable) {

        @Override
        public String toString() {
            return name;
        }
    }
}
