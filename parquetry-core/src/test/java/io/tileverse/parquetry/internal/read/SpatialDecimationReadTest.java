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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.filter.SpatialReadProbe.Decision;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testkit.TestCorpus;
import io.tileverse.parquetry.testsupport.CellProbe;
import io.tileverse.parquetry.testsupport.RecordingByteRangeSource;
import io.tileverse.parquetry.testsupport.Wkb;

/**
 * Reads GeoParquet point files through a {@link SpatialReadProbe} and asserts how each decimation tier responds. The
 * leaf tier keeps the first point seen per integer-X cell; the row-group tier drops a whole group whose cell an earlier
 * group already painted, before its bytes are fetched; the page tier substitutes one sub-pixel page per cell and drops
 * the rest before fetch; and without a probe the read is inert, returning every row. Each tier owns its fixture; the
 * row-reading and cell-extraction helpers are shared.
 */
class SpatialDecimationReadTest {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");

    @TempDir
    Path tempDir;

    /**
     * The leaf (per-row) tier over a single-row-group file: a probe that keeps the first point in each integer-X cell
     * and skips the rest returns exactly one point per distinct integer X.
     */
    @Nested
    class LeafTier {

        @Test
        void keepsFirstPointPerIntegerCellOnTheRowsPath() throws Exception {
            List<double[]> points = pointsSpreadAcrossCells();
            Path file = writePointFile(points);
            Set<Integer> distinctCells = distinctIntegerXCells(points);

            try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
                ParquetFileReader reader = ParquetFileReader.open(source);

                assertThat(rowCount(reader, ReadOptions.DEFAULTS))
                        .as("a read without a probe returns every point")
                        .isEqualTo(points.size());

                ReadOptions decimating = ReadOptions.builder()
                        .spatialReadProbe(keepFirstPerIntegerXCell())
                        .build();
                List<Integer> survivingCells = readIntegerXCells(reader, decimating);

                assertThat(survivingCells)
                        .as("the probe keeps exactly one point per distinct integer-X cell")
                        .containsExactlyInAnyOrderElementsOf(distinctCells)
                        .hasSize(distinctCells.size());
            }
        }

        /**
         * A leaf probe that keeps the first point it sees in each integer-X cell and skips every later point in that
         * cell. The single row group's coarse consultation defaults to {@code Descend} (this probe overrides only the
         * per-row {@code probe}), hence the leaf tier is the sole decimator here.
         */
        private SpatialReadProbe keepFirstPerIntegerXCell() {
            Set<Integer> painted = new HashSet<>();
            return (minX, minY, maxX, maxY) -> painted.add(cellOf(minX)) ? Decision.keep() : Decision.skip();
        }

        /** Several points per integer-X cell across a handful of cells, kept in one row group. */
        private List<double[]> pointsSpreadAcrossCells() {
            List<double[]> points = new ArrayList<>();
            for (int cell = 0; cell < 5; cell++) {
                for (int withinCell = 0; withinCell < 4; withinCell++) {
                    double x = cell + 0.1 * withinCell;
                    double y = 10.0 + cell;
                    points.add(new double[] {x, y});
                }
            }
            return points;
        }

        private Set<Integer> distinctIntegerXCells(List<double[]> points) {
            Set<Integer> cells = new HashSet<>();
            for (double[] point : points) {
                cells.add(cellOf(point[0]));
            }
            return cells;
        }

        private Path writePointFile(List<double[]> points) throws Exception {
            ParquetSchema schema = geometrySchema();
            WriteOptions options = WriteOptions.builder()
                    .tempDir(tempDir)
                    .crsEpsg("geometry", 4326)
                    .build();
            Path file = tempDir.resolve("points.parquet");
            try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
                ParquetRecordBatchBuilder appender = writer.appender(points.size());
                for (double[] point : points) {
                    WriteFixtures.appendRow(appender, schema, pointRow(point[0], point[1]));
                }
                appender.flush();
            }
            return file;
        }
    }

    /**
     * The row-group (coarse) tier over a file with two row groups per integer-X cell: a probe that paints a cell from
     * the first group's rows skips the second group before fetch. A serial runtime over an I/O-recording byte source
     * proves the skipped group's on-disk bytes are never read.
     */
    @Nested
    class RowGroupTier {

        private static final long ROWS_PER_GROUP = 4L;
        private static final int CELL_COUNT = 3;
        private static final int GROUPS_PER_CELL = 2;
        private static final int GROUP_COUNT = CELL_COUNT * GROUPS_PER_CELL;

        @Test
        void skipsPaintedRowGroupsBeforeFetchOnTheRowsPath() throws Exception {
            Path file = writeTwoGroupsPerIntegerXCell();
            List<ByteSpan> groupSpans = columnChunkSpans(file);

            ByteRangeSource backing = ByteRangeSource.ofFile(file);
            RecordingByteRangeSource spy = new RecordingByteRangeSource(backing);
            ParquetRuntime serialRuntime =
                    ParquetRuntime.defaultRuntime().withMaxDecodeAhead(0).withPrefetchDepth(0);
            try (spy) {
                ParquetFileReader reader = ParquetFileReader.open(spy, serialRuntime, Optional.empty());

                assertThat(rowCount(reader, ReadOptions.DEFAULTS))
                        .as("a read without a probe returns every row")
                        .isEqualTo((long) GROUP_COUNT * ROWS_PER_GROUP);

                spy.reset();
                ReadOptions decimating = ReadOptions.builder()
                        .spatialReadProbe(skipSecondGroupPerCell())
                        .build();
                List<Integer> survivingCells = readIntegerXCells(reader, decimating);

                assertThat(survivingCells)
                        .as("each cell keeps its first row group and drops the second")
                        .containsExactlyInAnyOrderElementsOf(firstGroupCells())
                        .hasSize(CELL_COUNT * (int) ROWS_PER_GROUP);

                assertSecondGroupPerCellNeverFetched(spy, groupSpans);
            }
        }

        /** The integer-X cells the surviving (first-per-cell) row groups contribute, one entry per surviving row. */
        private List<Integer> firstGroupCells() {
            List<Integer> cells = new ArrayList<>();
            for (int cell = 0; cell < CELL_COUNT; cell++) {
                for (int row = 0; row < ROWS_PER_GROUP; row++) {
                    cells.add(cell);
                }
            }
            return cells;
        }

        /**
         * A screen-map-style probe. The per-row leaf consultation ({@code probe}) paints the point's cell and keeps the
         * row. The read-only coarse consultation ({@code probeRegion}) skips a whole row group whose cell is already
         * painted by an earlier group's rows, and otherwise descends without painting. With two groups per cell, the
         * first group's rows paint the cell and the second group is skipped before fetch.
         */
        private SpatialReadProbe skipSecondGroupPerCell() {
            Set<Integer> painted = new HashSet<>();
            return new SpatialReadProbe() {
                @Override
                public Decision probe(double minX, double minY, double maxX, double maxY) {
                    painted.add(cellOf(minX));
                    return Decision.keep();
                }

                @Override
                public Decision probeRegion(double minX, double minY, double maxX, double maxY) {
                    return painted.contains(cellOf(minX)) ? Decision.skip() : Decision.descend();
                }
            };
        }

        /** The second row group in each cell is dropped before fetch; the first is read. */
        private void assertSecondGroupPerCellNeverFetched(RecordingByteRangeSource spy, List<ByteSpan> groupSpans) {
            for (int group = 0; group < groupSpans.size(); group++) {
                ByteSpan span = groupSpans.get(group);
                boolean firstGroupInCell = group % GROUPS_PER_CELL == 0;
                assertThat(spy.readAnyByteIn(span.start(), span.end()))
                        .as("row group %d (bytes [%d, %d)) fetched", group, span.start(), span.end())
                        .isEqualTo(firstGroupInCell);
            }
        }

        /**
         * Writes points placed such that each integer-X cell fills exactly {@link #GROUPS_PER_CELL} consecutive row
         * groups, every group spanning a single cell. The row-group threshold of {@link #ROWS_PER_GROUP} cuts a new
         * group every that many rows, and the points within a cell stay inside it (offsets below {@code 1.0}).
         */
        private Path writeTwoGroupsPerIntegerXCell() throws Exception {
            ParquetSchema schema = geometrySchema();
            WriteOptions options = WriteOptions.builder()
                    .tempDir(tempDir)
                    .crsEpsg("geometry", 4326)
                    .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                    .build();
            Path file = tempDir.resolve("groups.parquet");
            int rowsPerCell = GROUPS_PER_CELL * (int) ROWS_PER_GROUP;
            try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
                // Flushing one batch per group threshold makes each batch boundary a new row group. A larger batch
                // would
                // land every row in a single group no matter the threshold, which would defeat the multi-group fixture.
                ParquetRecordBatchBuilder appender = writer.appender((int) ROWS_PER_GROUP);
                for (int cell = 0; cell < CELL_COUNT; cell++) {
                    for (int withinCell = 0; withinCell < rowsPerCell; withinCell++) {
                        double x = cell + 0.01 * withinCell;
                        double y = 10.0 + cell;
                        WriteFixtures.appendRow(appender, schema, pointRow(x, y));
                    }
                }
                appender.flush();
            }
            return file;
        }

        /** The on-disk byte span covering every column chunk of each row group, in file order. */
        private List<ByteSpan> columnChunkSpans(Path file) {
            try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
                FileMetaData footer = ParquetFormat.readFooter(source);
                List<ByteSpan> spans = new ArrayList<>();
                for (RowGroup rowGroup : footer.rowGroups()) {
                    spans.add(spanOf(rowGroup));
                }
                return spans;
            }
        }

        private ByteSpan spanOf(RowGroup rowGroup) {
            long start = Long.MAX_VALUE;
            long end = Long.MIN_VALUE;
            for (ColumnChunk chunk : rowGroup.columns()) {
                ColumnMetaData meta = chunk.metaData().orElseThrow();
                long chunkStart = chunkStartOffset(meta);
                long chunkEnd = chunkStart + meta.totalCompressedSize();
                start = Math.min(start, chunkStart);
                end = Math.max(end, chunkEnd);
            }
            return new ByteSpan(start, end);
        }

        /**
         * The first on-disk byte of a column chunk: its dictionary page when present, otherwise its first data page.
         */
        private long chunkStartOffset(ColumnMetaData meta) {
            return meta.dictionaryPageOffset().orElse(meta.dataPageOffset());
        }
    }

    /**
     * The page tier over a file whose every integer-X cell fills two geometry pages: the plan of a row group
     * substitutes one sub-pixel page per cell and drops the rest before fetch, hence no row of the file is emitted at
     * all. A recording byte source proves that no geometry page is ever read.
     */
    @Nested
    class PageTier {

        private static final ColumnPath ID = ColumnPath.of("id");
        private static final int ROWS_PER_GROUP = 16;
        private static final int ROWS_PER_PAGE = 4;
        private static final int ROWS_PER_CELL = 8;
        private static final int GROUPS = 2;

        @ParameterizedTest(name = "decode ahead = {0}")
        @ValueSource(ints = {0, 2})
        void substitutesEverySubPixelPageAndNeverReadsAGeometryPage(int ahead) throws Exception {
            Path file = writeTwoPagesPerCell();
            List<ByteSpan> geometryPages = everyGeometryPage(file);

            RecordingByteRangeSource spy = new RecordingByteRangeSource(ByteRangeSource.ofFile(file));
            ParquetRuntime runtime = ParquetRuntime.builder()
                    .maxCoalesceGap(0)
                    .maxDecodeAhead(ahead)
                    .prefetchDepth(ahead)
                    .build();
            try (spy) {
                ParquetFileReader reader = ParquetFileReader.open(spy, runtime, Optional.empty());
                CellProbe probe = new CellProbe();
                ReadOptions decimating =
                        ReadOptions.builder().spatialReadProbe(probe).build();

                spy.reset();
                List<Integer> survivingCells = readIntegerXCells(reader, coveringEverything(), decimating);

                assertThat(survivingCells)
                        .as("every page is substituted; no row is emitted")
                        .isEmpty();
                assertThat(probe.substitutes())
                        .as("the first page of each cell substitutes, the second finds the cell painted")
                        .isEqualTo(GROUPS * ROWS_PER_GROUP / ROWS_PER_CELL);
                assertThat(probe.painted()).containsExactlyInAnyOrderElementsOf(everyCellInRowOrder());
                for (int page = 0; page < geometryPages.size(); page++) {
                    ByteSpan span = geometryPages.get(page);
                    assertThat(spy.readAnyByteIn(span.start(), span.end()))
                            .as("geometry page %d was fetched", page)
                            .isFalse();
                }
            }
        }

        /** The one integer-X cell of each substituted page, in file order. */
        private List<Integer> everyCellInRowOrder() {
            List<Integer> cells = new ArrayList<>();
            for (int group = 0; group < GROUPS; group++) {
                for (int cell = 0; cell < ROWS_PER_GROUP / ROWS_PER_CELL; cell++) {
                    cells.add(group * 10 + cell);
                }
            }
            return cells;
        }

        /**
         * Writes {@link #GROUPS} row groups of {@link #ROWS_PER_GROUP} points in pages of {@link #ROWS_PER_PAGE}, with
         * the rows of each integer-X cell filling two consecutive pages. Group {@code g} occupies cells {@code g * 10}
         * and {@code g * 10 + 1}, far enough apart that no two groups share a cell.
         */
        private Path writeTwoPagesPerCell() throws Exception {
            ParquetSchema schema = idAndGeometrySchema();
            WriteOptions options = WriteOptions.builder()
                    .tempDir(tempDir)
                    .crsEpsg("geometry", 4326)
                    .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                    .pageValueLimit(ROWS_PER_PAGE)
                    .build();
            Path file = tempDir.resolve("two-pages-per-cell.parquet");
            try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
                ParquetRecordBatchBuilder appender = writer.appender(ROWS_PER_GROUP);
                long id = 0;
                for (int group = 0; group < GROUPS; group++) {
                    for (int row = 0; row < ROWS_PER_GROUP; row++) {
                        double x = group * 10 + row / ROWS_PER_CELL + 0.1 * (row % ROWS_PER_CELL);
                        appendPoint(appender, schema, id, x);
                        id++;
                    }
                }
                appender.flush();
            }
            return file;
        }

        private void appendPoint(ParquetRecordBatchBuilder appender, ParquetSchema schema, long id, double x) {
            Map<ColumnPath, Object> values = new HashMap<>(2);
            values.put(ID, id);
            values.put(GEOMETRY, Wkb.fromWkt("POINT (" + x + " 0.5)"));
            WriteFixtures.appendRow(appender, schema, values);
        }

        /** The byte span of every geometry data page of every row group, in file order. */
        private List<ByteSpan> everyGeometryPage(Path file) {
            try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
                FileMetaData footer = ParquetFormat.readFooter(source);
                List<ByteSpan> spans = new ArrayList<>();
                for (RowGroup rowGroup : footer.rowGroups()) {
                    for (PageLocation location : geometryPages(source, rowGroup)) {
                        spans.add(new ByteSpan(location.offset(), location.offset() + location.compressedPageSize()));
                    }
                }
                return spans;
            }
        }

        private List<PageLocation> geometryPages(ByteRangeSource source, RowGroup rowGroup) {
            for (ColumnChunk chunk : rowGroup.columns()) {
                ColumnMetaData meta = chunk.metaData().orElseThrow();
                if (!GEOMETRY.equals(ColumnPath.of(meta.pathInSchema()))) {
                    continue;
                }
                long offset = chunk.offsetIndexOffset().orElseThrow();
                int length = chunk.offsetIndexLength().orElseThrow();
                return ParquetFormat.readOffsetIndex(source, offset, length).pageLocations();
            }
            throw new IllegalStateException("the fixture row group has no geometry column chunk");
        }

        private Predicate coveringEverything() {
            return new Predicate.Spatial.BboxIntersects(GEOMETRY, Bbox.of2d(-1, -1, 100, 2));
        }

        private ParquetSchema idAndGeometrySchema() {
            SchemaNode.Primitive id = new SchemaNode.Primitive(
                    "id", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
            SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                    "geometry",
                    Repetition.REQUIRED,
                    PrimitiveKind.BYTE_ARRAY,
                    OptionalInt.empty(),
                    Optional.empty(),
                    -1);
            SchemaNode.Group root =
                    new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, geometry), Optional.empty(), -1);
            return new ParquetSchema(root);
        }
    }

    /**
     * The decimation machinery is inert without a probe and neutral with a permissive one. A read reaches the spatial
     * gate only when {@link ReadOptions#spatialReadProbe()} is present; with no probe the read returns the full row set
     * unchanged, and a probe that keeps every unit drops nothing. The corpus file spans several row groups, hence both
     * the row-group tier and the leaf tier run under the permissive probe.
     */
    @Nested
    class WithoutAProbe {

        private static final String GEO_FILE = "parquetry/geo/buildings-gp110-bbox-covering.parquet";
        private static final long TOTAL_ROWS = 1801L;

        @Test
        void aNoProbeReadReturnsEveryRow() {
            Path file = TestCorpus.extractFile(GEO_FILE, tempDir);
            try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
                ParquetFileReader reader = ParquetFileReader.open(source);
                assertThat(rowCount(reader, ReadOptions.DEFAULTS)).isEqualTo(TOTAL_ROWS);
            }
        }

        @Test
        void aPermissiveProbeDropsNothing() {
            Path file = TestCorpus.extractFile(GEO_FILE, tempDir);
            SpatialReadProbe keepEverything = (minX, minY, maxX, maxY) -> Decision.keep();
            ReadOptions withProbe =
                    ReadOptions.builder().spatialReadProbe(keepEverything).build();

            try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
                ParquetFileReader reader = ParquetFileReader.open(source);
                assertThat(rowCount(reader, withProbe))
                        .as("a probe that keeps every unit returns the same rows as a read with no probe")
                        .isEqualTo(rowCount(reader, ReadOptions.DEFAULTS))
                        .isEqualTo(TOTAL_ROWS);
            }
        }
    }

    /** A half-open byte span of the file under test. */
    private record ByteSpan(long start, long end) {}

    private static long rowCount(ParquetFileReader reader, ReadOptions options) {
        try (Stream<ParquetRecord> rows = reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
            return rows.count();
        }
    }

    private static List<Integer> readIntegerXCells(ParquetFileReader reader, ReadOptions options) {
        return readIntegerXCells(reader, Predicate.ALWAYS_TRUE, options);
    }

    private static List<Integer> readIntegerXCells(ParquetFileReader reader, Predicate predicate, ReadOptions options) {
        List<Integer> cells = new ArrayList<>();
        try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, options)) {
            rows.forEach(row -> cells.add(integerXCellOf(row)));
        }
        return cells;
    }

    private static int integerXCellOf(ParquetRecord row) {
        MemorySegment wkb = (MemorySegment) row.get(GEOMETRY);
        double x = wkb.get(ValueLayout.JAVA_DOUBLE_UNALIGNED, 5);
        return cellOf(x);
    }

    private static int cellOf(double minX) {
        return (int) Math.floor(minX);
    }

    private static Map<ColumnPath, Object> pointRow(double x, double y) {
        Map<ColumnPath, Object> values = new HashMap<>(1);
        values.put(GEOMETRY, Wkb.fromWkt("POINT (" + x + " " + y + ")"));
        return values;
    }

    private static ParquetSchema geometrySchema() {
        SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                "geometry", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(geometry), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
