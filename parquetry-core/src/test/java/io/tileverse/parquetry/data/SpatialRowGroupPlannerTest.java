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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.filter.PredicateNormalizer;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.internal.footer.CompactFooter;
import io.tileverse.parquetry.internal.read.IndexSectionLoader;
import io.tileverse.parquetry.internal.read.RowGroupChunks;
import io.tileverse.parquetry.internal.read.RowGroupNarrowing;
import io.tileverse.parquetry.internal.read.RowGroupPlanner;
import io.tileverse.parquetry.internal.read.RowGroupSurvivor;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.testsupport.CellProbe;
import io.tileverse.parquetry.testsupport.Wkb;

/**
 * Plans row groups of parquetry-written point files through a scripted probe and asserts which rows each plan decides
 * to read. The files put every covering page of four rows inside one integer-X cell, which lets the probe answer
 * "sub-pixel" for a page and "larger than a cell" for the row group around it. A page proven inside the query in an
 * unpainted cell is substituted and dropped; a page in a painted cell is dropped; a page straddling the query keeps its
 * whole row span.
 */
class SpatialRowGroupPlannerTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");
    private static final long ROWS_PER_GROUP = 16L;
    private static final int GROUPS = 3;

    @TempDir
    Path tempDir;

    private final List<ByteRangeSource> openSources = new ArrayList<>();

    @AfterEach
    void closeSources() {
        for (ByteRangeSource source : openSources) {
            source.close();
        }
    }

    @Test
    void anAcceptedSubPixelPageIsSubstitutedAndDropped() throws Exception {
        Fixture fixture = fixture();
        CellProbe probe = new CellProbe();
        RowGroupPlanner planner = fixture.plannerFor(probe, bboxIntersects(-1, -1, 100, 2));

        RowGroupNarrowing narrowing = planner.plan(0);

        // Group 0 spans cells 0 to 3, wider than one cell, hence it descends; each of its four pages is one cell
        // wide and lies fully inside the query, hence each is substituted and nothing of the group is read.
        assertThat(narrowing).isEqualTo(RowGroupNarrowing.dropped());
        assertThat(probe.substitutes()).isEqualTo(4);
        assertThat(probe.painted()).containsExactlyInAnyOrder(0, 1, 2, 3);
        assertThat(probe.log()).hasSize(5).allMatch(entry -> entry.startsWith("accepted"));
    }

    @Test
    void aPageInAPaintedCellIsDroppedWithoutASubstitute() throws Exception {
        Fixture fixture = fixture();
        CellProbe probe = new CellProbe();
        probe.painted().add(1);
        RowGroupPlanner planner = fixture.plannerFor(probe, bboxIntersects(-1, -1, 100, 2));

        RowGroupNarrowing narrowing = planner.plan(0);

        assertThat(narrowing).isEqualTo(RowGroupNarrowing.dropped());
        assertThat(probe.substitutes())
                .as("pages 0, 2 and 3; page 1 was already painted")
                .isEqualTo(3);
    }

    @Test
    void aSecondPageInACellPaintedByASubstituteIsDropped() throws Exception {
        Fixture fixture = fixtureWithTwoPagesPerCell();
        CellProbe probe = new CellProbe();
        RowGroupPlanner planner = fixture.plannerFor(probe, bboxIntersects(-1, -1, 100, 2));

        RowGroupNarrowing narrowing = planner.plan(0);

        assertThat(narrowing).isEqualTo(RowGroupNarrowing.dropped());
        assertThat(probe.substitutes()).as("one substitute per cell, two cells").isEqualTo(2);
    }

    @Test
    void aPageStraddlingTheQueryIsKeptWholeAndNeverSubstituted() throws Exception {
        Fixture fixture = fixture();
        CellProbe probe = new CellProbe();
        // The query cuts through cell 1: page 1 (rows 4 to 7, x in [1, 1.3]) straddles it, page 0 lies outside and
        // pages 2 and 3 lie inside.
        RowGroupPlanner planner = fixture.plannerFor(probe, bboxIntersects(1.15, -1, 100, 2));

        RowRanges rows = keptRows(planner.plan(0));

        assertThat(rows.ranges())
                .as("page 0 and page 1 are not accepted and stay whole; pages 2 and 3 are substituted")
                .containsExactly(range(0, 3), range(4, 7));
        assertThat(probe.substitutes()).isEqualTo(2);
        assertThat(probe.log())
                .as("the straddling page is offered to the read-only region answer, never to the accepted one")
                .anyMatch(entry -> entry.startsWith("region 1."))
                .noneMatch(entry -> entry.startsWith("accepted 1."));
    }

    @Test
    void aStraddlingPageInAPaintedCellIsDropped() throws Exception {
        Fixture fixture = fixture();
        CellProbe probe = new CellProbe();
        probe.painted().add(1);
        RowGroupPlanner planner = fixture.plannerFor(probe, bboxIntersects(1.15, -1, 100, 2));

        RowRanges rows = keptRows(planner.plan(0));

        assertThat(rows.ranges())
                .as("only page 0 survives: page 1 is painted, pages 2 and 3 substituted")
                .containsExactly(range(0, 3));
    }

    @Test
    void anAttributePredicateAcceptsNoUnit() throws Exception {
        Fixture fixture = fixture();
        CellProbe probe = new CellProbe();
        Predicate withAttribute = new Predicate.And(
                List.of(bboxIntersects(-1, -1, 100, 2), Pred.col("id").gt(0)));
        RowGroupPlanner planner = fixture.plannerFor(probe, withAttribute);

        RowGroupNarrowing narrowing = planner.plan(0);

        assertThat(narrowing).isEqualTo(RowGroupNarrowing.whole());
        assertThat(probe.substitutes()).isZero();
        assertThat(probe.log()).hasSize(5).allMatch(entry -> entry.startsWith("region"));
    }

    @Test
    void aSubstituteAnsweredToTheReadOnlyConsultationIsRejected() throws Exception {
        Fixture fixture = fixture();
        // The attribute comparison leaves no unit proven inside the query, hence every unit goes to the read-only
        // consultation, which is not allowed to stand in for rows of unknown content.
        Predicate withAttribute = new Predicate.And(
                List.of(bboxIntersects(-1, -1, 100, 2), Pred.col("id").gt(0)));
        RowGroupPlanner planner = fixture.plannerFor(substituteEveryRegion(), withAttribute);

        assertThatThrownBy(() -> planner.plan(0))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Substitute");
    }

    @Test
    void aWholeSubPixelRowGroupIsSubstitutedWithoutConsultingItsPages() throws Exception {
        Fixture fixture = fixtureWithOneCellPerGroup();
        CellProbe probe = new CellProbe();
        RowGroupPlanner planner = fixture.plannerFor(probe, bboxIntersects(-1, -1, 100, 2));

        RowGroupNarrowing narrowing = planner.plan(0);

        assertThat(narrowing).isEqualTo(RowGroupNarrowing.dropped());
        assertThat(probe.substitutes()).isEqualTo(1);
        assertThat(probe.log())
                .as("one accepted consultation for the whole group")
                .hasSize(1);
        assertThat(probe.log().get(0)).startsWith("accepted 0.");
    }

    @Test
    void aWholeSubPixelRowGroupInAPaintedCellIsDropped() throws Exception {
        Fixture fixture = fixtureWithOneCellPerGroup();
        CellProbe probe = new CellProbe();
        probe.painted().add(0);
        RowGroupPlanner planner = fixture.plannerFor(probe, bboxIntersects(-1, -1, 100, 2));

        assertThat(planner.plan(0)).isEqualTo(RowGroupNarrowing.dropped());
        assertThat(probe.substitutes()).isZero();
    }

    @Test
    void paintFromASubstituteOutlivesItsPlan() throws Exception {
        Fixture fixture = fixtureWithSharedCell();
        CellProbe probe = new CellProbe();
        RowGroupPlanner planner = fixture.plannerFor(probe, bboxIntersects(-1, -1, 100, 2));

        RowGroupNarrowing first = planner.plan(0);
        RowGroupNarrowing second = planner.plan(1);

        assertThat(first).isEqualTo(RowGroupNarrowing.dropped());
        assertThat(second)
                .as("the first group's substitute painted the cell both groups share")
                .isEqualTo(RowGroupNarrowing.dropped());
        assertThat(probe.substitutes()).isEqualTo(1);
    }

    @Test
    void theNarrowingIsIntersectedWithTheSurvivorsOwnRanges() throws Exception {
        Fixture fixture = fixture();
        CellProbe probe = new CellProbe();
        probe.painted().add(0);
        List<RowGroupSurvivor> narrowed = fixture.survivorsNarrowedTo(rowRanges(range(4, 11)));
        // The query cuts through cells 1 and 2: pages 1 and 2 straddle it and stay whole, page 0 lies outside in a
        // painted cell and is dropped, page 3 lies outside in an unpainted cell and stays whole.
        RowGroupPlanner planner = fixture.plannerFor(probe, bboxIntersects(1.15, -1, 2.25, 2), narrowed);

        RowRanges rows = keptRows(planner.plan(0));

        assertThat(rows.ranges()).containsExactly(range(4, 7), range(8, 11));
    }

    @Test
    void aPageWithNullGeometriesIsStillAccepted() throws Exception {
        Fixture fixture = fixtureWithNullsInEveryOtherRow();
        CellProbe probe = new CellProbe();
        RowGroupPlanner planner = fixture.plannerFor(probe, bboxIntersects(-1, -1, 100, 2));

        RowGroupNarrowing narrowing = planner.plan(0);

        assertThat(narrowing)
                .as("a box exists because the non-null rows have one")
                .isEqualTo(RowGroupNarrowing.dropped());
        assertThat(probe.substitutes()).isEqualTo(4);
    }

    @Test
    void aRowGroupWithoutAnyKeptRangeIsDropped() throws Exception {
        Fixture fixture = fixture();
        CellProbe probe = new CellProbe();
        probe.painted().addAll(List.of(0, 1, 2, 3));
        RowGroupPlanner planner = fixture.plannerFor(probe, bboxIntersects(-1, -1, 100, 2));

        assertThat(planner.plan(0)).isEqualTo(RowGroupNarrowing.dropped());
    }

    @Test
    void noPlannerWithoutAProbe() throws Exception {
        Fixture fixture = fixture();
        Predicate normalized = PredicateNormalizer.normalize(bboxIntersects(-1, -1, 100, 2));

        assertThat(fixture.gates()
                        .rowGroupPlanner(fixture.survivors(), normalized, ReadOptions.DEFAULTS, fixture.schema()))
                .isEmpty();
    }

    @Test
    void noPlannerWhenTheGeometryIsNotInTheOutput() throws Exception {
        Fixture fixture = fixture();
        Predicate normalized = PredicateNormalizer.normalize(bboxIntersects(-1, -1, 100, 2));
        ReadOptions options =
                ReadOptions.builder().spatialReadProbe(new CellProbe()).build();
        ParquetSchema idOnly = fixture.outputSchemaOf(ID);

        assertThat(fixture.gates().rowGroupPlanner(fixture.survivors(), normalized, options, idOnly))
                .as("only the per-row gate paints, and it paints from the geometry of the emitted batch")
                .isEmpty();
    }

    @Test
    void aRowGroupWithoutRowsIsLeftAsTheFilterPipelineLeftIt() throws Exception {
        Fixture fixture = fixtureWithOneCellPerGroup();
        CellProbe probe = new CellProbe();
        List<RowGroupSurvivor> emptied = fixture.survivorsWithoutRows();
        RowGroupPlanner planner = fixture.plannerFor(probe, bboxIntersects(-1, -1, 100, 2), emptied);

        assertThat(planner.plan(0)).isEqualTo(RowGroupNarrowing.whole());
        assertThat(probe.log()).as("an empty row group has no unit to consult").isEmpty();
    }

    /** A probe that answers the read-only coarse consultation with a substitute, which the planner must reject. */
    private static SpatialReadProbe substituteEveryRegion() {
        return new SpatialReadProbe() {
            @Override
            public Decision probe(double minX, double minY, double maxX, double maxY) {
                return Decision.keep();
            }

            @Override
            public Decision probeRegion(double minX, double minY, double maxX, double maxY) {
                return Decision.substitute();
            }
        };
    }

    /** Row {@code row} of row group {@code group} sits at this X coordinate; Y is the same for every row. */
    @FunctionalInterface
    private interface PointPlacement {
        double xOf(int group, int row);
    }

    /** Whether row {@code row} of a row group is written with a null geometry. */
    @FunctionalInterface
    private interface NullPlacement {
        boolean isNull(int row);
    }

    /**
     * The gates built over one written file, together with the row group views passed to their planner. The whole file
     * schema stands in for the output schema of a read that projects every column.
     *
     * @param emptiedChunks the same row groups seen through a footer whose first row group records zero rows
     */
    private record Fixture(
            SpatialReadGates gates,
            List<RowGroupChunks> chunks,
            ParquetSchema schema,
            List<RowGroupChunks> emptiedChunks) {

        List<RowGroupSurvivor> survivors() {
            return chunks.stream().map(RowGroupSurvivor::full).toList();
        }

        List<RowGroupSurvivor> survivorsNarrowedTo(RowRanges rows) {
            return chunks.stream()
                    .map(chunk -> new RowGroupSurvivor(chunk, Optional.of(rows), true))
                    .toList();
        }

        /** The same survivors with the first row group's row count rewritten to zero, which no writer emits. */
        List<RowGroupSurvivor> survivorsWithoutRows() {
            return emptiedChunks.stream().map(RowGroupSurvivor::full).toList();
        }

        /** The file schema narrowed to {@code leaf}, which is what a read projecting that one column emits. */
        ParquetSchema outputSchemaOf(ColumnPath leaf) {
            return schema.project(Set.of(leaf));
        }

        RowGroupPlanner plannerFor(SpatialReadProbe probe, Predicate predicate) {
            return plannerFor(probe, predicate, survivors());
        }

        RowGroupPlanner plannerFor(SpatialReadProbe probe, Predicate predicate, List<RowGroupSurvivor> survivors) {
            ReadOptions options = ReadOptions.builder().spatialReadProbe(probe).build();
            Predicate normalized = PredicateNormalizer.normalize(predicate);
            return gates.rowGroupPlanner(survivors, normalized, options, schema).orElseThrow();
        }
    }

    /** Four one-cell pages per row group: row {@code r} of group {@code g} sits at {@code g*10 + r/4 + 0.1*(r%4)}. */
    private Fixture fixture() throws IOException {
        return fixtureOf("four-cells-per-group", (group, row) -> group * 10 + row / 4 + 0.1 * (row % 4));
    }

    /** Two four-row pages per cell and two cells per row group. */
    private Fixture fixtureWithTwoPagesPerCell() throws IOException {
        return fixtureOf("two-pages-per-cell", (group, row) -> group * 10 + row / 8 + 0.1 * (row % 8));
    }

    /** The whole sixteen-row row group inside one cell, each group ten cells from the next. */
    private Fixture fixtureWithOneCellPerGroup() throws IOException {
        return fixtureOf("one-cell-per-group", (group, row) -> group * 10 + 0.06 * row);
    }

    /** Every row group inside cell 0, which lets paint from one group's substitute be visible to the next. */
    private Fixture fixtureWithSharedCell() throws IOException {
        return fixtureOf("shared-cell", (_, row) -> 0.06 * row);
    }

    /** Four one-cell pages per row group as {@link #fixture()}, over an optional geometry with every odd row null. */
    private Fixture fixtureWithNullsInEveryOtherRow() throws IOException {
        return fixtureOf(
                "nulls-in-every-other-row",
                (group, row) -> group * 10 + row / 4 + 0.1 * (row % 4),
                Repetition.OPTIONAL,
                row -> row % 2 == 1);
    }

    private Fixture fixtureOf(String name, PointPlacement placement) throws IOException {
        return fixtureOf(name, placement, Repetition.REQUIRED, row -> false);
    }

    private Fixture fixtureOf(
            String name, PointPlacement placement, Repetition geometryRepetition, NullPlacement nullAt)
            throws IOException {
        Path file = writePoints(name, placement, geometryRepetition, nullAt);
        ByteRangeSource source = ByteRangeSource.ofFile(file);
        openSources.add(source);
        ParquetFileReader reader = ParquetFileReader.open(source);
        ParquetSchema schema = reader.schema();
        Optional<GeoParquetMetadata> geo =
                Optional.of(GeoParquetMetadata.parse(reader.keyValueMetadata().get("geo")));
        SpatialReadGates gates = new SpatialReadGates(reader.spatialBounds(), schema, geo);
        List<RowGroupChunks> chunks = rowGroupChunks(reader, source, reader.compactFooter());
        List<RowGroupChunks> emptied = rowGroupChunks(reader, source, footerWithoutRowsInTheFirstGroup(reader, source));
        return new Fixture(gates, chunks, schema, emptied);
    }

    private static List<RowGroupChunks> rowGroupChunks(
            ParquetFileReader reader, ByteRangeSource source, CompactFooter footer) {
        IndexSectionLoader loader = indexLoader(source);
        List<RowGroupChunks> chunks = new ArrayList<>(GROUPS);
        for (int rowGroup = 0; rowGroup < footer.rowGroupCount(); rowGroup++) {
            chunks.add(RowGroupChunks.of(footer, rowGroup, reader.leafIndex(), reader.schema(), loader));
        }
        return chunks;
    }

    /**
     * The file's footer with row group 0 rewritten to hold no rows. The writer never emits such a row group, and the
     * filter pipeline does not eliminate one either, hence this is the only way to put one in front of the planner.
     */
    private static CompactFooter footerWithoutRowsInTheFirstGroup(ParquetFileReader reader, ByteRangeSource source) {
        FileMetaData footer = ParquetFormat.readFooter(source);
        List<RowGroup> rowGroups = new ArrayList<>(footer.rowGroups());
        RowGroup first = rowGroups.get(0);
        rowGroups.set(
                0,
                new RowGroup(
                        first.columns(),
                        first.totalByteSize(),
                        0L,
                        first.sortingColumns(),
                        first.fileOffset(),
                        first.totalCompressedSize(),
                        first.ordinal()));
        FileMetaData emptied = new FileMetaData(
                footer.version(),
                footer.schema(),
                footer.numRows(),
                rowGroups,
                footer.keyValueMetadata(),
                footer.createdBy(),
                footer.columnOrders(),
                footer.encryptionAlgorithm(),
                footer.footerSigningKeyMetadata());
        return CompactFooter.encode(emptied, reader.leafIndex());
    }

    private static IndexSectionLoader indexLoader(ByteRangeSource source) {
        return new IndexSectionLoader() {
            @Override
            public OffsetIndex readOffsetIndex(long offset, int length) {
                return ParquetFormat.readOffsetIndex(source, offset, length);
            }

            @Override
            public ColumnIndex readColumnIndex(long offset, int length) {
                return ParquetFormat.readColumnIndex(source, offset, length);
            }

            @Override
            public SplitBlockBloomFilter readBloom(long offset, int length) {
                throw new UnsupportedOperationException("bloom filters are not used in this test");
            }
        };
    }

    /**
     * Writes {@link #GROUPS} row groups of {@link #ROWS_PER_GROUP} points, four rows to a page, with the default FLOAT
     * bbox covering that a CRS-annotated geometry column emits.
     */
    private Path writePoints(String name, PointPlacement placement, Repetition geometryRepetition, NullPlacement nullAt)
            throws IOException {
        ParquetSchema schema = pointSchema(geometryRepetition);
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .crsEpsg("geometry", 4326)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .pageValueLimit(4)
                .build();
        Path file = tempDir.resolve(name + ".parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender((int) ROWS_PER_GROUP);
            long id = 0;
            for (int group = 0; group < GROUPS; group++) {
                for (int row = 0; row < ROWS_PER_GROUP; row++) {
                    Map<ColumnPath, Object> values = pointRow(id, placement.xOf(group, row));
                    if (nullAt.isNull(row)) {
                        values.put(GEOMETRY, null);
                    }
                    WriteFixtures.appendRow(appender, schema, values);
                    id++;
                }
            }
            appender.flush();
        }
        return file;
    }

    private static Map<ColumnPath, Object> pointRow(long id, double x) {
        Map<ColumnPath, Object> values = new HashMap<>(2);
        values.put(ID, id);
        values.put(GEOMETRY, Wkb.fromWkt("POINT (" + x + " 0.5)"));
        return values;
    }

    private static ParquetSchema pointSchema(Repetition geometryRepetition) {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                "geometry", geometryRepetition, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, geometry), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static Predicate bboxIntersects(double minX, double minY, double maxX, double maxY) {
        return new Predicate.Spatial.BboxIntersects(GEOMETRY, Bbox.of2d(minX, minY, maxX, maxY));
    }

    private static RowRanges keptRows(RowGroupNarrowing narrowing) {
        assertThat(narrowing).isInstanceOf(RowGroupNarrowing.Kept.class);
        RowGroupNarrowing.Kept kept = (RowGroupNarrowing.Kept) narrowing;
        return kept.rows().orElseThrow();
    }

    private static RowRanges rowRanges(Range... ranges) {
        return new RowRanges(List.of(ranges));
    }

    private static Range range(long first, long last) {
        return new Range(first, last);
    }
}
