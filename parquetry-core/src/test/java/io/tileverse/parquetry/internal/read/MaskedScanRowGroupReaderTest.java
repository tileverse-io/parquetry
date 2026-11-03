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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.Util;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.FilteredRecordBatch;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.VectorizedPredicateEvaluator;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.filter.GeometryFilter;
import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.filter.PageIndex;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.internal.read.page.DataPageRun;
import io.tileverse.parquetry.internal.read.page.PageSizeLimit;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * A masked scan emits exactly the rows a decode-all-then-filter read of the same row group emits, narrowed to the
 * output projection, in row order.
 */
class MaskedScanRowGroupReaderTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath V = ColumnPath.of("v");
    private static final ColumnPath NAME = ColumnPath.of("name");
    private static final ColumnPath TAG = ColumnPath.of("tag");
    private static final ColumnPath ELEMENT = ColumnPath.of("items", "list", "element");
    private static final ColumnPath STRUCT = ColumnPath.of("s");
    private static final ColumnPath LABEL = ColumnPath.of("s", "label");

    private static final int ROW_COUNT = 40;
    private static final int ZERO_ROW_FIXTURE_ROWS = 8;
    private static final long MILLISECOND_NANOS = 1_000_000L;

    @TempDir
    Path tempDir;

    static Stream<Arguments> cases() {
        Predicate fromTen = Pred.col(ID).gtEq(10L);
        return Stream.of(
                Arguments.of(
                        "output-only",
                        Pred.and(Pred.col(ID).gtEq(10L), Pred.col(ID).lt(20L)),
                        Set.of(V, NAME),
                        10),
                Arguments.of("predicate-also-projected", Pred.col(ID).eq(25L), Set.of(ID, V), 1),
                Arguments.of("no-match", Pred.col(ID).gt(9_999L), Set.of(V, NAME), 0),
                Arguments.of("all-match", Pred.col(ID).gtEq(0L), Set.of(V, NAME), ROW_COUNT),
                Arguments.of("alternating", Pred.col(ID).notEq(0L), Set.of(ID, V, NAME), ROW_COUNT - 1),
                Arguments.of(
                        "later stage keeps rows",
                        Pred.and(fromTen, Pred.col(NAME).eq("row-25")),
                        Set.of(ID, V),
                        1),
                Arguments.of(
                        "later stage drops all",
                        Pred.and(fromTen, Pred.col(NAME).eq("nobody")),
                        Set.of(V, NAME),
                        0),
                Arguments.of(
                        "or in a later stage",
                        Pred.and(
                                fromTen,
                                Pred.or(
                                        Pred.col(NAME).eq("row-5"),
                                        Pred.col(NAME).eq("row-25"))),
                        Set.of(V, NAME),
                        1),
                Arguments.of(
                        "null in a later stage", Pred.and(fromTen, Pred.col(TAG).isNull()), Set.of(ID, TAG), 10),
                Arguments.of(
                        "not-null in a later stage",
                        Pred.and(
                                fromTen,
                                Pred.or(Pred.col(TAG).isNotNull(), Pred.col(ID).lt(12L))),
                        Set.of(ID, TAG),
                        20),
                Arguments.of(
                        "null kept by an or",
                        Pred.and(
                                fromTen,
                                Pred.or(Pred.col(TAG).isNotNull(), Pred.col(ID).lt(13L))),
                        Set.of(ID, TAG),
                        21),
                Arguments.of(
                        "later stage owning no leaf",
                        Pred.and(fromTen, Pred.col(TAG).isNotNull(), textTest(TAG, "tag-25"::equals)),
                        Set.of(ID, V),
                        1),
                Arguments.of(
                        "stage after an empty stage",
                        Pred.and(fromTen, Pred.col(NAME).eq("nobody"), textTest(TAG, _ -> true)),
                        Set.of(ID, TAG),
                        0),
                Arguments.of(
                        "second stage owning no leaf",
                        Pred.and(Pred.col(TAG).isNotNull(), textTest(TAG, "tag-25"::equals)),
                        Set.of(ID, TAG),
                        1));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void maskedScanEqualsDecodeAllThenFilter(
            String label, Predicate predicate, Set<ColumnPath> projected, int matchingRows) throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        ParquetSchema outputSchema = fileSchema.project(projected);

        List<Row> masked = drainMaskedScan(file, fileSchema, outputSchema, predicate);
        List<Row> reference = drainDecodeAllThenFilter(file, fileSchema, outputSchema, predicate);

        assertThat(reference)
                .as("%s: the reference read selects the rows named by the case", label)
                .hasSize(matchingRows);
        assertThat(masked).as("%s: rows", label).containsExactlyElementsOf(reference);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void maskedScanIsInvariantToBatchSize(
            String label, Predicate predicate, Set<ColumnPath> projected, int matchingRows) throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        ParquetSchema outputSchema = fileSchema.project(projected);

        List<Row> unbounded = drainMaskedScan(file, fileSchema, outputSchema, predicate, ScanOptions.defaults());
        List<Row> one = drainMaskedScan(
                file, fileSchema, outputSchema, predicate, ScanOptions.of(OptionalInt.of(1), BatchForm.ASSEMBLED));

        assertThat(unbounded)
                .as("%s: the scan selects the rows named by the case", label)
                .hasSize(matchingRows);
        assertThat(one).as("%s: batch size 1 selects the same rows", label).containsExactlyElementsOf(unbounded);
    }

    @Test
    void aFilterOnlyColumnDoesNotLeakIntoTheOutput() throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        ParquetSchema outputSchema = fileSchema.project(Set.of(V));
        Predicate predicate = Pred.col(ID).gtEq(10L);

        List<Set<ColumnPath>> emitted = overMaskedScan(
                file,
                fileSchema,
                outputSchema,
                predicate,
                ScanOptions.defaults(),
                MaskedScanRowGroupReaderTest::drainColumnKeys);

        assertThat(emitted)
                .as("a matching predicate emits batches and the predicate column stays out of them")
                .isNotEmpty()
                .allSatisfy(columns -> assertThat(columns).containsExactly(V));
    }

    @Test
    void rowsDecodedCountsFilterRowsAndRowsMatchedCountsSurvivors() throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        ParquetSchema outputSchema = fileSchema.project(Set.of(V, NAME));
        Predicate predicate = Pred.and(Pred.col(ID).gtEq(10L), Pred.col(ID).lt(20L));

        Tallies tallies = overMaskedScan(
                file,
                fileSchema,
                outputSchema,
                predicate,
                ScanOptions.defaults(),
                MaskedScanRowGroupReaderTest::drainTallies);

        assertThat(tallies.rowsProduced())
                .as("every row of the row group runs its filter column through decode")
                .isEqualTo(ROW_COUNT);
        assertThat(tallies.rowsMatched()).as("ten rows match id in [10, 20)").isEqualTo(10L);
        assertThat(tallies.recordFilterNanos())
                .as("the per-window predicate evaluation is timed")
                .isPositive();
    }

    @Test
    void filterTimeAddsEachStage() throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        ParquetSchema outputSchema = fileSchema.project(Set.of(V));
        Predicate predicate =
                Pred.and(Pred.col(ID).eq(25L), textTest(NAME, MaskedScanRowGroupReaderTest::takesAMillisecond));

        Tallies tallies = overMaskedScan(
                file,
                fileSchema,
                outputSchema,
                predicate,
                ScanOptions.defaults(),
                MaskedScanRowGroupReaderTest::drainTallies);

        assertThat(tallies.recordFilterNanos())
                .as("the tally holds the time of the last stage")
                .isGreaterThanOrEqualTo(MILLISECOND_NANOS);
    }

    private static boolean takesAMillisecond(String name) {
        long start = System.nanoTime();
        while (System.nanoTime() - start < MILLISECOND_NANOS) {
            Thread.onSpinWait();
        }
        return true;
    }

    @Test
    void aHighlySelectivePredicateLeavesTheDeadOutputPagesUndecoded() throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        ParquetSchema outputSchema = fileSchema.project(Set.of(V, NAME));
        Predicate predicate = Pred.col(ID).eq(25L);
        int pagesPerColumn = pageCount(file, fileSchema, V);

        ScanResult scanned = overMaskedScan(
                file,
                fileSchema,
                outputSchema,
                predicate,
                ScanOptions.defaults(),
                MaskedScanRowGroupReaderTest::drainRowsAndPages);
        List<Row> reference = drainDecodeAllThenFilter(file, fileSchema, outputSchema, predicate);

        assertThat(reference).as("one row matches id 25").hasSize(1);
        assertThat(scanned.rows())
                .as("the rows the scan emits still match the reference read")
                .containsExactlyElementsOf(reference);
        assertThat(scanned.pages().decoded() + scanned.pages().skipped())
                .as("the scan reaches every page of the filter column and the two output columns")
                .isEqualTo(3 * pagesPerColumn);
        assertThat(scanned.pages().decoded())
                .as("each output column decodes only the page holding the surviving row, beside the filter "
                        + "column's whole walk")
                .isEqualTo(pagesPerColumn + 2);
        assertThat(scanned.pages().skipped())
                .as("every other output page is stepped over with its bytes still compressed")
                .isEqualTo(2 * (pagesPerColumn - 1));
    }

    /** The rows a scan emitted and the data pages it decoded and stepped over. */
    private record ScanResult(List<Row> rows, BatchRowGroupReader.PageCounts pages) {}

    private static ScanResult drainRowsAndPages(MaskedScanRowGroupReader reader) {
        List<Row> rows = drainRows(reader);
        return new ScanResult(rows, reader.pageCounts());
    }

    /** The data pages {@code leaf} holds in the fixture's single row group, as its offset index reports them. */
    private int pageCount(Path file, ParquetSchema fileSchema, ColumnPath leaf) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            RowGroupChunks chunks = TestRowGroupChunks.of(footer, 0, fileSchema, indexLoader(source));
            return chunks.pageLocations(leaf).orElseThrow().pageCount();
        }
    }

    @Test
    void aRowMaskNarrowsTheFilterAndOutputColumnsToTheSameRows() throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        ParquetSchema outputSchema = fileSchema.project(Set.of(ID, V, NAME));
        Predicate predicate = Pred.col(ID).notEq(13L);
        RowRanges surviving = new RowRanges(List.of(new RowRanges.Range(4, 17), new RowRanges.Range(30, 33)));

        List<Row> masked = drainMaskedScan(file, fileSchema, outputSchema, predicate, ScanOptions.overRows(surviving));
        List<Row> reference =
                drainDecodeAllThenFilter(file, fileSchema, outputSchema, predicate, Optional.of(surviving));

        assertThat(reference)
                .as("eighteen masked rows, one of them dropped by the predicate")
                .hasSize(17);
        assertThat(masked).as("rows under a row mask").containsExactlyElementsOf(reference);
    }

    @Test
    void laterStagePagesStayCompressed() throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        ParquetSchema outputSchema = fileSchema.project(Set.of(V));
        Predicate predicate = Pred.and(Pred.col(ID).eq(25L), Pred.col(NAME).eq("row-25"));
        int pagesPerColumn = pageCount(file, fileSchema, NAME);

        ScanResult scanned = overMaskedScan(
                file,
                fileSchema,
                outputSchema,
                predicate,
                ScanOptions.defaults(),
                MaskedScanRowGroupReaderTest::drainRowsAndPages);
        List<Row> reference = drainDecodeAllThenFilter(file, fileSchema, outputSchema, predicate);

        assertThat(reference).as("one row matches id 25").hasSize(1);
        assertThat(scanned.rows()).as("rows").containsExactlyElementsOf(reference);
        assertThat(scanned.pages().decoded())
                .as("the first stage's walk, then the one page holding the row in the stage and the output")
                .isEqualTo(pagesPerColumn + 2);
        assertThat(scanned.pages().skipped())
                .as("the other pages of the stage and the output stay compressed")
                .isEqualTo(2 * (pagesPerColumn - 1));
    }

    @Test
    void laterStageReusesAnEarlierLeaf() throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        ParquetSchema outputSchema = fileSchema.project(Set.of(V));
        Predicate nameOrNegativeId =
                Pred.or(Pred.col(NAME).eq("row-25"), Pred.col(ID).lt(0L));
        Predicate predicate = Pred.and(Pred.col(ID).gt(1L), nameOrNegativeId);
        int pagesPerColumn = pageCount(file, fileSchema, NAME);

        ScanResult scanned = overMaskedScan(
                file,
                fileSchema,
                outputSchema,
                predicate,
                ScanOptions.defaults(),
                MaskedScanRowGroupReaderTest::drainRowsAndPages);
        List<Row> reference = drainDecodeAllThenFilter(file, fileSchema, outputSchema, predicate);

        assertThat(reference).as("one row matches").hasSize(1);
        assertThat(scanned.rows()).as("rows").containsExactlyElementsOf(reference);
        assertThat(scanned.pages().decoded())
                .as("id and name decode each page once, and the output decodes the page holding the row")
                .isEqualTo(2 * pagesPerColumn + 1);
    }

    @Test
    void sharedLeafGathersFromItsStage() throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        ParquetSchema outputSchema = fileSchema.project(Set.of(NAME));
        Predicate predicate = Pred.and(Pred.col(ID).gtEq(10L), Pred.col(NAME).notEq("row-25"));
        int pagesPerColumn = pageCount(file, fileSchema, NAME);

        ScanResult scanned = overMaskedScan(
                file,
                fileSchema,
                outputSchema,
                predicate,
                ScanOptions.defaults(),
                MaskedScanRowGroupReaderTest::drainRowsAndPages);
        List<Row> reference = drainDecodeAllThenFilter(file, fileSchema, outputSchema, predicate);

        assertThat(reference).as("rows 10 to 39 but one").hasSize(ROW_COUNT - 11);
        assertThat(scanned.rows()).as("rows").containsExactlyElementsOf(reference);
        assertThat(scanned.pages().decoded())
                .as("id decodes each page, and name decodes each page with rows left, once")
                .isEqualTo(2 * pagesPerColumn - 2);
        assertThat(scanned.pages().skipped())
                .as("the two pages of name before row 10 stay compressed")
                .isEqualTo(2);
    }

    @Test
    void rowMaskTakesEachStage() throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        ParquetSchema outputSchema = fileSchema.project(Set.of(ID, V, NAME));
        Predicate predicate = Pred.and(Pred.col(ID).gtEq(12L), Pred.col(NAME).notEq("row-13"));
        RowRanges surviving = new RowRanges(List.of(new RowRanges.Range(4, 17), new RowRanges.Range(30, 33)));

        List<Row> masked = drainMaskedScan(file, fileSchema, outputSchema, predicate, ScanOptions.overRows(surviving));
        List<Row> reference =
                drainDecodeAllThenFilter(file, fileSchema, outputSchema, predicate, Optional.of(surviving));

        assertThat(reference).as("rows 12 to 17 and 30 to 33, without row 13").hasSize(9);
        assertThat(masked).as("rows under a row mask").containsExactlyElementsOf(reference);
    }

    @Test
    void countWalksEachStage() throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        ParquetSchema noOutputColumns = fileSchema.project(Set.of());
        Predicate predicate = Pred.and(Pred.col(ID).gtEq(10L), Pred.col(NAME).notEq("row-25"));

        Tallies tallies = overMaskedScan(
                file,
                fileSchema,
                noOutputColumns,
                predicate,
                ScanOptions.defaults(),
                MaskedScanRowGroupReaderTest::drainEmptyBatches);

        assertThat(tallies.rowsProduced()).as("the windows cover the row group").isEqualTo(ROW_COUNT);
        assertThat(tallies.rowsMatched()).as("rows 10 to 39 but one").isEqualTo(ROW_COUNT - 11L);
    }

    /**
     * A leaf of an optional struct, over both batch forms and sizes. A plain test reads the vector of the leaf, and a
     * quantified test reads a record of the stage's batch. Both read a null struct as a null label.
     */
    static Stream<Arguments> structCases() {
        Predicate fromTen = Pred.col(ID).gtEq(10L);
        Predicate labelIsNull = Pred.col(LABEL).isNull();
        Predicate aLabelIsThere = anyElement(Pred.col(LABEL).isNotNull());
        List<Arguments> cases = new ArrayList<>();
        for (BatchForm outputForm : BatchForm.values()) {
            for (OptionalInt batchSizeCap : List.of(OptionalInt.empty(), OptionalInt.of(1))) {
                cases.add(Arguments.of("null label", Pred.and(fromTen, labelIsNull), outputForm, batchSizeCap, 14));
                cases.add(Arguments.of("label", Pred.and(fromTen, aLabelIsThere), outputForm, batchSizeCap, 16));
            }
        }
        return cases.stream();
    }

    @ParameterizedTest(name = "{0}, {2}, batch size {3}")
    @MethodSource("structCases")
    void structLeafInALaterStage(
            String label, Predicate predicate, BatchForm outputForm, OptionalInt batchSizeCap, int matchingRows)
            throws Exception {
        Path file = writeStructsAcrossManyPages();
        ParquetSchema fileSchema = structFileSchema();
        ParquetSchema outputSchema = fileSchema.project(Set.of(ID, LABEL));

        List<Row> masked =
                drainMaskedScan(file, fileSchema, outputSchema, predicate, ScanOptions.of(batchSizeCap, outputForm));
        List<Row> reference = drainDecodeAllThenFilter(file, fileSchema, outputSchema, predicate);

        assertThat(reference)
                .as("%s: the reference read selects the rows named by the case", label)
                .hasSize(matchingRows);
        assertThat(masked).as("%s: rows", label).containsExactlyElementsOf(reference);
    }

    @Test
    void repeatedLeafBesideALaterStage() throws Exception {
        Path file = writeListsAcrossManyPages();
        ParquetSchema fileSchema = listFileSchema();
        ParquetSchema outputSchema = fileSchema.project(Set.of(ID));
        // The first stage drops row 33, which holds no list. A column out of step shows as a row lost or gained.
        Predicate namedRows = Pred.col(NAME).inStrings("row-32", "row-33");
        Predicate predicate = Pred.and(anyElement(Pred.col(ELEMENT).gt(100L)), namedRows);

        List<Row> reference = drainDecodeAllThenFilter(file, fileSchema, outputSchema, predicate);

        assertThat(reference).as("one row matches").hasSize(1);
        for (OptionalInt batchSizeCap : List.of(OptionalInt.empty(), OptionalInt.of(1))) {
            ScanOptions options = ScanOptions.of(batchSizeCap, BatchForm.ASSEMBLED);
            List<Row> masked = drainMaskedScan(file, fileSchema, outputSchema, predicate, options);
            assertThat(masked).as("batch size %s", batchSizeCap).containsExactlyElementsOf(reference);
        }
        ScanResult nameAlone = overMaskedScan(
                file,
                fileSchema,
                fileSchema.project(Set.of()),
                predicate,
                ScanOptions.defaults(),
                MaskedScanRowGroupReaderTest::drainRowsAndPages);
        assertThat(nameAlone.pages().skipped())
                .as("no row before row 10 passes the first stage, which leaves two pages of name compressed")
                .isEqualTo(2);
    }

    // Rows 0 and 1 end the first page of name, and a zero-row page stands before the page of the next rows.
    @Test
    void zeroRowPageInALaterStage() throws IOException {
        ParquetSchema fileSchema =
                flatSchema(requiredLeaf("id", PrimitiveKind.INT64), requiredLeaf("name", PrimitiveKind.BYTE_ARRAY));
        List<FetchedColumnChunk> chunks = List.of(
                plainChunk(ID, longPage(0, 4), longPage(4, ZERO_ROW_FIXTURE_ROWS)),
                plainChunk(NAME, namePage(0, 2), namePage(2, 2), namePage(2, ZERO_ROW_FIXTURE_ROWS)));
        ParquetSchema outputSchema = fileSchema.project(Set.of(ID, NAME));
        Predicate deadBeforeThePage =
                Pred.and(Pred.col(ID).gtEq(4L), Pred.col(NAME).notEq("nobody"));
        Predicate aliveAcrossThePage =
                Pred.and(Pred.col(ID).gtEq(0L), Pred.col(NAME).notEq("row-3"));

        ScanResult dead = scanChunks(chunks, fileSchema, outputSchema, deadBeforeThePage);
        ScanResult alive = scanChunks(chunks, fileSchema, outputSchema, aliveAcrossThePage);
        List<Row> deadReference =
                decodeAllThenFilter(chunks, fileSchema, outputSchema, deadBeforeThePage, Optional.empty());
        List<Row> aliveReference =
                decodeAllThenFilter(chunks, fileSchema, outputSchema, aliveAcrossThePage, Optional.empty());

        assertThat(deadReference).as("rows 4 to 7").hasSize(4);
        assertThat(dead.rows()).as("rows stepped over the zero-row page").containsExactlyElementsOf(deadReference);
        assertThat(dead.pages().skipped())
                .as("the first page of name and the zero-row page are discarded unread")
                .isEqualTo(2);
        assertThat(dead.pages().decoded())
                .as("two pages of id, and the page of name holding rows 4 to 7")
                .isEqualTo(3);
        assertThat(aliveReference).as("each row but row 3").hasSize(7);
        assertThat(alive.rows()).as("rows read across the zero-row page").containsExactlyElementsOf(aliveReference);
    }

    private static ScanResult scanChunks(
            List<FetchedColumnChunk> chunks,
            ParquetSchema fileSchema,
            ParquetSchema outputSchema,
            Predicate predicate) {
        return overChunks(
                chunks,
                fileSchema,
                outputSchema,
                predicate,
                ScanOptions.defaults(),
                Optional.empty(),
                ZERO_ROW_FIXTURE_ROWS,
                MaskedScanRowGroupReaderTest::drainRowsAndPages);
    }

    /** The nested output shape: a flat predicate column and a LIST output column, over both batch forms and sizes. */
    static Stream<Arguments> nestedCases() {
        List<Arguments> cases = new ArrayList<>();
        for (BatchForm outputForm : BatchForm.values()) {
            for (OptionalInt batchSizeCap : List.of(OptionalInt.empty(), OptionalInt.of(1))) {
                cases.add(Arguments.of(
                        "middle-block",
                        Pred.and(Pred.col(ID).gtEq(10L), Pred.col(ID).lt(20L)),
                        outputForm,
                        batchSizeCap,
                        10));
                cases.add(Arguments.of("all-but-one", Pred.col(ID).notEq(7L), outputForm, batchSizeCap, ROW_COUNT - 1));
            }
        }
        return cases.stream();
    }

    @ParameterizedTest(name = "{0}, {2}, batch size {3}")
    @MethodSource("nestedCases")
    void aListOutputColumnMatchesDecodeAllThenFilter(
            String label, Predicate predicate, BatchForm outputForm, OptionalInt batchSizeCap, int matchingRows)
            throws Exception {
        Path file = writeListsAcrossManyPages();
        ParquetSchema fileSchema = listFileSchema();
        ParquetSchema outputSchema = fileSchema.project(Set.of(ELEMENT));

        List<Row> masked =
                drainMaskedScan(file, fileSchema, outputSchema, predicate, ScanOptions.of(batchSizeCap, outputForm));
        List<Row> reference = drainDecodeAllThenFilter(file, fileSchema, outputSchema, predicate);

        assertThat(reference)
                .as("%s: the reference read selects the rows named by the case", label)
                .hasSize(matchingRows);
        assertThat(masked).as("%s: list rows", label).containsExactlyElementsOf(reference);
    }

    /**
     * The list column as the predicate's leaf and the output's alike: a quantified predicate over
     * {@code items.list.element} while the output projects that same column, over both batch forms and sizes.
     */
    static Stream<Arguments> quantifiedCases() {
        List<Arguments> cases = new ArrayList<>();
        for (BatchForm outputForm : BatchForm.values()) {
            for (OptionalInt batchSizeCap : List.of(OptionalInt.empty(), OptionalInt.of(1))) {
                cases.add(Arguments.of(
                        "any-element-at-or-above-200",
                        anyElement(Pred.col(ELEMENT).gtEq(200L)),
                        outputForm,
                        batchSizeCap,
                        13));
                cases.add(Arguments.of(
                        "any-element-equal-to-100",
                        anyElement(Pred.col(ELEMENT).eq(100L)),
                        outputForm,
                        batchSizeCap,
                        1));
                cases.add(Arguments.of(
                        "all-elements-below-200",
                        allElements(Pred.col(ELEMENT).lt(200L)),
                        outputForm,
                        batchSizeCap,
                        17));
            }
        }
        return cases.stream();
    }

    private static Predicate anyElement(Predicate leaf) {
        return new Predicate.Quantified(MatchAction.ANY, leaf);
    }

    private static Predicate allElements(Predicate leaf) {
        return new Predicate.Quantified(MatchAction.ALL, leaf);
    }

    @ParameterizedTest(name = "{0}, {2}, batch size {3}")
    @MethodSource("quantifiedCases")
    void aQuantifiedPredicateOnTheProjectedListMatchesDecodeAllThenFilter(
            String label, Predicate predicate, BatchForm outputForm, OptionalInt batchSizeCap, int matchingRows)
            throws Exception {
        Path file = writeListsAcrossManyPages();
        ParquetSchema fileSchema = listFileSchema();
        ParquetSchema outputSchema = fileSchema.project(Set.of(ELEMENT));

        List<Row> masked =
                drainMaskedScan(file, fileSchema, outputSchema, predicate, ScanOptions.of(batchSizeCap, outputForm));
        List<Row> reference = drainDecodeAllThenFilter(file, fileSchema, outputSchema, predicate);

        assertThat(reference)
                .as("%s: the reference read selects the rows named by the case", label)
                .hasSize(matchingRows);
        assertThat(masked)
                .as("%s: the leaf shared by the predicate and the output is gathered to the surviving rows", label)
                .containsExactlyElementsOf(reference);
    }

    @Test
    void anOutputSchemaWithNoLeafEmitsRowCountedBatchesWithNoColumns() throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        ParquetSchema noOutputColumns = fileSchema.project(Set.of());
        Predicate predicate = Pred.and(Pred.col(ID).gtEq(10L), Pred.col(ID).lt(20L));

        Tallies tallies = overMaskedScan(
                file,
                fileSchema,
                noOutputColumns,
                predicate,
                ScanOptions.defaults(),
                MaskedScanRowGroupReaderTest::drainEmptyBatches);

        assertThat(tallies.rowsProduced())
                .as("the filter columns alone size the windows and cover the row group")
                .isEqualTo(ROW_COUNT);
        assertThat(tallies.rowsMatched())
                .as("the emitted batches count the matching rows without decoding a value for them")
                .isEqualTo(10L);
    }

    /** Drains a scan whose batches hold no column, asserting each one still reports its surviving rows. */
    private static Tallies drainEmptyBatches(MaskedScanRowGroupReader reader) {
        while (reader.hasMore()) {
            try (ParquetRecordBatch batch = reader.nextBatch()) {
                assertThat(batch.columns())
                        .as("an output set of no columns emits none")
                        .isEmpty();
                assertThat(batch.rowCount())
                        .as("the batch still counts its rows")
                        .isPositive();
            }
        }
        return new Tallies(reader.rowsProduced(), reader.rowsMatched(), reader.recordFilterNanos());
    }

    @Test
    void closingWithABatchStillPendingReleasesItsBuffers() throws Exception {
        Path file = writeListsAcrossManyPages();
        ParquetSchema fileSchema = listFileSchema();
        ParquetSchema outputSchema = fileSchema.project(Set.of(ELEMENT));
        Predicate everyRow = Pred.col(ID).gtEq(0L);
        SegmentPool pool = SegmentPool.create();
        ScanOptions overOwnPool =
                ScanOptions.of(OptionalInt.empty(), BatchForm.LEVELS).withPool(pool);

        overMaskedScan(
                file,
                fileSchema,
                outputSchema,
                everyRow,
                overOwnPool,
                MaskedScanRowGroupReaderTest::takeOneBatchAndLeaveTheNextPending);

        assertThat(pool.stats().outstandingBorrows())
                .as("the batch computed ahead of the consumer is released when the scan closes")
                .isZero();
    }

    @Test
    void closingMidwayThroughALaterPage() throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        ParquetSchema outputSchema = fileSchema.project(Set.of(V));
        Predicate predicate = Pred.and(Pred.col(ID).gtEq(1L), Pred.col(NAME).notEq("nobody"));
        SegmentPool pool = SegmentPool.create();
        ScanOptions oneRowAtATime =
                ScanOptions.of(OptionalInt.of(1), BatchForm.LEVELS).withPool(pool);

        overMaskedScan(
                file,
                fileSchema,
                outputSchema,
                predicate,
                oneRowAtATime,
                MaskedScanRowGroupReaderTest::takeOneBatchAndLeaveTheNextPending);

        assertThat(pool.stats().outstandingBorrows())
                .as("the pending batch and the later stage's open page are released when the scan closes")
                .isZero();
    }

    // A batch becomes pending only after each stage read its window. A page stays held when an earlier stage fails.
    @Test
    void closingHoldingALaterPage() throws Exception {
        Path file = writeFortyRowsAcrossManyPages();
        ParquetSchema fileSchema = fileSchema();
        ParquetSchema outputSchema = fileSchema.project(Set.of(V));
        Predicate failingCondition = Pred.or(
                textTest(NAME, MaskedScanRowGroupReaderTest::failsOnRowFive),
                Pred.col(NAME).eq("nobody"));
        Predicate predicate = Pred.and(Pred.col(ID).gtEq(0L), failingCondition, textTest(TAG, _ -> true));
        SegmentPool pool = SegmentPool.create();
        ScanOptions overOwnPool =
                ScanOptions.of(OptionalInt.empty(), BatchForm.LEVELS).withPool(pool);

        ParquetRecordBatch stillOpen = overMaskedScan(
                file,
                fileSchema,
                outputSchema,
                predicate,
                overOwnPool,
                MaskedScanRowGroupReaderTest::takeOneBatchThenFailOnTheNext);
        stillOpen.close();

        assertThat(pool.stats().outstandingBorrows())
                .as("the failed window, the scan and the batch taken before the failure release their buffers")
                .isZero();
    }

    private static boolean failsOnRowFive(String name) {
        if (name.equals("row-5")) {
            throw new IllegalStateException("the test cannot read " + name);
        }
        return true;
    }

    /** Takes the batch of the first window, and leaves the page of the last stage held by the failed second window. */
    private static ParquetRecordBatch takeOneBatchThenFailOnTheNext(MaskedScanRowGroupReader reader) {
        ParquetRecordBatch first = reader.nextBatch();
        assertThatThrownBy(reader::hasMore)
                .as("the second stage reads row 5 in the second window")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("row-5");
        assertThat(reader.pageCounts().decoded())
                .as("id and name decoded two pages each, tag and the output one, and tag holds its second page")
                .isEqualTo(6);
        return first;
    }

    /** Consumes one batch, then leaves the batch {@code hasMore} computes behind it unclaimed. */
    private static boolean takeOneBatchAndLeaveTheNextPending(MaskedScanRowGroupReader reader) {
        try (ParquetRecordBatch batch = reader.nextBatch()) {
            assertThat(batch.rowCount()).as("the scan emits a first batch").isPositive();
        }
        assertThat(reader.hasMore())
                .as("a further batch is computed and held pending")
                .isTrue();
        return true;
    }

    // --- drains ---

    private List<Row> drainMaskedScan(
            Path file, ParquetSchema fileSchema, ParquetSchema outputSchema, Predicate predicate) throws Exception {
        return drainMaskedScan(file, fileSchema, outputSchema, predicate, ScanOptions.defaults());
    }

    private List<Row> drainMaskedScan(
            Path file, ParquetSchema fileSchema, ParquetSchema outputSchema, Predicate predicate, ScanOptions options)
            throws Exception {
        return overMaskedScan(
                file, fileSchema, outputSchema, predicate, options, MaskedScanRowGroupReaderTest::drainRows);
    }

    private static List<Row> drainRows(MaskedScanRowGroupReader reader) {
        List<Row> rows = new ArrayList<>();
        while (reader.hasMore()) {
            try (ParquetRecordBatch batch = reader.nextBatch()) {
                collectRows(batch, rows);
            }
        }
        return rows;
    }

    private static List<Set<ColumnPath>> drainColumnKeys(MaskedScanRowGroupReader reader) {
        List<Set<ColumnPath>> columns = new ArrayList<>();
        while (reader.hasMore()) {
            try (ParquetRecordBatch batch = reader.nextBatch()) {
                columns.add(Set.copyOf(batch.columns().keySet()));
            }
        }
        return columns;
    }

    private static Tallies drainTallies(MaskedScanRowGroupReader reader) {
        drainRows(reader);
        return new Tallies(reader.rowsProduced(), reader.rowsMatched(), reader.recordFilterNanos());
    }

    private record Tallies(long rowsProduced, long rowsMatched, long recordFilterNanos) {}

    private List<Row> drainDecodeAllThenFilter(
            Path file, ParquetSchema fileSchema, ParquetSchema outputSchema, Predicate predicate) throws Exception {
        return drainDecodeAllThenFilter(file, fileSchema, outputSchema, predicate, Optional.empty());
    }

    /**
     * Decodes every row the read reaches - all of them, or the rows a row mask keeps - evaluates the predicate over
     * each batch, and keeps the surviving rows' output columns.
     */
    private List<Row> drainDecodeAllThenFilter(
            Path file,
            ParquetSchema fileSchema,
            ParquetSchema outputSchema,
            Predicate predicate,
            Optional<RowRanges> survivingRows)
            throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ReaderFixture fixture = openFixture(source, fileSchema, SegmentPool.getDefault());
            try (RowGroupFetch fetch = fixture.fetch()) {
                Optional<RowMask> rowMask = fixture.rowMask(survivingRows);
                return decodeAllThenFilter(fetch.columns(), fileSchema, outputSchema, predicate, rowMask);
            }
        }
    }

    private static List<Row> decodeAllThenFilter(
            List<FetchedColumnChunk> chunks,
            ParquetSchema fileSchema,
            ParquetSchema outputSchema,
            Predicate predicate,
            Optional<RowMask> rowMask) {
        List<Row> rows = new ArrayList<>();
        try (BatchRowGroupReader reader = new BatchRowGroupReader(
                TestDecodeBuffers.ample(),
                chunks,
                fileSchema,
                fileSchema,
                OptionalInt.empty(),
                rowMask,
                BatchForm.ASSEMBLED)) {
            while (reader.hasMore()) {
                try (ParquetRecordBatch batch = reader.nextBatch()) {
                    BitSet survivors = VectorizedPredicateEvaluator.eval(predicate, batch);
                    collectRows(FilteredRecordBatch.filtered(batch, survivors, outputSchema), rows);
                }
            }
        }
        return rows;
    }

    /** Appends every row of {@code batch}, each read across the columns the batch exposes. */
    private static void collectRows(ParquetRecordBatch batch, List<Row> rows) {
        Set<ColumnPath> columns = batch.columns().keySet();
        for (int row = 0; row < batch.rowCount(); row++) {
            ParquetRecord materialized = batch.materialize(row);
            Map<ColumnPath, Object> values = new HashMap<>();
            for (ColumnPath column : columns) {
                values.put(column, comparable(materialized.get(column)));
            }
            rows.add(new Row(values));
        }
    }

    /** A materialized value in a form two decode paths compare by value: binary as text, a list element by element. */
    private static Object comparable(Object value) {
        return switch (value) {
            case null -> null;
            case MemorySegment segment -> new String(segment.toArray(JAVA_BYTE), StandardCharsets.UTF_8);
            case List<?> list ->
                list.stream().map(MaskedScanRowGroupReaderTest::comparable).toList();
            case ParquetRecord struct -> comparableFields(struct);
            default -> value;
        };
    }

    private static Map<ColumnPath, Object> comparableFields(ParquetRecord struct) {
        Map<ColumnPath, Object> fields = new HashMap<>();
        for (int column = 0; column < struct.columnCount(); column++) {
            ColumnPath field = struct.columnPath(column);
            fields.put(field, comparable(struct.get(field)));
        }
        return fields;
    }

    /** One emitted row: its column values keyed by column path. */
    private record Row(Map<ColumnPath, Object> values) {}

    // --- reader construction ---

    /** What a test does with one open masked-scan reader over the fixture's single row group. */
    @FunctionalInterface
    private interface ScanAction<T> {
        T over(MaskedScanRowGroupReader reader);
    }

    /**
     * The knobs a masked scan is opened with: its batch cap, its batch form, any column-index row mask, and the pool
     * every fetch and decode buffer of the scan is borrowed from.
     */
    private record ScanOptions(
            OptionalInt batchSizeCap, BatchForm outputForm, Optional<RowRanges> survivingRows, SegmentPool pool) {

        static ScanOptions defaults() {
            return of(OptionalInt.empty(), BatchForm.ASSEMBLED);
        }

        static ScanOptions of(OptionalInt batchSizeCap, BatchForm outputForm) {
            return new ScanOptions(batchSizeCap, outputForm, Optional.empty(), SegmentPool.getDefault());
        }

        static ScanOptions overRows(RowRanges survivingRows) {
            return new ScanOptions(
                    OptionalInt.empty(), BatchForm.ASSEMBLED, Optional.of(survivingRows), SegmentPool.getDefault());
        }

        /** The same knobs over a test-owned pool, which lets the test assert that pool's borrow accounting. */
        ScanOptions withPool(SegmentPool pool) {
            return new ScanOptions(batchSizeCap, outputForm, survivingRows, pool);
        }
    }

    /**
     * Opens a masked scan over the fixture's single row group, fetching every column of {@code fileSchema}, and hands
     * the reader to {@code action}. The predicate's own columns are the filter leaves, split into the stages of a read;
     * a fetched column outside both the predicate and the output is never read.
     */
    private <T> T overMaskedScan(
            Path file,
            ParquetSchema fileSchema,
            ParquetSchema outputSchema,
            Predicate predicate,
            ScanOptions options,
            ScanAction<T> action)
            throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ReaderFixture fixture = openFixture(source, fileSchema, options.pool());
            try (RowGroupFetch fetch = fixture.fetch()) {
                Optional<RowMask> rowMask = fixture.rowMask(options.survivingRows());
                long rows = fixture.rowsToScan(options.survivingRows());
                return overChunks(fetch.columns(), fileSchema, outputSchema, predicate, options, rowMask, rows, action);
            }
        }
    }

    /** Opens a masked scan over {@code chunks}, the columns of one row group with {@code rows} rows to scan. */
    @SuppressWarnings("java:S107") // test factory: each parameter is one input of the scan under test
    private static <T> T overChunks(
            List<FetchedColumnChunk> chunks,
            ParquetSchema fileSchema,
            ParquetSchema outputSchema,
            Predicate predicate,
            ScanOptions options,
            Optional<RowMask> rowMask,
            long rows,
            ScanAction<T> action) {
        ScanStages stages = ScanStages.of(predicate, Predicate.columns(predicate), fileSchema);
        try (MaskedScanRowGroupReader reader = new MaskedScanRowGroupReader(
                TestDecodeBuffers.ample(options.pool()),
                chunks,
                fileSchema,
                outputSchema,
                stages,
                options.batchSizeCap(),
                rowMask,
                options.outputForm(),
                Optional.empty(),
                rows)) {
            return action.over(reader);
        }
    }

    private ReaderFixture openFixture(ByteRangeSource source, ParquetSchema fileSchema, SegmentPool pool) {
        FileMetaData footer = ParquetFormat.readFooter(source);
        assertThat(footer.rowGroups())
                .as("the fixture holds a single row group")
                .hasSize(1);
        RowGroupChunks chunks = TestRowGroupChunks.of(footer, 0, fileSchema, indexLoader(source));
        RowGroupFetcher fetcher = TestFetchers.over(source, fileSchema, fileSchema, pool);
        return new ReaderFixture(
                fetcher, RowGroupSurvivor.full(chunks), pageIndexes(fileSchema, chunks), chunks.numRows());
    }

    private static Map<ColumnPath, PageIndex> pageIndexes(ParquetSchema fileSchema, RowGroupChunks chunks) {
        Map<ColumnPath, PageIndex> indexes = new HashMap<>();
        for (ColumnPath leaf : fileSchema.leafColumns()) {
            indexes.put(leaf, chunks.pageLocations(leaf).orElseThrow());
        }
        return indexes;
    }

    private record ReaderFixture(
            RowGroupFetcher fetcher, RowGroupSurvivor survivor, Map<ColumnPath, PageIndex> pageIndexes, long numRows) {

        RowGroupFetch fetch() throws Exception {
            return fetcher.fetch(survivor, fetcher.planFor(survivor, Optional.empty()), BudgetReservation.NONE);
        }

        /** The decode-time mask for {@code survivingRows}, resolving the page index of each leaf. */
        Optional<RowMask> rowMask(Optional<RowRanges> survivingRows) {
            return survivingRows.map(rows -> new RowMask(rows, pageIndexes));
        }

        /** The rows a scan covers: the row group's rows, or the rows a mask keeps. */
        long rowsToScan(Optional<RowRanges> survivingRows) {
            return survivingRows.map(RowRanges::totalRows).orElse(numRows);
        }
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
                throw new UnsupportedOperationException("bloom filters not used in this test");
            }
        };
    }

    // --- fixtures ---

    private Path writeFortyRowsAcrossManyPages() throws Exception {
        Path file = tempDir.resolve("masked-scan-flat.parquet");
        ParquetSchema schema = fileSchema();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (long id = 0; id < ROW_COUNT; id++) {
            rows.add(row(id));
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, pageEvery(4))) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }
        return file;
    }

    /** A row of the flat fixture. Each third row has a null tag. */
    private static Map<ColumnPath, Object> row(long id) {
        Map<ColumnPath, Object> row = new HashMap<>();
        row.put(ID, id);
        row.put(V, id * 1.5);
        row.put(NAME, text("row-" + id));
        if (id % 3 != 0) {
            row.put(TAG, text("tag-" + id));
        }
        return row;
    }

    private static MemorySegment text(String value) {
        return MemorySegment.ofArray(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Flat columns beside a list column whose rows differ in length and mix null elements, empty lists and null lists,
     * written at a page limit that ends most list pages in the middle of a row.
     */
    private Path writeListsAcrossManyPages() throws Exception {
        Path file = tempDir.resolve("masked-scan-lists.parquet");
        ParquetSchema schema = listFileSchema();
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        for (int row = 0; row < ROW_COUNT; row++) {
            appendListRow(builder, row);
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, pageEvery(5))) {
            writer.writeBatch(builder.build());
        }
        return file;
    }

    private static void appendListRow(ParquetRecordBatchBuilder builder, int row) {
        builder.setLong(ID, row);
        builder.setBinary(NAME, text("row-" + row));
        if (isNullListRow(row)) {
            builder.endRow();
            return;
        }
        builder.beginList("items");
        for (int element = 0; element < listLength(row); element++) {
            if (isNullElement(row, element)) {
                builder.addNull();
            } else {
                builder.addLong(row * 10L + element);
            }
        }
        builder.endList();
        builder.endRow();
    }

    /** Rows at this stride hold no list at all; leaving the column unset writes the absent-list definition level. */
    private static boolean isNullListRow(int row) {
        return row % 7 == 5;
    }

    /** Rows at this stride hold an empty list; every other row holds one to three elements. */
    private static int listLength(int row) {
        if (row % 7 == 6) {
            return 0;
        }
        return 1 + (row % 3);
    }

    private static boolean isNullElement(int row, int element) {
        return (row + element) % 4 == 0;
    }

    /** A flat predicate column beside an optional struct with one optional leaf. */
    private Path writeStructsAcrossManyPages() throws Exception {
        Path file = tempDir.resolve("masked-scan-structs.parquet");
        ParquetSchema schema = structFileSchema();
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        for (int row = 0; row < ROW_COUNT; row++) {
            appendStructRow(builder, row);
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, pageEvery(4))) {
            writer.writeBatch(builder.build());
        }
        return file;
    }

    /** A row of the struct fixture. Each third row has a null struct, and each fifth a null label in its struct. */
    private static void appendStructRow(ParquetRecordBatchBuilder builder, int row) {
        builder.setLong(ID, row);
        if (row % 3 == 0) {
            builder.setNull(STRUCT);
        } else {
            builder.beginStruct(STRUCT);
            if (row % 5 == 0) {
                builder.setNull(LABEL);
            } else {
                builder.setBinary(LABEL, text("label-" + row));
            }
            builder.endStruct();
        }
        builder.endRow();
    }

    /** The uncompressed chunk of a required column of the zero-row fixture, one page per entry of {@code pages}. */
    private static FetchedColumnChunk plainChunk(ColumnPath path, byte[]... pages) throws IOException {
        ByteArrayOutputStream chunk = new ByteArrayOutputStream();
        for (byte[] page : pages) {
            chunk.write(page);
        }
        MemorySegment bytes = MemorySegment.ofArray(chunk.toByteArray()).asReadOnly();
        return new FetchedColumnChunk(
                path,
                CompressionCodec.UNCOMPRESSED,
                ZERO_ROW_FIXTURE_ROWS,
                0,
                0,
                List.of(new DataPageRun(bytes, 0)),
                Optional.empty(),
                PageSizeLimit.NONE);
    }

    /** A PLAIN page holding the longs from {@code first} to {@code end}, the end left out. */
    private static byte[] longPage(long first, long end) throws IOException {
        int rows = Math.toIntExact(end - first);
        ByteBuffer values = ByteBuffer.allocate(rows * Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (long value = first; value < end; value++) {
            values.putLong(value);
        }
        return plainPage(rows, values.array());
    }

    /** A PLAIN page holding the names of the rows from {@code first} to {@code end}, the end left out. */
    private static byte[] namePage(int first, int end) throws IOException {
        ByteArrayOutputStream values = new ByteArrayOutputStream();
        for (int row = first; row < end; row++) {
            byte[] name = ("row-" + row).getBytes(StandardCharsets.UTF_8);
            byte[] length = ByteBuffer.allocate(Integer.BYTES)
                    .order(ByteOrder.LITTLE_ENDIAN)
                    .putInt(name.length)
                    .array();
            values.write(length);
            values.write(name);
        }
        return plainPage(end - first, values.toByteArray());
    }

    private static byte[] plainPage(int rows, byte[] values) throws IOException {
        PageHeader header = new PageHeader(PageType.DATA_PAGE, values.length, values.length);
        header.setData_page_header(new DataPageHeader(rows, Encoding.PLAIN, Encoding.RLE, Encoding.RLE));
        ByteArrayOutputStream page = new ByteArrayOutputStream();
        Util.writePageHeader(header, page);
        page.write(values);
        return page.toByteArray();
    }

    private WriteOptions pageEvery(int values) {
        return WriteOptions.builder().tempDir(tempDir).pageValueLimit(values).build();
    }

    // --- schemas ---

    private static ParquetSchema fileSchema() {
        return flatSchema(
                requiredLeaf("id", PrimitiveKind.INT64),
                requiredLeaf("v", PrimitiveKind.DOUBLE),
                requiredLeaf("name", PrimitiveKind.BYTE_ARRAY),
                WriteFixtures.optionalLeaf("tag", PrimitiveKind.BYTE_ARRAY));
    }

    private static ParquetSchema listFileSchema() {
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group items = new SchemaNode.Group(
                "items", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        SchemaNode.Group root = new SchemaNode.Group(
                "schema",
                Repetition.REQUIRED,
                List.of(requiredLeaf("id", PrimitiveKind.INT64), requiredLeaf("name", PrimitiveKind.BYTE_ARRAY), items),
                Optional.empty(),
                -1);
        return new ParquetSchema(root);
    }

    private static ParquetSchema structFileSchema() {
        SchemaNode.Primitive label = WriteFixtures.optionalLeaf("label", PrimitiveKind.BYTE_ARRAY);
        SchemaNode.Group struct = new SchemaNode.Group("s", Repetition.OPTIONAL, List.of(label), Optional.empty(), -1);
        return WriteFixtures.schemaOf(requiredLeaf("id", PrimitiveKind.INT64), struct);
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children =
                Stream.of(leaves).map(SchemaNode.class::cast).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredLeaf(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.REQUIRED, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    // --- a per-row test ---

    /** A geometry test over the text of {@code column}. As a condition of its own, it runs in the last stage. */
    private static Predicate textTest(ColumnPath column, TextTest test) {
        return new Predicate.GeometryFilterPredicate(new TextFilter(column, test));
    }

    /** What a {@link TextFilter} asks of the text of one row. */
    @FunctionalInterface
    private interface TextTest {
        boolean holds(String text);
    }

    private record TextFilter(ColumnPath column, TextTest test) implements GeometryFilter<String> {

        @Override
        public Optional<Predicate.Spatial> pruningPredicate() {
            return Optional.empty();
        }

        @Override
        public String decode(MemorySegment wkb) {
            return new String(wkb.toArray(JAVA_BYTE), StandardCharsets.UTF_8);
        }

        @Override
        public boolean matches(String text) {
            return test.holds(text);
        }
    }
}
