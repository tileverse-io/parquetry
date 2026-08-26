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

import java.lang.foreign.MemorySegment;
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
import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
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
    private static final ColumnPath ELEMENT = ColumnPath.of("items", "list", "element");

    private static final int ROW_COUNT = 40;

    @TempDir
    Path tempDir;

    static Stream<Arguments> cases() {
        return Stream.of(
                Arguments.of(
                        "output-only",
                        Pred.and(Pred.col(ID).gtEq(10L), Pred.col(ID).lt(20L)),
                        Set.of(V, NAME),
                        10),
                Arguments.of("predicate-also-projected", Pred.col(ID).eq(25L), Set.of(ID, V), 1),
                Arguments.of("no-match", Pred.col(ID).gt(9_999L), Set.of(V, NAME), 0),
                Arguments.of("all-match", Pred.col(ID).gtEq(0L), Set.of(V, NAME), ROW_COUNT),
                Arguments.of("alternating", Pred.col(ID).notEq(0L), Set.of(ID, V, NAME), ROW_COUNT - 1));
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
                .as("%s: the reference read selects the rows the case names", label)
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
                .as("%s: the scan selects the rows the case names", label)
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
                .as("%s: the reference read selects the rows the case names", label)
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
                .as("%s: the reference read selects the rows the case names", label)
                .hasSize(matchingRows);
        assertThat(masked)
                .as("%s: the leaf the predicate and the output share is gathered to the surviving rows", label)
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
        List<Row> rows = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ReaderFixture fixture = openFixture(source, fileSchema, SegmentPool.getDefault());
            try (RowGroupFetch fetch = fixture.fetch();
                    BatchRowGroupReader reader = new BatchRowGroupReader(
                            TestDecodeBuffers.ample(),
                            fetch.columns(),
                            fileSchema,
                            fileSchema,
                            OptionalInt.empty(),
                            fixture.rowMask(survivingRows),
                            BatchForm.ASSEMBLED)) {
                while (reader.hasMore()) {
                    try (ParquetRecordBatch batch = reader.nextBatch()) {
                        BitSet survivors = VectorizedPredicateEvaluator.eval(predicate, batch);
                        collectRows(FilteredRecordBatch.filtered(batch, survivors, outputSchema), rows);
                    }
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
            default -> value;
        };
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
     * the reader to {@code action}. The predicate's own columns are the filter leaves; a fetched column outside both
     * the predicate and the output is never read.
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
            try (RowGroupFetch fetch = fixture.fetch();
                    MaskedScanRowGroupReader reader = new MaskedScanRowGroupReader(
                            TestDecodeBuffers.ample(options.pool()),
                            fetch.columns(),
                            fileSchema,
                            outputSchema,
                            Predicate.columns(predicate),
                            predicate,
                            options.batchSizeCap(),
                            fixture.rowMask(options.survivingRows()),
                            options.outputForm(),
                            Optional.empty(),
                            fixture.rowsToScan(options.survivingRows()))) {
                return action.over(reader);
            }
        }
    }

    private ReaderFixture openFixture(ByteRangeSource source, ParquetSchema fileSchema, SegmentPool pool) {
        FileMetaData footer = ParquetFormat.readFooter(source);
        assertThat(footer.rowGroups())
                .as("the fixture holds a single row group")
                .hasSize(1);
        RowGroupChunks chunks = RowGroupChunks.of(footer.rowGroups().get(0), fileSchema, indexLoader(source));
        RowGroupFetcher fetcher = TestFetchers.over(source, fileSchema, fileSchema, pool);
        return new ReaderFixture(
                fetcher, RowGroupSurvivor.full(chunks), offsetIndexes(fileSchema, chunks), chunks.numRows());
    }

    private static Map<ColumnPath, OffsetIndex> offsetIndexes(ParquetSchema fileSchema, RowGroupChunks chunks) {
        Map<ColumnPath, OffsetIndex> indexes = new HashMap<>();
        for (ColumnPath leaf : fileSchema.leafColumns()) {
            indexes.put(leaf, chunks.offsetIndex(leaf).orElseThrow());
        }
        return indexes;
    }

    private record ReaderFixture(
            RowGroupFetcher fetcher,
            RowGroupSurvivor survivor,
            Map<ColumnPath, OffsetIndex> offsetIndexes,
            long numRows) {

        RowGroupFetch fetch() throws Exception {
            return fetcher.fetch(survivor, fetcher.planFor(survivor, Optional.empty()), BudgetReservation.NONE);
        }

        /** The decode-time mask for {@code survivingRows}, resolving every leaf's offset index. */
        Optional<RowMask> rowMask(Optional<RowRanges> survivingRows) {
            return survivingRows.map(rows -> new RowMask(rows, offsetIndexes));
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

    private static Map<ColumnPath, Object> row(long id) {
        return Map.of(ID, id, V, id * 1.5, NAME, MemorySegment.ofArray(("row-" + id).getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * A flat predicate column beside a list column whose rows differ in length and mix null elements, empty lists and
     * null lists, written at a page limit that ends most list pages in the middle of a row.
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

    private WriteOptions pageEvery(int values) {
        return WriteOptions.builder().tempDir(tempDir).pageValueLimit(values).build();
    }

    // --- schemas ---

    private static ParquetSchema fileSchema() {
        return flatSchema(
                requiredLeaf("id", PrimitiveKind.INT64),
                requiredLeaf("v", PrimitiveKind.DOUBLE),
                requiredLeaf("name", PrimitiveKind.BYTE_ARRAY));
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
                List.of(requiredLeaf("id", PrimitiveKind.INT64), items),
                Optional.empty(),
                -1);
        return new ParquetSchema(root);
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
}
