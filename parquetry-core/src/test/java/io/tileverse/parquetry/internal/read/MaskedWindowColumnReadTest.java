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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.IntPredicate;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.Levels;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * A windowed masked reader emits exactly the surviving rows of every window, with the values, nulls and level windows
 * an eager reader would have produced for the same rows, while running fewer values through a value decoder. This holds
 * however a drain carves a page's rows into windows.
 */
class MaskedWindowColumnReadTest {

    private static final ColumnPath V = ColumnPath.of("v");
    private static final ColumnPath ELEMENT = ColumnPath.of("items", "list", "element");
    private static final int ROW_COUNT = 50;
    private static final int ROWS_PER_PAGE = 5;

    /** Page width of the fixture whose single window holds room for two separated runs of kept rows. */
    private static final int WIDE_ROWS_PER_PAGE = 12;

    @TempDir
    Path tempDir;

    /**
     * The row-keep patterns every fixture is read back under. The last one keeps two adjacent rows of the same page,
     * which a split drain lands in different windows: that is the shape whose earlier window can end on a kept null
     * row, with rows dropped between it and the window before.
     */
    static Stream<Arguments> survivorPatterns() {
        return Stream.of(
                Arguments.of("every-third", (IntPredicate) row -> row % 3 == 0),
                Arguments.of("first-of-each-page", (IntPredicate) row -> row % ROWS_PER_PAGE == 0),
                Arguments.of("last-of-each-page", (IntPredicate) row -> row % ROWS_PER_PAGE == ROWS_PER_PAGE - 1),
                Arguments.of("none", (IntPredicate) _ -> false),
                Arguments.of("all", (IntPredicate) _ -> true),
                Arguments.of("one-row", (IntPredicate) row -> row == 27),
                Arguments.of("last-two-of-each-page", (IntPredicate) row -> row % ROWS_PER_PAGE >= ROWS_PER_PAGE - 2));
    }

    /** Every survivor pattern read back under every window split. */
    static Stream<Arguments> survivorPatternsBySplit() {
        return survivorPatterns().flatMap(MaskedWindowColumnReadTest::acrossSplits);
    }

    private static Stream<Arguments> acrossSplits(Arguments pattern) {
        Object[] values = pattern.get();
        String label = (String) values[0];
        IntPredicate survives = (IntPredicate) values[1];
        return Stream.of(WindowSplit.values()).map(split -> Arguments.of(label, survives, split));
    }

    @ParameterizedTest(name = "{0}, {2}")
    @MethodSource("survivorPatternsBySplit")
    void maskedWindowsMatchTheEagerReaderForARequiredColumn(String label, IntPredicate survives, WindowSplit split)
            throws Exception {
        Path file = writeRequiredLongs();
        ParquetSchema schema = flatSchema(requiredInt64("v"));
        DrainResult eager = drainEagerKeeping(file, schema, V, survives);
        DrainResult masked = drainMasked(file, schema, V, survives, split);

        assertThat(masked.values()).as("%s: values", label).isEqualTo(eager.values());
        assertThat(masked.nulls()).as("%s: nulls", label).isEqualTo(eager.nulls());
        assertThat(masked.decodedValueCount())
                .as("%s: a masked read decodes no more values than survive", label)
                .isLessThanOrEqualTo(eager.decodedValueCount());
    }

    @ParameterizedTest(name = "{0}, {2}")
    @MethodSource("survivorPatternsBySplit")
    void maskedWindowsMatchTheEagerReaderForANullableColumn(String label, IntPredicate survives, WindowSplit split)
            throws Exception {
        Path file = writeNullableLongs();
        ParquetSchema schema = flatSchema(optionalInt64("v"));
        DrainResult eager = drainEagerKeeping(file, schema, V, survives);
        DrainResult masked = drainMasked(file, schema, V, survives, split);

        assertThat(masked.values()).as("%s: values", label).isEqualTo(eager.values());
        assertThat(masked.nulls()).as("%s: nulls", label).isEqualTo(eager.nulls());
        assertThat(masked.defLevels())
                .as("%s: the window's def levels mark the same rows absent as its validity", label)
                .isEqualTo(presenceLevels(masked.nulls()));
    }

    /** The def levels a top-level optional column has for the given nullness: one for present, zero for absent. */
    private static List<Integer> presenceLevels(List<Boolean> nulls) {
        return nulls.stream().map(isNull -> isNull ? 0 : 1).toList();
    }

    @ParameterizedTest(name = "{0}, {2}")
    @MethodSource("survivorPatternsBySplit")
    void maskedWindowsMatchTheEagerReaderForAListColumn(String label, IntPredicate survives, WindowSplit split)
            throws Exception {
        Path file = writeListsOfLongs();
        ParquetSchema schema = listOfInt64Schema();
        DrainResult eager = drainEagerKeeping(file, schema, ELEMENT, survives);
        DrainResult masked = drainMasked(file, schema, ELEMENT, survives, split);

        assertThat(masked.values()).as("%s: values", label).isEqualTo(eager.values());
        assertThat(masked.nulls()).as("%s: nulls", label).isEqualTo(eager.nulls());
        assertThat(masked.repLevels()).as("%s: rep levels", label).isEqualTo(eager.repLevels());
        assertThat(masked.defLevels()).as("%s: def levels", label).isEqualTo(eager.defLevels());
        assertThat(masked.decodedValueCount())
                .as("%s: a masked read decodes no more values than survive", label)
                .isLessThanOrEqualTo(eager.decodedValueCount());
    }

    @Test
    void aWindowKeepingEveryRowOfAPageDecodesItInOneBulkCall() throws Exception {
        Path file = writeRequiredLongs();
        ParquetSchema schema = flatSchema(requiredInt64("v"));
        DrainResult masked = drainMasked(file, schema, V, _ -> true, WindowSplit.WHOLE_PAGE);

        assertThat(masked.decodedValueCount())
                .as("every row's value was decoded")
                .isEqualTo(ROW_COUNT);
        assertThat(masked.windowDecodeCallCount())
                .as("one bulk call per page-wide window, not one per value")
                .isEqualTo(ROW_COUNT / ROWS_PER_PAGE);
    }

    @Test
    void aWindowKeepingEveryRowOfANullablePageDecodesOneRunAtATime() throws Exception {
        Path file = writeNullableLongs();
        ParquetSchema schema = flatSchema(optionalInt64("v"));
        DrainResult masked = drainMasked(file, schema, V, _ -> true, WindowSplit.WHOLE_PAGE);

        assertThat(masked.windowDecodeCallCount())
                .as("the present rows between the null ones decode in runs, not value by value")
                .isEqualTo(nonNullRunCount());
    }

    /**
     * Runs of adjacent present rows in the nullable fixture, counted page by page: a run opens on a present row that
     * either starts a page or follows a null one, and no run spans a page because no window does.
     */
    private static int nonNullRunCount() {
        int runs = 0;
        for (int row = 0; row < ROW_COUNT; row++) {
            if (isNullRow(row)) {
                continue;
            }
            if (row % ROWS_PER_PAGE == 0 || isNullRow(row - 1)) {
                runs++;
            }
        }
        return runs;
    }

    @Test
    void aWindowKeepingEveryRowOfAListPageDecodesFewerCallsThanValues() throws Exception {
        Path file = writeListsOfLongs();
        ParquetSchema schema = listOfInt64Schema();
        DrainResult eager = drainEagerKeeping(file, schema, ELEMENT, _ -> true);
        DrainResult masked = drainMasked(file, schema, ELEMENT, _ -> true, WindowSplit.WHOLE_PAGE);

        assertThat(masked.values()).isEqualTo(eager.values());
        assertThat(masked.windowDecodeCallCount())
                .as("a repeated column's fully surviving windows decode in runs too")
                .isLessThan(masked.decodedValueCount());
    }

    @Test
    void aWindowWhoseKeptRowsNeverAdjoinDecodesOneCallPerValue() throws Exception {
        Path file = writeRequiredLongs();
        ParquetSchema schema = flatSchema(requiredInt64("v"));
        DrainResult masked = drainMasked(file, schema, V, row -> row % 2 == 0, WindowSplit.WHOLE_PAGE);

        assertThat(masked.windowDecodeCallCount())
                .as("no two kept rows adjoin, hence no run is longer than one value")
                .isEqualTo(masked.decodedValueCount());
    }

    @Test
    void aWindowKeepingTwoRunsOfRowsDecodesOneBulkCallPerRun() throws Exception {
        Path file = writeRequiredLongs(WIDE_ROWS_PER_PAGE);
        ParquetSchema schema = flatSchema(requiredInt64("v"));
        DrainResult masked = overColumn(file, schema, V, ValueDecode.WINDOWED_MASK, reader -> {
            BitSet survivors = new BitSet(WIDE_ROWS_PER_PAGE);
            survivors.set(2, 6);
            survivors.set(8, 10);
            DrainedRows drained = new DrainedRows();
            drained.addWindow(reader.readMaskedWindow(WIDE_ROWS_PER_PAGE, survivors, new ArrayList<>()));
            return drained.toResult(reader.decodedValueCount(), reader.windowDecodeCallCount());
        });

        assertThat(masked.values()).containsExactly(2L, 3L, 4L, 5L, 8L, 9L);
        assertThat(masked.decodedValueCount()).isEqualTo(6);
        assertThat(masked.windowDecodeCallCount())
                .as("the two runs of kept rows cost one bulk call each, not one call per row")
                .isEqualTo(2);
    }

    @Test
    void aWindowLongerThanTheCurrentPageIsRejected() throws Exception {
        Path file = writeRequiredLongs();
        ParquetSchema schema = flatSchema(requiredInt64("v"));
        overColumn(file, schema, V, ValueDecode.WINDOWED_MASK, reader -> {
            int servableRows = reader.logicalRowsRemainingInCurrentPage();
            int tooManyRows = servableRows + 1;
            BitSet survivors = survivorsOf(0, tooManyRows, _ -> true);
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> reader.readMaskedWindow(tooManyRows, survivors, new ArrayList<>()))
                    .withMessageContainingAll(String.valueOf(tooManyRows), String.valueOf(servableRows));
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> reader.skipMaskedWindow(tooManyRows))
                    .withMessageContainingAll(String.valueOf(tooManyRows), String.valueOf(servableRows));
            return null;
        });
    }

    @Test
    void theMaskedEntryPointsRejectAnEagerReader() throws Exception {
        Path file = writeRequiredLongs();
        ParquetSchema schema = flatSchema(requiredInt64("v"));
        overColumn(file, schema, V, ValueDecode.EAGER, reader -> {
            BitSet survivors = survivorsOf(0, 1, _ -> true);
            assertThatIllegalStateException()
                    .isThrownBy(() -> reader.readMaskedWindow(1, survivors, new ArrayList<>()))
                    .withMessageContaining("windowed value decode");
            assertThatIllegalStateException()
                    .isThrownBy(() -> reader.skipMaskedWindow(1))
                    .withMessageContaining("windowed value decode");
            assertThatIllegalStateException()
                    .isThrownBy(() -> reader.readMaskedSpanningRow(true, new ArrayList<>()))
                    .withMessageContaining("windowed value decode");
            return null;
        });
    }

    @Test
    void windowsThatDropEveryRowOfTwoPagesStepOverThemUndecoded() throws Exception {
        Path file = writeRequiredLongs();
        ParquetSchema schema = flatSchema(requiredInt64("v"));
        overColumn(file, schema, V, ValueDecode.WINDOWED_MASK, reader -> {
            skipWholePage(reader);
            skipWholePage(reader);

            assertThat(reader.decodedDataPageCount())
                    .as("neither page of dead rows was decompressed")
                    .isZero();
            assertThat(reader.skippedDataPageCount())
                    .as("both pages of dead rows count as pruned")
                    .isEqualTo(2);

            assertThat(readWholePageKeepingEveryRow(reader))
                    .as("the page after them still opens at its own first row")
                    .containsExactly(10L, 11L, 12L, 13L, 14L);
            assertThat(reader.decodedDataPageCount())
                    .as("only the page a surviving row asked for was decompressed")
                    .isEqualTo(1);
            return null;
        });
    }

    /** Steps the reader over every row its current page can serve, keeping none of them. */
    private static void skipWholePage(BatchColumnReader reader) {
        reader.skipMaskedWindow(reader.logicalRowsRemainingInCurrentPage());
    }

    /** Reads every row the reader's current page can serve, keeping all of them. */
    private static List<Long> readWholePageKeepingEveryRow(BatchColumnReader reader) {
        int windowRows = reader.logicalRowsRemainingInCurrentPage();
        BitSet survivors = survivorsOf(0, windowRows, _ -> true);
        BatchColumnReader.MaskedWindow window = reader.readMaskedWindow(windowRows, survivors, new ArrayList<>());
        LongVector vector = (LongVector) window.vector();
        List<Long> values = new ArrayList<>();
        for (int i = 0; i < vector.size(); i++) {
            values.add(vector.getLong(i));
        }
        return values;
    }

    @Test
    void aWholeBatchReadRejectsAWindowedReader() throws Exception {
        Path file = writeRequiredLongs();
        ParquetSchema schema = flatSchema(requiredInt64("v"));
        overColumn(file, schema, V, ValueDecode.WINDOWED_MASK, reader -> {
            assertThatIllegalStateException()
                    .isThrownBy(() -> reader.readBatch(1, new ArrayList<>()))
                    .withMessageContaining("one window at a time");
            return null;
        });
    }

    // --- drains ---

    /** How many of a page's still-servable rows one window takes. */
    private enum WindowSplit {

        /** One window per page: every row the page can still serve. */
        WHOLE_PAGE {
            @Override
            int windowRows(int rowsServableByPage) {
                return rowsServableByPage;
            }
        },

        /** Several windows per page: half the servable rows at a time, never fewer than one. */
        HALVES {
            @Override
            int windowRows(int rowsServableByPage) {
                return Math.max(1, rowsServableByPage / 2);
            }
        };

        abstract int windowRows(int rowsServableByPage);
    }

    /**
     * Drains the masked reader window by window: each window is a run of the rows the current page can still serve, as
     * {@code split} carves them, and the survivors are the window's rows that {@code survives} accepts.
     */
    private DrainResult drainMasked(
            Path file, ParquetSchema schema, ColumnPath column, IntPredicate survives, WindowSplit split)
            throws Exception {
        return overColumn(file, schema, column, ValueDecode.WINDOWED_MASK, reader -> {
            DrainedRows drained = new DrainedRows();
            List<AutoCloseable> acquiredBuffers = new ArrayList<>();
            int firstRow = 0;
            while (reader.hasMore()) {
                int rowsServableByPage = reader.logicalRowsRemainingInCurrentPage();
                if (rowsServableByPage == 0) {
                    drained.addWindow(reader.readMaskedSpanningRow(survives.test(firstRow), acquiredBuffers));
                    firstRow++;
                    continue;
                }
                int windowRows = split.windowRows(rowsServableByPage);
                BitSet survivors = survivorsOf(firstRow, windowRows, survives);
                if (survivors.isEmpty()) {
                    reader.skipMaskedWindow(windowRows);
                } else {
                    drained.addWindow(reader.readMaskedWindow(windowRows, survivors, acquiredBuffers));
                }
                firstRow += windowRows;
            }
            return drained.toResult(reader.decodedValueCount(), reader.windowDecodeCallCount());
        });
    }

    /** Drains an eager reader over every row and keeps the ones {@code survives} accepts: the parity oracle. */
    private DrainResult drainEagerKeeping(Path file, ParquetSchema schema, ColumnPath column, IntPredicate survives)
            throws Exception {
        return overColumn(file, schema, column, ValueDecode.EAGER, reader -> {
            DrainedRows drained = new DrainedRows();
            List<AutoCloseable> acquiredBuffers = new ArrayList<>();
            int firstRow = 0;
            while (reader.hasMore()) {
                int windowRows = reader.logicalRowsRemainingInCurrentPage();
                if (windowRows == 0) {
                    drained.addSpanningRow(reader.readSpanningRow(acquiredBuffers), survives.test(firstRow));
                    firstRow++;
                    continue;
                }
                readEagerWindow(reader, windowRows, firstRow, survives, acquiredBuffers, drained);
                firstRow += windowRows;
            }
            return drained.toResult(reader.decodedValueCount(), reader.windowDecodeCallCount());
        });
    }

    /** Reads one whole window through the eager reader and keeps the rows {@code survives} accepts. */
    private static void readEagerWindow(
            BatchColumnReader reader,
            int windowRows,
            int firstRow,
            IntPredicate survives,
            List<AutoCloseable> acquiredBuffers,
            DrainedRows drained) {
        Levels pageRepLevels = reader.currentPageRepLevels();
        Levels pageDefLevels = reader.currentPageDefLevels();
        int windowFirstValue = reader.valuesConsumedInCurrentPage();
        int windowValues = reader.valuesForLogicalRows(windowRows);
        int[] repWindow = levelWindow(pageRepLevels, windowFirstValue, windowValues);
        int[] defWindow = levelWindow(pageDefLevels, windowFirstValue, windowValues);
        LongVector vector = (LongVector) reader.readBatch(windowValues, acquiredBuffers);
        int slot = 0;
        for (int row = 0; row < windowRows; row++) {
            int rowValues = (repWindow == null) ? 1 : rowLength(repWindow, slot);
            if (survives.test(firstRow + row)) {
                for (int i = 0; i < rowValues; i++) {
                    drained.add(vector, slot + i, repWindow, defWindow);
                }
            }
            slot += rowValues;
        }
    }

    private static int[] levelWindow(Levels levels, int from, int count) {
        if (levels == null) {
            return null;
        }
        return levels.toArray(from, count);
    }

    /** Entries from {@code start} up to the next row-start marker: the length of the row opening at {@code start}. */
    private static int rowLength(int[] repLevels, int start) {
        int end = start + 1;
        while (end < repLevels.length && repLevels[end] != 0) {
            end++;
        }
        return end - start;
    }

    private static BitSet survivorsOf(int firstRow, int windowRows, IntPredicate survives) {
        BitSet survivors = new BitSet(windowRows);
        for (int row = 0; row < windowRows; row++) {
            if (survives.test(firstRow + row)) {
                survivors.set(row);
            }
        }
        return survivors;
    }

    /** The values, nulls and level entries of the kept rows, in read order. */
    private static final class DrainedRows {

        private final List<Long> values = new ArrayList<>();
        private final List<Boolean> nulls = new ArrayList<>();
        private final List<Integer> repLevels = new ArrayList<>();
        private final List<Integer> defLevels = new ArrayList<>();

        void addWindow(BatchColumnReader.MaskedWindow window) {
            addAll((LongVector) window.vector(), window.repLevels(), window.defLevels());
        }

        void addSpanningRow(BatchColumnReader.SpanningRow row, boolean keep) {
            if (!keep) {
                return;
            }
            addAll((LongVector) row.vector(), row.repLevels(), row.defLevels());
        }

        private void addAll(LongVector vector, Levels rep, Levels def) {
            int[] repWindow = levelWindow(rep, 0, (rep == null) ? 0 : rep.size());
            int[] defWindow = levelWindow(def, 0, (def == null) ? 0 : def.size());
            for (int i = 0; i < vector.size(); i++) {
                add(vector, i, repWindow, defWindow);
            }
        }

        void add(LongVector vector, int index, int[] repWindow, int[] defWindow) {
            boolean isNull = vector.isNull(index);
            nulls.add(isNull);
            values.add(isNull ? null : vector.getLong(index));
            if (repWindow != null) {
                repLevels.add(repWindow[index]);
            }
            if (defWindow != null) {
                defLevels.add(defWindow[index]);
            }
        }

        DrainResult toResult(long decodedValueCount, long windowDecodeCallCount) {
            return new DrainResult(values, nulls, repLevels, defLevels, decodedValueCount, windowDecodeCallCount);
        }
    }

    private record DrainResult(
            List<Long> values,
            List<Boolean> nulls,
            List<Integer> repLevels,
            List<Integer> defLevels,
            long decodedValueCount,
            long windowDecodeCallCount) {}

    // --- reader construction ---

    /** What a test does with one open column reader over the file's single row group. */
    @FunctionalInterface
    private interface ColumnDrain {
        DrainResult over(BatchColumnReader reader);
    }

    /** What a test does with one fetched column chunk of the file's single row group. */
    @FunctionalInterface
    private interface ChunkAction<T> {
        T over(FetchedColumnChunk chunk, SchemaNode.Primitive leaf);
    }

    /**
     * Opens {@code column} of the file's first row group in {@code valueDecode} mode and hands the reader to
     * {@code drain}. The reader is built with neither a row mask nor an offset index, the index-free shape a driver
     * takes when the column index proved nothing.
     */
    private DrainResult overColumn(
            Path file, ParquetSchema schema, ColumnPath column, ValueDecode valueDecode, ColumnDrain drain)
            throws Exception {
        return overChunk(file, schema, column, (chunk, leaf) -> {
            BatchColumnReader reader =
                    new BatchColumnReader(TestDecodeBuffers.ample(), chunk, leaf, null, null, valueDecode);
            try {
                return drain.over(reader);
            } finally {
                reader.close();
            }
        });
    }

    /** Fetches {@code column} of the file's first row group and hands the chunk and its leaf to {@code action}. */
    private <T> T overChunk(Path file, ParquetSchema schema, ColumnPath column, ChunkAction<T> action)
            throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            RowGroupChunks chunks = RowGroupChunks.of(footer.rowGroups().get(0), schema, indexLoader(source));
            RowGroupFetcher fetcher = TestFetchers.over(source, schema, schema, SegmentPool.getDefault());
            RowGroupSurvivor survivor = RowGroupSurvivor.full(chunks);
            try (RowGroupFetch fetch =
                    fetcher.fetch(survivor, fetcher.planFor(survivor, Optional.empty()), BudgetReservation.NONE)) {
                SchemaNode.Primitive leaf =
                        (SchemaNode.Primitive) schema.find(column).orElseThrow();
                return action.over(fetch.columns().get(0), leaf);
            }
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

    private Path writeRequiredLongs() throws Exception {
        return writeRequiredLongs(ROWS_PER_PAGE);
    }

    private Path writeRequiredLongs(int rowsPerPage) throws Exception {
        Path file = tempDir.resolve("masked-required-" + rowsPerPage + ".parquet");
        ParquetSchema schema = flatSchema(requiredInt64("v"));
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int v = 0; v < ROW_COUNT; v++) {
            rows.add(requiredRow(v));
        }
        try (ParquetFileWriter writer =
                ParquetFileWriter.create(Files.newOutputStream(file), schema, pageEvery(rowsPerPage))) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }
        return file;
    }

    private Path writeNullableLongs() throws Exception {
        Path file = tempDir.resolve("masked-nullable.parquet");
        ParquetSchema schema = flatSchema(optionalInt64("v"));
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int v = 0; v < ROW_COUNT; v++) {
            rows.add(nullableRow(v));
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, pageEvery())) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }
        return file;
    }

    /**
     * A list column whose rows differ in length and mix null elements, empty lists and null lists, written at a page
     * limit that ends most pages in the middle of a row.
     */
    private Path writeListsOfLongs() throws Exception {
        Path file = tempDir.resolve("masked-lists.parquet");
        ParquetSchema schema = listOfInt64Schema();
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        for (int row = 0; row < ROW_COUNT; row++) {
            appendListRow(builder, row);
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, pageEvery())) {
            writer.writeBatch(builder.build());
        }
        return file;
    }

    private static void appendListRow(ParquetRecordBatchBuilder builder, int row) {
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

    private static ParquetSchema listOfInt64Schema() {
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group items = new SchemaNode.Group(
                "items", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(items), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private WriteOptions pageEvery() {
        return pageEvery(ROWS_PER_PAGE);
    }

    private WriteOptions pageEvery(int rowsPerPage) {
        return WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(rowsPerPage)
                .build();
    }

    /** Every third row is written as null in the nullable fixture. */
    private static boolean isNullRow(int row) {
        return row % 3 == 0;
    }

    private static Map<ColumnPath, Object> requiredRow(int v) {
        return Map.of(V, (long) v);
    }

    private static Map<ColumnPath, Object> nullableRow(int v) {
        Map<ColumnPath, Object> values = new HashMap<>();
        if (!isNullRow(v)) {
            values.put(V, (long) v);
        }
        return values;
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children = Stream.of(leaves).map(f -> (SchemaNode) f).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt64(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive optionalInt64(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }
}
