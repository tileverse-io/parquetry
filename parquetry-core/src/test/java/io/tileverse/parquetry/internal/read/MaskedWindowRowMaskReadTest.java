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
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
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
 * A windowed masked reader whose column-index row mask narrowed the row group reads in the mask's surviving row space:
 * it emits exactly the rows an eager reader under the same mask emits, with the same values, nulls and definition
 * levels, while running fewer values through a value decoder. This holds however a drain carves a page's surviving rows
 * into windows, and whether the mask drops whole pages or row ranges inside them.
 */
class MaskedWindowRowMaskReadTest {

    private static final ColumnPath V = ColumnPath.of("v");
    private static final ColumnPath ELEMENT = ColumnPath.of("items", "list", "element");
    private static final int ROW_COUNT = 50;
    private static final int ROWS_PER_PAGE = 5;

    /** The mask that keeps row ranges inside pages: no page is either kept whole or dropped whole. */
    private static final RowRanges RANGES_INSIDE_PAGES =
            new RowRanges(List.of(new Range(3, 6), new Range(11, 11), new Range(22, 27), new Range(38, 41)));

    @TempDir
    Path tempDir;

    /**
     * The row masks every fixture is read back under. Pages hold five rows each: the first mask keeps whole pages and
     * drops the seven between them, the second cuts row ranges inside pages, and the third keeps the row group whole.
     */
    static Stream<Arguments> masks() {
        return Stream.of(
                Arguments.of(
                        "whole-pages-dropped",
                        new RowRanges(List.of(new Range(0, 4), new Range(20, 24), new Range(45, 49)))),
                Arguments.of("ranges-inside-pages", RANGES_INSIDE_PAGES),
                Arguments.of("keeps-everything", RowRanges.all(ROW_COUNT)));
    }

    /**
     * The window-keep patterns every mask is read back under, indexed over the mask's surviving rows: that dense row
     * space is what both readers walk. The last one keeps adjacent pairs separated by gaps, the shape whose earlier
     * window can end on a kept null row with rows dropped between it and the window before.
     */
    static Stream<Arguments> survivorPatterns() {
        return Stream.of(
                Arguments.of("every-third", (IntPredicate) row -> row % 3 == 0),
                Arguments.of("none", (IntPredicate) _ -> false),
                Arguments.of("all", (IntPredicate) _ -> true),
                Arguments.of("one-row", (IntPredicate) row -> row == 7),
                Arguments.of("last-two-of-every-five", (IntPredicate) row -> row % 5 >= 3));
    }

    /** Every mask read back under every window-keep pattern and every window split. */
    static Stream<Arguments> caseMatrix() {
        return masks().flatMap(MaskedWindowRowMaskReadTest::acrossPatterns);
    }

    private static Stream<Arguments> acrossPatterns(Arguments mask) {
        String maskLabel = (String) mask.get()[0];
        RowRanges rows = (RowRanges) mask.get()[1];
        return survivorPatterns().flatMap(pattern -> {
            String label = maskLabel + " / " + pattern.get()[0];
            IntPredicate survives = (IntPredicate) pattern.get()[1];
            return Stream.of(WindowSplit.values()).map(split -> Arguments.of(label, rows, survives, split));
        });
    }

    @ParameterizedTest(name = "{0}, {3}")
    @MethodSource("caseMatrix")
    void maskedWindowsMatchTheEagerMaskedReaderForARequiredColumn(
            String label, RowRanges mask, IntPredicate survives, WindowSplit split) throws Exception {
        Path file = writeRequiredLongs();
        ParquetSchema schema = flatSchema(requiredInt64("v"));
        DrainResult eager = drainEagerKeeping(file, schema, V, mask, survives);
        DrainResult windowed = drainWindowed(file, schema, V, mask, survives, split);

        assertThat(windowed.values()).as("%s: values", label).isEqualTo(eager.values());
        assertThat(windowed.nulls()).as("%s: nulls", label).isEqualTo(eager.nulls());
        assertThat(windowed.decodedValueCount())
                .as("%s: a windowed read decodes no more values than the eager masked read", label)
                .isLessThanOrEqualTo(eager.decodedValueCount());
        assertThat(windowed.decodedDataPageCount())
                .as("%s: both readers reach the same pages", label)
                .isEqualTo(eager.decodedDataPageCount());
    }

    @ParameterizedTest(name = "{0}, {3}")
    @MethodSource("caseMatrix")
    void maskedWindowsMatchTheEagerMaskedReaderForANullableColumn(
            String label, RowRanges mask, IntPredicate survives, WindowSplit split) throws Exception {
        Path file = writeNullableLongs();
        ParquetSchema schema = flatSchema(optionalInt64("v"));
        DrainResult eager = drainEagerKeeping(file, schema, V, mask, survives);
        DrainResult windowed = drainWindowed(file, schema, V, mask, survives, split);

        assertThat(windowed.values()).as("%s: values", label).isEqualTo(eager.values());
        assertThat(windowed.nulls()).as("%s: nulls", label).isEqualTo(eager.nulls());
        assertThat(windowed.defLevels()).as("%s: def levels", label).isEqualTo(eager.defLevels());
        assertThat(windowed.decodedValueCount())
                .as("%s: a windowed read decodes no more values than the eager masked read", label)
                .isLessThanOrEqualTo(eager.decodedValueCount());
    }

    /**
     * The absolute form of the parity assertion: the required fixture writes each row's index as its value, hence a
     * drain that keeps every row it is offered must emit exactly the row indexes the mask kept, in order.
     */
    @Test
    void aWindowedDrainKeepingEveryRowEmitsTheMasksSurvivingRowIndexes() throws Exception {
        Path file = writeRequiredLongs();
        ParquetSchema schema = flatSchema(requiredInt64("v"));

        DrainResult windowed = drainWindowed(file, schema, V, RANGES_INSIDE_PAGES, _ -> true, WindowSplit.HALVES);

        assertThat(windowed.values()).isEqualTo(rowIndexesOf(RANGES_INSIDE_PAGES));
    }

    /** The row indexes {@code mask} keeps, ascending. */
    private static List<Long> rowIndexesOf(RowRanges mask) {
        List<Long> rows = new ArrayList<>();
        for (Range range : mask.ranges()) {
            for (long row = range.first(); row <= range.last(); row++) {
                rows.add(row);
            }
        }
        return rows;
    }

    @Test
    void aMaskDroppingWholePagesLeavesThemUndecoded() throws Exception {
        Path file = writeRequiredLongs();
        ParquetSchema schema = flatSchema(requiredInt64("v"));
        RowRanges mask = new RowRanges(List.of(new Range(0, 4), new Range(20, 24), new Range(45, 49)));

        DrainResult windowed = drainWindowed(file, schema, V, mask, _ -> true, WindowSplit.WHOLE_PAGE);

        assertThat(windowed.values()).hasSize((int) mask.totalRows());
        assertThat(windowed.decodedDataPageCount())
                .as("only the three pages the mask reaches are decoded")
                .isEqualTo(3);
        assertThat(windowed.skippedDataPageCount())
                .as("the seven pages outside the mask are stepped over")
                .isEqualTo(7);
    }

    @Test
    void aWindowedReadDecodesFarFewerValuesThanTheEagerMaskedRead() throws Exception {
        Path file = writeRequiredLongs();
        ParquetSchema schema = flatSchema(requiredInt64("v"));
        RowRanges mask = new RowRanges(List.of(new Range(3, 6), new Range(22, 27), new Range(38, 41)));
        IntPredicate oneSurvivingRow = row -> row == 5;

        DrainResult eager = drainEagerKeeping(file, schema, V, mask, oneSurvivingRow);
        DrainResult windowed = drainWindowed(file, schema, V, mask, oneSurvivingRow, WindowSplit.HALVES);

        assertThat(windowed.values()).isEqualTo(eager.values()).hasSize(1);
        assertThat(windowed.decodedValueCount())
                .as("only the kept row's value reaches a decoder")
                .isEqualTo(1)
                .isLessThan(eager.decodedValueCount());
    }

    /**
     * Every value-decode mode places a mask's rows through the offset index; none of them may read the column blind.
     */
    static Stream<ValueDecode> valueDecodes() {
        return Stream.of(ValueDecode.values());
    }

    @ParameterizedTest
    @MethodSource("valueDecodes")
    void aRowMaskWithoutAnOffsetIndexIsRejected(ValueDecode valueDecode) throws Exception {
        Path file = writeRequiredLongs();
        ParquetSchema schema = flatSchema(requiredInt64("v"));
        RowRanges mask = RowRanges.all(ROW_COUNT);
        overChunk(file, schema, V, mask, (chunk, leaf, _) -> {
            assertThatIllegalArgumentException()
                    .isThrownBy(() ->
                            new BatchColumnReader(TestDecodeBuffers.ample(), chunk, leaf, mask, null, valueDecode))
                    .withMessageContainingAll(V.dot(), "offset index");
            return null;
        });
    }

    @Test
    void theWindowedDecodeRejectsARowMaskOnARepeatedColumn() throws Exception {
        Path file = writeListsOfLongs();
        ParquetSchema schema = listOfInt64Schema();
        RowRanges mask = RowRanges.all(ROW_COUNT);
        overChunk(file, schema, ELEMENT, mask, (chunk, leaf, offsetIndex) -> {
            assertThatIllegalArgumentException()
                    .isThrownBy(() -> new BatchColumnReader(
                            TestDecodeBuffers.ample(), chunk, leaf, mask, offsetIndex, ValueDecode.WINDOWED_MASK))
                    .withMessageContainingAll(ELEMENT.dot(), "flat columns");
            return null;
        });
    }

    @Test
    void theMaskedEntryPointsRejectAnEagerMaskedReader() throws Exception {
        Path file = writeRequiredLongs();
        ParquetSchema schema = flatSchema(requiredInt64("v"));
        RowRanges mask = RowRanges.all(ROW_COUNT);
        overColumn(file, schema, V, mask, ValueDecode.EAGER, reader -> {
            BitSet survivors = new BitSet(1);
            survivors.set(0);
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

    // --- drains ---

    /** How many of a page's still-servable surviving rows one window takes. */
    private enum WindowSplit {

        /** One window per page: every surviving row the page can still serve. */
        WHOLE_PAGE {
            @Override
            int windowRows(int rowsServableByPage) {
                return rowsServableByPage;
            }
        },

        /** Several windows per page: half the servable surviving rows at a time, never fewer than one. */
        HALVES {
            @Override
            int windowRows(int rowsServableByPage) {
                return Math.max(1, rowsServableByPage / 2);
            }
        };

        abstract int windowRows(int rowsServableByPage);
    }

    /**
     * Drains the windowed masked reader window by window: each window is a run of the surviving rows the current page
     * can still serve, as {@code split} carves them, and the kept rows are the window's rows that {@code survives}
     * accepts. The row counter walks the mask's surviving rows, the row space both readers share.
     */
    private DrainResult drainWindowed(
            Path file,
            ParquetSchema schema,
            ColumnPath column,
            RowRanges mask,
            IntPredicate survives,
            WindowSplit split)
            throws Exception {
        return overColumn(file, schema, column, mask, ValueDecode.WINDOWED_MASK, reader -> {
            DrainedRows drained = new DrainedRows();
            List<AutoCloseable> acquiredBuffers = new ArrayList<>();
            int firstRow = 0;
            while (reader.hasMore()) {
                int rowsServableByPage = reader.logicalRowsRemainingInCurrentPage();
                assertThat(rowsServableByPage)
                        .as("a masked flat page always serves at least one surviving row")
                        .isPositive();
                int windowRows = split.windowRows(rowsServableByPage);
                BitSet survivors = survivorsOf(firstRow, windowRows, survives);
                if (survivors.isEmpty()) {
                    reader.skipMaskedWindow(windowRows);
                } else {
                    drained.addWindow(reader.readMaskedWindow(windowRows, survivors, acquiredBuffers));
                }
                firstRow += windowRows;
            }
            return drained.toResult(reader);
        });
    }

    /**
     * Drains an eager reader under the same mask over every surviving row and keeps the ones {@code survives} accepts:
     * the parity oracle.
     */
    private DrainResult drainEagerKeeping(
            Path file, ParquetSchema schema, ColumnPath column, RowRanges mask, IntPredicate survives)
            throws Exception {
        return overColumn(file, schema, column, mask, ValueDecode.EAGER, reader -> {
            DrainedRows drained = new DrainedRows();
            List<AutoCloseable> acquiredBuffers = new ArrayList<>();
            int firstRow = 0;
            while (reader.hasMore()) {
                int pageRows = reader.rowsRemainingInCurrentPage();
                readEagerWindow(reader, pageRows, firstRow, survives, acquiredBuffers, drained);
                firstRow += pageRows;
            }
            return drained.toResult(reader);
        });
    }

    /** Reads the current page's remaining surviving rows and keeps the ones {@code survives} accepts. */
    private static void readEagerWindow(
            BatchColumnReader reader,
            int pageRows,
            int firstRow,
            IntPredicate survives,
            List<AutoCloseable> acquiredBuffers,
            DrainedRows drained) {
        Levels pageDefLevels = reader.currentPageDefLevels();
        int windowFirstValue = reader.valuesConsumedInCurrentPage();
        int[] defWindow = (pageDefLevels == null) ? null : pageDefLevels.toArray(windowFirstValue, pageRows);
        LongVector vector = (LongVector) reader.readBatch(pageRows, acquiredBuffers);
        for (int row = 0; row < pageRows; row++) {
            if (survives.test(firstRow + row)) {
                drained.add(vector, row, defWindow);
            }
        }
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

    /** The values, nulls and definition levels of the kept rows, in read order. */
    private static final class DrainedRows {

        private final List<Long> values = new ArrayList<>();
        private final List<Boolean> nulls = new ArrayList<>();
        private final List<Integer> defLevels = new ArrayList<>();

        void addWindow(BatchColumnReader.MaskedWindow window) {
            LongVector vector = (LongVector) window.vector();
            Levels def = window.defLevels();
            int[] defWindow = (def == null) ? null : def.toArray(0, def.size());
            for (int i = 0; i < vector.size(); i++) {
                add(vector, i, defWindow);
            }
        }

        void add(LongVector vector, int index, int[] defWindow) {
            boolean isNull = vector.isNull(index);
            nulls.add(isNull);
            values.add(isNull ? null : vector.getLong(index));
            if (defWindow != null) {
                defLevels.add(defWindow[index]);
            }
        }

        DrainResult toResult(BatchColumnReader reader) {
            return new DrainResult(
                    values,
                    nulls,
                    defLevels,
                    reader.decodedValueCount(),
                    reader.decodedDataPageCount(),
                    reader.skippedDataPageCount());
        }
    }

    private record DrainResult(
            List<Long> values,
            List<Boolean> nulls,
            List<Integer> defLevels,
            long decodedValueCount,
            int decodedDataPageCount,
            int skippedDataPageCount) {}

    // --- reader construction ---

    /** What a test does with one open column reader over the file's single row group. */
    @FunctionalInterface
    private interface ColumnDrain {
        DrainResult over(BatchColumnReader reader);
    }

    /** What a test does with one fetched column chunk of the file's single row group and its offset index. */
    @FunctionalInterface
    private interface ChunkAction<T> {
        T over(FetchedColumnChunk chunk, SchemaNode.Primitive leaf, OffsetIndex offsetIndex);
    }

    /**
     * Opens {@code column} of the file's first row group under {@code mask} in {@code valueDecode} mode and hands the
     * reader to {@code drain}. The chunk is fetched under the same mask, the narrowed fetch a driver plans when the
     * column index pruned the row group.
     */
    private DrainResult overColumn(
            Path file,
            ParquetSchema schema,
            ColumnPath column,
            RowRanges mask,
            ValueDecode valueDecode,
            ColumnDrain drain)
            throws Exception {
        return overChunk(file, schema, column, mask, (chunk, leaf, offsetIndex) -> {
            BatchColumnReader reader =
                    new BatchColumnReader(TestDecodeBuffers.ample(), chunk, leaf, mask, offsetIndex, valueDecode);
            try {
                return drain.over(reader);
            } finally {
                reader.close();
            }
        });
    }

    /** Fetches {@code column} of the file's first row group under {@code mask} and hands it to {@code action}. */
    private <T> T overChunk(Path file, ParquetSchema schema, ColumnPath column, RowRanges mask, ChunkAction<T> action)
            throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            RowGroup rowGroup = footer.rowGroups().get(0);
            OffsetIndex offsetIndex = loadOffsetIndex(source, rowGroup);
            RowGroupChunks chunks = RowGroupChunks.of(rowGroup, schema, indexLoader(source));
            RowGroupFetcher fetcher = TestFetchers.over(source, schema, schema, SegmentPool.getDefault());
            RowGroupSurvivor survivor = new RowGroupSurvivor(chunks, Optional.of(mask), true);
            try (RowGroupFetch fetch =
                    fetcher.fetch(survivor, fetcher.planFor(survivor, Optional.empty()), BudgetReservation.NONE)) {
                SchemaNode.Primitive leaf =
                        (SchemaNode.Primitive) schema.find(column).orElseThrow();
                return action.over(fetch.columns().get(0), leaf, offsetIndex);
            }
        }
    }

    private static OffsetIndex loadOffsetIndex(ByteRangeSource source, RowGroup rowGroup) {
        ColumnChunk chunk = rowGroup.columns().get(0);
        return ParquetFormat.readOffsetIndex(
                source,
                chunk.offsetIndexOffset().getAsLong(),
                chunk.offsetIndexLength().getAsInt());
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
        Path file = tempDir.resolve("row-mask-required.parquet");
        ParquetSchema schema = flatSchema(requiredInt64("v"));
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int v = 0; v < ROW_COUNT; v++) {
            rows.add(requiredRow(v));
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, pageEvery())) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }
        return file;
    }

    private Path writeNullableLongs() throws Exception {
        Path file = tempDir.resolve("row-mask-nullable.parquet");
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

    /** A list column whose rows differ in length: the repeated shape a row mask has no row-to-slot mapping for. */
    private Path writeListsOfLongs() throws Exception {
        Path file = tempDir.resolve("row-mask-lists.parquet");
        ParquetSchema schema = listOfInt64Schema();
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        for (int row = 0; row < ROW_COUNT; row++) {
            builder.beginList("items");
            for (int element = 0; element <= row % 3; element++) {
                builder.addLong(row * 10L + element);
            }
            builder.endList();
            builder.endRow();
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, pageEvery())) {
            writer.writeBatch(builder.build());
        }
        return file;
    }

    private WriteOptions pageEvery() {
        return WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(ROWS_PER_PAGE)
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
