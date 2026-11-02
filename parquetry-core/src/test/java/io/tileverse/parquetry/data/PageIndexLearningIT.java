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

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.WriteOptions.EncodingPolicy;
import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.Tier;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.filter.LearnedPages;
import io.tileverse.parquetry.internal.footer.LeafIndex;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.CountingByteRangeSource;

/**
 * A filtered read of a file without a page index learns pages from the row groups fetched whole. A later read of the
 * same row groups prunes pages with them. The headers file holds 5,000 rows in row groups of 1,000 rows and pages of
 * 100 values, with Parquet 1 page headers and no page index. Its twin holds the same rows and pages with a page index.
 *
 * <p>Row {@code i} holds id {@code i}, a name of 80 characters, the ordinal of its page within its row group as a
 * value, and a note. A name is {@code name-}, the zero-padded id and a filler. The note holds the id on the even pages
 * of a row group, and the odd pages hold only nulls. The id, name and note columns are plain-encoded and no page is
 * compressed. The pages are then much larger than the page index of the twin, and a read pruned by learned pages reads
 * about as much as the twin.
 */
class PageIndexLearningIT {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath NAME = ColumnPath.of("name");
    private static final ColumnPath VALUE = ColumnPath.of("value");
    private static final ColumnPath NOTE = ColumnPath.of("note");

    private static final int ROWS = 5_000;
    private static final int ROWS_PER_GROUP = 1_000;
    private static final int ROW_GROUPS = ROWS / ROWS_PER_GROUP;
    private static final int PAGE_VALUES = 100;
    private static final String NAME_TAIL = "-" + "x".repeat(69);

    /** Holds for ids 2,310 to 2,320, on one page of the third row group. */
    private static final Predicate ONE_PAGE = Pred.and(col(ID).gtEq(2_310L), col(ID).ltEq(2_320L));

    /** Holds for the rows of the first page of each row group. */
    private static final Predicate FIRST_PAGES = col(VALUE).lt(1.0);

    private static final ReadOptions NO_PAGE_INDEX =
            ReadOptions.builder().useColumnIndexFilter(false).build();

    @TempDir
    static Path tempDir;

    private static Path headersFile;
    private static Path indexedFile;

    @BeforeAll
    static void writeTheFiles() throws IOException {
        headersFile = writeFile("headers.parquet", ParquetVersion.V1_1);
        indexedFile = writeFile("indexed.parquet", ParquetVersion.V2_0);
    }

    @BeforeEach
    void clearTheCaches() {
        ParquetCaches.clearAll();
    }

    @Test
    void secondTouchReadsFewerBytes() {
        try (CountingByteRangeSource source = named(headersFile)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            Touch unlearned = touch(reader, source, ONE_PAGE, NO_PAGE_INDEX);
            Touch first = touch(reader, source, ONE_PAGE, ReadOptions.DEFAULTS);
            Touch second = touch(reader, source, ONE_PAGE, ReadOptions.DEFAULTS);

            assertThat(first.bytes()).isEqualTo(unlearned.bytes());
            assertThat(first.requests()).isEqualTo(unlearned.requests());
            assertThat(second.bytes()).isLessThan(first.bytes() / 2);
            assertThat(first.rows()).isEqualTo(unlearned.rows());
            assertThat(second.rows()).isEqualTo(unlearned.rows()).hasSize(11);
        }
    }

    @Test
    void secondTouchMatchesTheTwin() {
        Touch second = secondTouch(headersFile, ONE_PAGE);
        Touch twin = firstTouch(indexedFile, ONE_PAGE);

        assertThat(second.bytes()).isCloseTo(twin.bytes(), withinPercentage(10));
        assertThat(second.rows()).isEqualTo(twin.rows());
    }

    static Stream<Arguments> selectivePredicates() {
        return Stream.of(
                Arguments.of("a range", Pred.and(col(ID).gtEq(1_250L), col(ID).ltEq(1_420L)), 171),
                Arguments.of("a point", col(ID).eq(3_456L), 1),
                Arguments.of("a name", col(NAME).eq(nameOf(2_315L)), 1),
                Arguments.of("the last page of each row group", col(VALUE).gtEq(9.0), 500),
                Arguments.of("two ranges", Pred.or(col(ID).lt(150L), col(ID).gtEq(4_850L)), 300),
                Arguments.of("the notes", col(NOTE).isNotNull(), 2_500),
                Arguments.of("the missing notes", col(NOTE).isNull(), 2_500));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("selectivePredicates")
    void rowsAgreeOnBothTouches(String label, Predicate predicate, int expectedRows) {
        List<List<Object>> unpruned;
        Touch first;
        Touch second;
        try (CountingByteRangeSource source = named(headersFile)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            unpruned = rows(reader, predicate, NO_PAGE_INDEX);
            first = touch(reader, source, predicate, ReadOptions.DEFAULTS);
            second = touch(reader, source, predicate, ReadOptions.DEFAULTS);
        }
        List<List<Object>> twin = firstTouch(indexedFile, predicate).rows();

        assertThat(unpruned).as("a read without page indexes").hasSize(expectedRows);
        assertThat(first.rows()).as("the first touch").isEqualTo(unpruned);
        assertThat(second.rows()).as("the second touch").isEqualTo(unpruned);
        assertThat(twin).as("the twin").isEqualTo(unpruned);
        assertThat(second.bytes()).as("the bytes of the second touch").isLessThan(first.bytes());
    }

    static Stream<Arguments> predicatesPassingEachRow() {
        return Stream.of(
                Arguments.of("no predicate", Predicate.ALWAYS_TRUE), Arguments.of("each id", col(ID).gtEq(0L)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("predicatesPassingEachRow")
    void fullScanLeavesTheCacheEmpty(String label, Predicate predicate) {
        Touch read = firstTouch(headersFile, predicate);

        assertThat(read.rows()).hasSize(ROWS);
        assertThat(FooterMetadataCache.learnedEntryCount()).isZero();
    }

    @Test
    void countPrunesOnTheSecondTouch() {
        Predicate lastPage = col(ID).gt(4_950L);
        try (CountingByteRangeSource source = named(headersFile)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            Counted first = count(reader, source, lastPage);
            Counted second = count(reader, source, lastPage);

            assertThat(first.rows()).isEqualTo(49L);
            assertThat(second.rows()).isEqualTo(first.rows());
            assertThat(second.bytes()).isLessThan(first.bytes());
        }
    }

    @Test
    void clearedCacheReadsWholeAgain() {
        try (CountingByteRangeSource source = named(headersFile)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            Touch first = touch(reader, source, ONE_PAGE, ReadOptions.DEFAULTS);
            ParquetCaches.clearAll();
            Touch afterClear = touch(reader, source, ONE_PAGE, ReadOptions.DEFAULTS);

            assertThat(afterClear.bytes()).isEqualTo(first.bytes());
            assertThat(afterClear.rows()).isEqualTo(first.rows());
        }
    }

    // Without prefetch and decode-ahead, the read fetches the first row group alone before it is closed.
    @Test
    void earlyCloseLearnsFetchedGroups() {
        ParquetRuntime serial =
                ParquetRuntime.defaultRuntime().withPrefetchDepth(0).withMaxDecodeAhead(0);
        try (CountingByteRangeSource source = named(headersFile)) {
            ParquetFileReader reader = ParquetFileReader.open(source, serial, Optional.empty());

            readFirstBatch(reader, FIRST_PAGES);

            assertThat(learnedColumns(reader, source, 0)).containsExactlyInAnyOrder(ID, NAME, VALUE, NOTE);
            for (int rowGroup = 1; rowGroup < ROW_GROUPS; rowGroup++) {
                assertThat(learnedColumns(reader, source, rowGroup))
                        .as("row group %d", rowGroup)
                        .isEmpty();
            }
            List<List<Object>> unpruned = rows(reader, FIRST_PAGES, NO_PAGE_INDEX);
            assertThat(rows(reader, FIRST_PAGES, ReadOptions.DEFAULTS)).isEqualTo(unpruned);
        }
    }

    @Test
    void toggleOffLearnsNothing() {
        try (CountingByteRangeSource source = named(headersFile)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            Touch firstOff = touch(reader, source, ONE_PAGE, NO_PAGE_INDEX);
            Touch secondOff = touch(reader, source, ONE_PAGE, NO_PAGE_INDEX);
            long learnedWhileOff = FooterMetadataCache.learnedEntryCount();
            Touch firstOn = touch(reader, source, ONE_PAGE, ReadOptions.DEFAULTS);
            Touch secondOn = touch(reader, source, ONE_PAGE, ReadOptions.DEFAULTS);

            assertThat(secondOff.bytes()).isEqualTo(firstOff.bytes());
            assertThat(learnedWhileOff).isZero();
            assertThat(secondOn.bytes()).isLessThan(firstOn.bytes());
        }
    }

    @Test
    void toggleOffUsesNoLearnedPages() {
        try (CountingByteRangeSource source = named(headersFile)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            Touch first = touch(reader, source, ONE_PAGE, ReadOptions.DEFAULTS);
            Touch second = touch(reader, source, ONE_PAGE, ReadOptions.DEFAULTS);
            Touch off = touch(reader, source, ONE_PAGE, NO_PAGE_INDEX);

            assertThat(second.bytes()).isLessThan(first.bytes());
            assertThat(off.bytes()).isEqualTo(first.bytes());
            assertThat(off.rows()).isEqualTo(first.rows());
        }
    }

    // The twin is fetched whole, as a learner would need. Each of its chunks declares an offset index.
    @Test
    void declaredIndexLearnsNothing() {
        ReadOptions wholeChunks =
                ReadOptions.builder().usePageNarrowedFetch(false).build();
        try (CountingByteRangeSource source = named(indexedFile)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            touch(reader, source, ONE_PAGE, wholeChunks);
        }

        assertThat(FooterMetadataCache.learnedEntryCount()).isZero();
    }

    @Test
    void explainShowsLearnedDecisions() {
        try (CountingByteRangeSource source = named(headersFile)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            PruningDecision before = columnIndexDecision(reader, ONE_PAGE, 2);
            touch(reader, source, ONE_PAGE, ReadOptions.DEFAULTS);
            PruningDecision after = columnIndexDecision(reader, ONE_PAGE, 2);

            assertThat(before).isInstanceOf(PruningDecision.NotApplied.class);
            assertThat(after).isInstanceOf(PruningDecision.NarrowedTo.class);
        }
    }

    @Test
    void unnamedSourceLearnsNothing() {
        try (CountingByteRangeSource source = CountingByteRangeSource.unnamed(ByteRangeSource.ofFile(headersFile))) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            Touch first = touch(reader, source, ONE_PAGE, ReadOptions.DEFAULTS);
            Touch second = touch(reader, source, ONE_PAGE, ReadOptions.DEFAULTS);

            assertThat(second.bytes()).isEqualTo(first.bytes());
            assertThat(FooterMetadataCache.learnedEntryCount()).isZero();
        }
    }

    // --- reads ---

    /** What one read returned, and what it read from the file. */
    private record Touch(long bytes, long requests, List<List<Object>> rows) {}

    /** What one count returned, and what it read from the file. */
    private record Counted(long rows, long bytes) {}

    /** The first read of {@code predicate} over {@code file}, opened anew. */
    private static Touch firstTouch(Path file, Predicate predicate) {
        try (CountingByteRangeSource source = named(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            return touch(reader, source, predicate, ReadOptions.DEFAULTS);
        }
    }

    /** The second read of {@code predicate} over {@code file}, after a first read of the same predicate. */
    private static Touch secondTouch(Path file, Predicate predicate) {
        try (CountingByteRangeSource source = named(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            touch(reader, source, predicate, ReadOptions.DEFAULTS);
            return touch(reader, source, predicate, ReadOptions.DEFAULTS);
        }
    }

    private static Touch touch(
            ParquetFileReader reader, CountingByteRangeSource source, Predicate predicate, ReadOptions options) {
        source.reset();
        List<List<Object>> rows = rows(reader, predicate, options);
        return new Touch(source.bytes(), source.requests(), rows);
    }

    private static Counted count(ParquetFileReader reader, CountingByteRangeSource source, Predicate predicate) {
        source.reset();
        long rows = reader.count(predicate, ReadOptions.DEFAULTS);
        return new Counted(rows, source.bytes());
    }

    private static List<List<Object>> rows(ParquetFileReader reader, Predicate predicate, ReadOptions options) {
        try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, options)) {
            return rows.map(PageIndexLearningIT::valuesOf).toList();
        }
    }

    private static List<Object> valuesOf(ParquetRecord row) {
        Long note = row.isNull(NOTE) ? null : row.getLong(NOTE);
        return Arrays.asList(row.getLong(ID), row.getString(NAME), row.getDouble(VALUE), note);
    }

    /** Reads the first batch of a read of {@code predicate}, and closes the read before its end. */
    private static void readFirstBatch(ParquetFileReader reader, Predicate predicate) {
        ReadOptions smallBatches = ReadOptions.builder().batchSize(10).build();
        try (Stream<ParquetRecordBatch> batches = reader.readBatches(predicate, Projection.ALL, smallBatches)) {
            Iterator<ParquetRecordBatch> iterator = batches.iterator();
            try (ParquetRecordBatch first = iterator.next()) {
                assertThat(first.rowCount()).isPositive();
            }
        }
    }

    /** The columns of row group {@code rowGroup} with pages learned in the cache. */
    private static List<ColumnPath> learnedColumns(ParquetFileReader reader, ByteRangeSource source, int rowGroup) {
        String sourceId = source.sourceIdentifier().orElseThrow();
        LeafIndex leaves = reader.footer().leafIndex();
        List<ColumnPath> learned = new ArrayList<>();
        for (ColumnPath column : reader.schema().leafColumns()) {
            int ordinal = leaves.ordinalOf(column);
            Optional<LearnedPages> pages = FooterMetadataCache.learnedPages(sourceId, source.size(), rowGroup, ordinal);
            if (pages.isPresent()) {
                learned.add(column);
            }
        }
        return learned;
    }

    /** The decision of the COLUMN_INDEX tier on row group {@code rowGroup}, in an explain of {@code predicate}. */
    private static PruningDecision columnIndexDecision(ParquetFileReader reader, Predicate predicate, int rowGroup) {
        ExplainPlan plan = reader.explain(predicate, Projection.ALL, ReadOptions.DEFAULTS);
        List<PruningDecision> decisions = plan.rowGroups().get(rowGroup).tiers();
        List<PruningDecision> columnIndex = decisions.stream()
                .filter(decision -> decision.tier() == Tier.COLUMN_INDEX)
                .toList();
        assertThat(columnIndex)
                .as("COLUMN_INDEX decisions on row group %d", rowGroup)
                .hasSize(1);
        return columnIndex.getFirst();
    }

    private static CountingByteRangeSource named(Path file) {
        return CountingByteRangeSource.named(ByteRangeSource.ofFile(file));
    }

    // --- the files ---

    private static Path writeFile(String fileName, ParquetVersion version) throws IOException {
        Path file = tempDir.resolve(fileName);
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .parquetVersion(version)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .pageValueLimit(PAGE_VALUES)
                .defaultCompression(Compression.uncompressed())
                .encodingPolicy(ID.dot(), EncodingPolicy.FORCE_PLAIN)
                .encodingPolicy(NAME.dot(), EncodingPolicy.FORCE_PLAIN)
                .encodingPolicy(NOTE.dot(), EncodingPolicy.FORCE_PLAIN)
                .build();
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema(), options)) {
            // The writer seals a row group between two batches.
            ParquetRecordBatchBuilder appender = writer.appender(ROWS_PER_GROUP);
            for (long id = 0; id < ROWS; id++) {
                appender.setLong(ID, id);
                appender.setString(NAME, nameOf(id));
                appender.setDouble(VALUE, pageOf(id));
                setNote(appender, id);
                appender.endRow();
            }
        }
        return file;
    }

    /** Sets the note of row {@code id}. It is the id on an even page of the row group, and null on an odd page. */
    private static void setNote(ParquetRecordBatchBuilder appender, long id) {
        if (pageOf(id) % 2 == 0) {
            appender.setLong(NOTE, id);
        } else {
            appender.setNull(NOTE);
        }
    }

    private static String nameOf(long id) {
        return String.format("name-%05d", id) + NAME_TAIL;
    }

    /** The ordinal of the page holding row {@code id}, within its row group. */
    private static long pageOf(long id) {
        return (id % ROWS_PER_GROUP) / PAGE_VALUES;
    }

    private static ParquetSchema schema() {
        SchemaNode.Primitive id = WriteFixtures.requiredLeaf("id", PrimitiveKind.INT64);
        SchemaNode.Primitive name = WriteFixtures.requiredLeaf(
                "name", PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), new LogicalType.StringType());
        SchemaNode.Primitive value = WriteFixtures.requiredLeaf("value", PrimitiveKind.DOUBLE);
        SchemaNode.Primitive note = WriteFixtures.optionalLeaf("note", PrimitiveKind.INT64);
        return WriteFixtures.schemaOf(id, name, value, note);
    }
}
