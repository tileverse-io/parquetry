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

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.filter.LearnedPages;
import io.tileverse.parquetry.internal.filter.PageLearning;
import io.tileverse.parquetry.internal.footer.ChunkMeta;
import io.tileverse.parquetry.internal.read.PageIndexLearner.WholeChunk;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaBuilder;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Runs the learner over the chunks of a file without a page index, read whole from the file as a fetch reads them. The
 * file holds two row groups of 1,000 rows in pages of 100 values. Row {@code i} holds id {@code i}, a name and a list
 * of one tag.
 *
 * <p>Two checks read another file, of one row group of 300 rows. Its stepped column has pages of 100 values. It holds
 * the row number in its first page, and the row number plus 1,000 after it. Its row column holds the row number, in
 * pages of 150 values.
 */
class PageIndexLearnerTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath NAME = ColumnPath.of("name");
    private static final ColumnPath TAGS = ColumnPath.of("tags");
    private static final ColumnPath TAG = ColumnPath.of("tags", "list", "element");
    private static final ColumnPath STEPPED = ColumnPath.of("stepped");
    private static final ColumnPath ROW = ColumnPath.of("row");

    private static final int ROW_GROUPS = 2;
    private static final int ROWS_PER_GROUP = 1_000;
    private static final int PAGE_VALUES = 100;
    private static final int PAGES_PER_GROUP = ROWS_PER_GROUP / PAGE_VALUES;
    private static final int TWO_PACED_ROWS = 300;
    private static final int ROW_PAGE_VALUES = 150;

    /** Holds for the ids of the last page of the second row group alone. */
    private static final Predicate LAST_PAGE = col(ID).gt(1_950L);

    @TempDir
    Path tempDir;

    private Path file;
    private List<RowGroupChunks> rowGroups;
    private final InMemoryLearnedPageIndexes sink = new InMemoryLearnedPageIndexes();

    @BeforeEach
    void writeTheFile() throws IOException {
        open(writeIdsNamesAndTags());
    }

    @Test
    void aDroppingPredicateStoresEntries() {
        learnerFor(LAST_PAGE).learn(survivor(1), wholeChunks(1, NAME, ID));

        assertThat(sink.columnsOf(1)).containsExactlyInAnyOrder(ordinalOf(ID), ordinalOf(NAME));
        assertThat(pageCountOf(1, ID)).contains(PAGES_PER_GROUP);
        assertThat(pageCountOf(1, NAME)).contains(PAGES_PER_GROUP);
        assertThat(firstIdOf(1)).as("the first id kept for the id column").contains(1_000L);
    }

    @Test
    void allRowsPassingLearnsNothing() {
        Predicate eachId = col(ID).gtEq(0L);
        BitSet learnable = learnableRowGroups(eachId);

        learner(eachId, learnable).learn(survivor(0), wholeChunks(0, ID, NAME));

        assertThat(learnable.cardinality()).isZero();
        assertThat(sink.isEmpty()).isTrue();
    }

    // Ids 0 to 50 fail the predicate, and each page of 100 ids still holds a passing id.
    @Test
    void noDroppedPageKeepsNothing() {
        Predicate idAbove = col(ID).gt(50L);
        BitSet learnable = learnableRowGroups(idAbove);

        learner(idAbove, learnable).learn(survivor(0), wholeChunks(0, ID, NAME));

        assertThat(learnable.get(0))
                .as("the first row group allowed to the learner")
                .isTrue();
        assertThat(sink.isEmpty()).isTrue();
    }

    // A read scanning a repeated leaf reads the row group whole, because a row mask needs flat leaves.
    @Test
    void aRepeatedLeafLearnsNothing() {
        learnerFor(LAST_PAGE).learn(survivor(1), wholeChunks(1, ID, NAME, TAG));

        assertThat(sink.isEmpty()).isTrue();
    }

    // The learner keeps a copy of the set handed to it.
    @Test
    void learnsOnlyTheAllowedGroups() {
        BitSet secondOnly = new BitSet();
        secondOnly.set(1);
        PageIndexLearner learner = learner(LAST_PAGE, secondOnly);
        secondOnly.set(0);

        assertThat(learner.learns(0)).isFalse();
        assertThat(learner.learns(1)).isTrue();
        assertThat(PageIndexLearner.NONE.learns(1)).isFalse();
    }

    @Test
    void anUnlearnableGroupIsLeftAlone() {
        learner(LAST_PAGE, new BitSet()).learn(survivor(1), wholeChunks(1, ID, NAME));

        assertThat(sink.isEmpty()).isTrue();
    }

    // The id chunk alone would learn, and an entry missing for the names would leave the row group without a mask.
    @Test
    void aBadChunkDropsTheRowGroup() {
        List<WholeChunk> chunks = wholeChunks(1, ID, NAME);
        List<WholeChunk> namesCut = List.of(chunks.get(0), withoutLastByte(chunks.get(1)));

        learnerFor(LAST_PAGE).learn(survivor(1), namesCut);

        assertThat(sink.isEmpty()).isTrue();
    }

    // The sink fails on the first entry handed to it. The pass ends there, and the fetch of the row group goes on.
    @Test
    void aFailingSinkEndsThePass() {
        FailingSink failing = new FailingSink();
        PageIndexLearner learner =
                new HeaderPageIndexLearner(prunablePart(LAST_PAGE), learnableRowGroups(LAST_PAGE), failing);
        List<WholeChunk> chunks = wholeChunks(1, NAME, ID);

        assertThatCode(() -> learner.learn(survivor(1), chunks)).doesNotThrowAnyException();
        assertThat(failing.entries()).as("the entries handed to the sink").isOne();
    }

    // Each column drops a page, and some rows of each dropped page survive through the other column.
    @Test
    void anUnevenNarrowingKeepsNothing() throws IOException {
        open(writeTwoPacedColumns());
        Predicate eitherEnd = new Predicate.Or(List.of(col(STEPPED).lt(100L), col(ROW).gtEq(150L)));

        learnerFor(eitherEnd).learn(survivor(0), wholeChunks(0, STEPPED, ROW));

        assertThat(sink.isEmpty()).isTrue();
    }

    // 500 lies within the bounds of the row group, and above the first page and below the others.
    @Test
    void anEliminatedGroupStoresEntries() throws IOException {
        open(writeTwoPacedColumns());

        learnerFor(col(STEPPED).eq(500L)).learn(survivor(0), wholeChunks(0, STEPPED, ROW));

        assertThat(sink.columnsOf(0)).containsExactlyInAnyOrder(ordinalOf(STEPPED), ordinalOf(ROW));
    }

    /** Reads the footer of {@code written} into the chunk views of its row groups. */
    private void open(Path written) {
        file = written;
        FileMetaData footer = readFooter(written);
        ParquetSchema schema = SchemaBuilder.build(footer.schema());
        rowGroups = TestRowGroupChunks.allOf(footer, schema, TestRowGroupChunks.noIndexSections());
    }

    /** The learner of {@code predicate}, allowed the row groups whose statistics leave a row failing it. */
    private PageIndexLearner learnerFor(Predicate predicate) {
        return learner(predicate, learnableRowGroups(predicate));
    }

    private PageIndexLearner learner(Predicate predicate, BitSet learnableRowGroups) {
        return new HeaderPageIndexLearner(prunablePart(predicate), learnableRowGroups, sink);
    }

    /** The row groups whose statistics do not show each row passing {@code predicate}, as a read plans them. */
    private BitSet learnableRowGroups(Predicate predicate) {
        Predicate prunablePart = prunablePart(predicate);
        BitSet learnable = new BitSet(rowGroups.size());
        for (RowGroupChunks rowGroup : rowGroups) {
            boolean allRowsPass = PageLearning.allRowsPass(prunablePart, rowGroup::summary, rowGroup.numRows());
            if (!allRowsPass) {
                learnable.set(rowGroup.rowGroupIndex());
            }
        }
        return learnable;
    }

    private static Predicate prunablePart(Predicate predicate) {
        return PageLearning.prunablePart(predicate).orElseThrow();
    }

    private RowGroupSurvivor survivor(int rowGroup) {
        return RowGroupSurvivor.full(rowGroups.get(rowGroup));
    }

    private Optional<Integer> pageCountOf(int rowGroup, ColumnPath column) {
        Optional<LearnedPages> entry = sink.get(rowGroup, ordinalOf(column));
        return entry.map(LearnedPages::pageCount);
    }

    /** The minimum of the first page, in the entry kept for the id column of row group {@code rowGroup}. */
    private Optional<Long> firstIdOf(int rowGroup) {
        ValueLayout.OfLong littleEndian = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
        Optional<LearnedPages> entry = sink.get(rowGroup, ordinalOf(ID));
        Optional<MemorySegment> minimum = entry.flatMap(pages -> pages.min(0));
        return minimum.map(bound -> bound.get(littleEndian, 0));
    }

    /** The leaf ordinal of {@code column}, as the chunk views give it to the learner. */
    private int ordinalOf(ColumnPath column) {
        return rowGroups.getFirst().ordinalOf(column);
    }

    /** The chunks of {@code columns} in row group {@code rowGroup}, each read whole from the file. */
    private List<WholeChunk> wholeChunks(int rowGroup, ColumnPath... columns) {
        RowGroupChunks view = rowGroups.get(rowGroup);
        List<WholeChunk> chunks = new ArrayList<>(columns.length);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            for (ColumnPath column : columns) {
                ChunkMeta meta = view.chunk(column).orElseThrow();
                chunks.add(new WholeChunk(column, meta, readWhole(source, meta)));
            }
        }
        return chunks;
    }

    private static MemorySegment readWhole(ByteRangeSource source, ChunkMeta meta) {
        MemorySegment bytes = MemorySegment.ofArray(new byte[meta.totalCompressedSize()]);
        source.readFully(meta.chunkStart(), bytes);
        return bytes;
    }

    private static WholeChunk withoutLastByte(WholeChunk chunk) {
        MemorySegment bytes = chunk.bytes();
        MemorySegment cut = bytes.asSlice(0, bytes.byteSize() - 1);
        return new WholeChunk(chunk.path(), chunk.meta(), cut);
    }

    private static FileMetaData readFooter(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return ParquetFormat.readFooter(source);
        }
    }

    /** Writes the ids, the names and the tags. */
    private Path writeIdsNamesAndTags() throws IOException {
        WriteOptions options =
                v1Pages().rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP)).build();
        Path target = tempDir.resolve("ids-names-tags.parquet");
        try (OutputStream out = Files.newOutputStream(target);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema(), options)) {
            // The writer seals a row group between two batches.
            ParquetRecordBatchBuilder appender = writer.appender(ROWS_PER_GROUP);
            for (long id = 0; id < (long) ROW_GROUPS * ROWS_PER_GROUP; id++) {
                appendRow(appender, id);
            }
        }
        return target;
    }

    /** Writes the stepped column and the row column in one row group. */
    private Path writeTwoPacedColumns() throws IOException {
        ParquetSchema schema = WriteFixtures.schemaOf(
                WriteFixtures.requiredLeaf("stepped", PrimitiveKind.INT64),
                WriteFixtures.requiredLeaf("row", PrimitiveKind.INT64));
        WriteOptions options = v1Pages().pageValueLimit("row", ROW_PAGE_VALUES).build();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(TWO_PACED_ROWS);
        for (long row = 0; row < TWO_PACED_ROWS; row++) {
            long stepped = row < PAGE_VALUES ? row : row + 1_000;
            rows.add(Map.of(STEPPED, stepped, ROW, row));
        }
        return WriteFixtures.writeRows(tempDir.resolve("two-paced.parquet"), schema, options, rows);
    }

    /** Pages of {@link #PAGE_VALUES} values in the V1 form. The writer declares no page index for V1 pages. */
    private WriteOptions.Builder v1Pages() {
        return WriteOptions.builder()
                .tempDir(tempDir)
                .parquetVersion(ParquetVersion.V1_1)
                .pageValueLimit(PAGE_VALUES);
    }

    private static void appendRow(ParquetRecordBatchBuilder appender, long id) {
        appender.setLong(ID, id);
        appender.setString(NAME, "name-" + id);
        appender.beginList(TAGS);
        appender.addLong(id % 7);
        appender.endList();
        appender.endRow();
    }

    private static ParquetSchema schema() {
        SchemaNode.Primitive id = WriteFixtures.requiredLeaf("id", PrimitiveKind.INT64);
        SchemaNode.Primitive name = WriteFixtures.requiredLeaf(
                "name", PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), new LogicalType.StringType());
        SchemaNode.Primitive tag = WriteFixtures.optionalLeaf("element", PrimitiveKind.INT64);
        SchemaNode.Group list = new SchemaNode.Group("list", Repetition.REPEATED, List.of(tag), Optional.empty(), -1);
        Optional<LogicalType> listType = Optional.of(new LogicalType.ListType());
        SchemaNode.Group tags = new SchemaNode.Group("tags", Repetition.OPTIONAL, List.of(list), listType, -1);
        return WriteFixtures.schemaOf(id, name, tags);
    }

    /** Page indexes failing on each entry handed to them. They count the entries and keep none. */
    private static final class FailingSink implements LearnedPageIndexes {

        private int entries;

        @Override
        public Optional<LearnedPages> get(int rowGroup, int column) {
            return Optional.empty();
        }

        @Override
        public void putIfAbsent(int rowGroup, int column, LearnedPages pages) {
            entries++;
            throw new IllegalStateException("The sink fails on entry " + entries);
        }

        int entries() {
            return entries;
        }
    }
}
