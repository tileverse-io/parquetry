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
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.EncodingPolicy;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.internal.read.page.DataPageRun;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Plan-level behavior of {@link RowGroupFetcher#planFor(RowGroupSurvivor, Optional)} against a real multi-page file: a
 * present row mask narrows each column to the byte runs of its surviving pages plus the dictionary prefix, an absent
 * mask plans whole chunks, and every shape the mask cannot be trusted for falls back to the whole chunk.
 *
 * <p>The fixture holds one row group of two long columns split into twenty pages each. Column {@code a} is written
 * {@code PLAIN} and has no dictionary page; column {@code b} is written dictionary-encoded, which puts a dictionary
 * page ahead of its first data page. The fetcher coalesces with a zero gap, hence only abutting byte ranges merge and
 * each planned run stays visible as its own range boundary.
 */
class PageNarrowedPlanTest {

    private static final ColumnPath A = ColumnPath.of("a");
    private static final ColumnPath B = ColumnPath.of("b");
    private static final int ROW_COUNT = 2_000;
    private static final int PAGE_VALUE_LIMIT = 100;
    private static final int PAGE_COUNT = ROW_COUNT / PAGE_VALUE_LIMIT;
    private static final int LAST_PAGE = PAGE_COUNT - 1;
    private static final int DEAD_MIDDLE_PAGE = PAGE_COUNT / 2;
    private static final int NO_COALESCE_GAP = 0;
    private static final int AMPLE_COALESCED_SPAN = 8 << 20;

    private static final ParquetSchema SCHEMA = twoLongColumnsSchema();

    @TempDir
    static Path tempDir;

    private static Path file;

    @BeforeAll
    static void writeFixtureFile() throws IOException {
        file = writeTwentyPagesPerColumn();
    }

    @Test
    void maskAbsentPlansWholeChunks() throws IOException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            Fixture fixture = Fixture.open(source);

            FetchPlan plan = fixture.plan(Optional.empty());

            assertWholeChunk(plan, fixture.meta(A), A);
            assertWholeChunk(plan, fixture.meta(B), B);
            assertThat(plan.totalBytes())
                    .as("a whole-chunk plan reads every byte of both chunks and nothing else")
                    .isEqualTo(fixture.meta(A).totalCompressedSize()
                            + fixture.meta(B).totalCompressedSize());
        }
    }

    @Test
    void maskNarrowsToSurvivingRunsPlusDictionaryPrefixWhenPresent() throws IOException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            Fixture fixture = Fixture.open(source);
            assertThat(fixture.offsetIndex(A).pageLocations()).hasSize(PAGE_COUNT);
            assertThat(fixture.offsetIndex(B).pageLocations()).hasSize(PAGE_COUNT);

            FetchPlan whole = fixture.plan(Optional.empty());
            FetchPlan narrowed = fixture.plan(Optional.of(fixture.maskOver(firstAndLastRows())));

            assertThat(narrowed.totalBytes())
                    .as("skipping eighteen of twenty pages per column must read fewer bytes")
                    .isLessThan(whole.totalBytes());
            for (ColumnPath path : List.of(A, B)) {
                List<PageLocation> pages = fixture.offsetIndex(path).pageLocations();
                assertPageFetched(narrowed, pages.get(0), path);
                assertPageFetched(narrowed, pages.get(LAST_PAGE), path);
                assertPageNotFetched(narrowed, pages.get(DEAD_MIDDLE_PAGE), path);
                assertThat(runOrdinals(narrowed, path))
                        .as("one run per surviving page, seeded with that page's offset-index ordinal, %s", path.dot())
                        .containsExactly(0, LAST_PAGE);
            }
            assertThat(narrowed.slices().get(A).dictionaryPrefix())
                    .as("a PLAIN column's chunk starts at its first data page, leaving no prefix to fetch")
                    .isEmpty();
            assertDictionaryPrefix(narrowed, fixture, B);
        }
    }

    @Test
    void allPagesSurvivingPlansExactlyTheWholeChunk() throws IOException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            Fixture fixture = Fixture.open(source);

            FetchPlan narrowed = fixture.plan(Optional.of(fixture.maskOver(RowRanges.all(ROW_COUNT))));

            assertThat(narrowed)
                    .as("with every page surviving the whole chunk IS the narrowed plan, trailing bytes included")
                    .isEqualTo(fixture.plan(Optional.empty()));
        }
    }

    @Test
    void columnMissingFromTheMaskFallsBackToItsWholeChunk() throws IOException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            Fixture fixture = Fixture.open(source);
            RowMask maskWithoutB = new RowMask(firstAndLastRows(), Map.of(A, fixture.offsetIndex(A)));

            FetchPlan plan = fixture.plan(Optional.of(maskWithoutB));

            assertThat(runOrdinals(plan, A)).containsExactly(0, LAST_PAGE);
            assertWholeChunk(plan, fixture.meta(B), B);
        }
    }

    @Test
    void malformedFirstDataPageOffsetFallsBackToWholeChunk() throws IOException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            Fixture fixture = Fixture.open(source);
            OffsetIndex beforeTheChunk = withFirstPageOffset(fixture.offsetIndex(A), chunkStartOf(fixture.meta(A)) - 1);
            RowMask mask = new RowMask(firstAndLastRows(), Map.of(A, beforeTheChunk, B, fixture.offsetIndex(B)));

            FetchPlan plan = fixture.plan(Optional.of(mask));

            assertWholeChunk(plan, fixture.meta(A), A);
            assertThat(runOrdinals(plan, B))
                    .as("one column's unusable offset index does not widen the other")
                    .containsExactly(0, LAST_PAGE);
        }
    }

    @Test
    void lastPageEndingPastTheChunkFallsBackToWholeChunk() throws IOException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            Fixture fixture = Fixture.open(source);
            OffsetIndex spillingPastTheChunk =
                    withLastPageEndingAt(fixture.offsetIndex(A), chunkEndOf(fixture.meta(A)) + 1);
            RowMask mask = new RowMask(firstAndLastRows(), Map.of(A, spillingPastTheChunk, B, fixture.offsetIndex(B)));

            FetchPlan plan = fixture.plan(Optional.of(mask));

            assertWholeChunk(plan, fixture.meta(A), A);
            assertThat(runOrdinals(plan, B))
                    .as("one column's out-of-extent offset index does not widen the other")
                    .containsExactly(0, LAST_PAGE);
        }
    }

    @Test
    void middlePageOutsideTheChunkFallsBackToWholeChunk() throws IOException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            Fixture fixture = Fixture.open(source);
            OffsetIndex middlePagePastTheChunk =
                    withPageOffset(fixture.offsetIndex(A), DEAD_MIDDLE_PAGE, chunkEndOf(fixture.meta(A)));
            RowMask mask =
                    new RowMask(firstAndLastRows(), Map.of(A, middlePagePastTheChunk, B, fixture.offsetIndex(B)));

            FetchPlan plan = fixture.plan(Optional.of(mask));

            assertWholeChunk(plan, fixture.meta(A), A);
            assertThat(runOrdinals(plan, B))
                    .as("an out-of-extent page between in-extent extremes must not be narrowed on either")
                    .containsExactly(0, LAST_PAGE);
        }
    }

    @Test
    void firstDataPageAtTheChunkEndFallsBackToWholeChunk() throws IOException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            Fixture fixture = Fixture.open(source);
            OffsetIndex pastTheChunk = withFirstPageOffset(fixture.offsetIndex(A), chunkEndOf(fixture.meta(A)));
            RowMask mask = new RowMask(firstAndLastRows(), Map.of(A, pastTheChunk, B, fixture.offsetIndex(B)));

            FetchPlan plan = fixture.plan(Optional.of(mask));

            assertWholeChunk(plan, fixture.meta(A), A);
            assertThat(runOrdinals(plan, B)).containsExactly(0, LAST_PAGE);
        }
    }

    @Test
    void unsetDictionaryOffsetWithDataPageOffsetAtTheDictionaryPageStillYieldsThePrefix() throws IOException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            Fixture fixture = Fixture.open(source);
            long dictionaryPageOffset = fixture.meta(B).dictionaryPageOffset().orElseThrow();
            RowGroupSurvivor impalaShaped = fixture.survivorWithDataPageOffsetAtTheDictionaryPage(B);

            FetchPlan plan = fixture.fetcher().planFor(impalaShaped, Optional.of(fixture.maskOver(firstAndLastRows())));

            ColumnSlice prefix = plan.slices().get(B).dictionaryPrefix().orElseThrow();
            assertThat(absoluteOffset(plan, prefix.rangeIndex(), prefix.offsetWithinRange()))
                    .as("the prefix starts at the chunk's first byte even with dictionaryPageOffset unset")
                    .isEqualTo(dictionaryPageOffset);
            assertThat(prefix.length())
                    .isEqualTo(Math.toIntExact(
                            fixture.offsetIndex(B).pageLocations().get(0).offset() - dictionaryPageOffset));
        }
    }

    @Test
    void zeroSurvivingPagesForAColumnFailsLoud() throws IOException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            Fixture fixture = Fixture.open(source);
            RowRanges pastTheRowGroup = new RowRanges(List.of(new Range(ROW_COUNT + 10, ROW_COUNT + 20)));
            Optional<RowMask> contradictoryMask = Optional.of(fixture.maskOver(pastTheRowGroup));

            assertThatIllegalStateException()
                    .isThrownBy(() -> fixture.plan(contradictoryMask))
                    .withMessageContaining(A.dot())
                    .withMessageContaining("No surviving page");
        }
    }

    @Test
    void fetchingWithAColumnMissingFromThePlanFailsLoud() throws IOException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            Fixture fixture = Fixture.open(source);
            FetchPlan withoutB = fixture.planWithoutSlicesFor(B);

            assertThatIllegalStateException()
                    .isThrownBy(() -> fixture.fetcher().fetch(fixture.survivor(), withoutB, BudgetReservation.NONE))
                    .withMessageContaining(B.dot());
        }
    }

    @Test
    void dictionaryPrefixWhoseFirstPageIsADataPageFailsLoud() throws IOException {
        try (Arena arena = Arena.ofConfined();
                ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            Fixture fixture = Fixture.open(source);
            MemorySegment firstDataPage = fixture.readFirstDataPage(A, arena);
            List<DataPageRun> runs = List.of(new DataPageRun(firstDataPage, 0));

            assertThatThrownBy(
                            () -> ColumnChunkSlicer.slice(Optional.of(firstDataPage), runs, fixture.meta(A), A, SCHEMA))
                    .isInstanceOf(MalformedFileException.class)
                    .hasMessageContaining(A.dot())
                    .hasMessageContaining("DATA_PAGE");
        }
    }

    @Test
    void slicingAColumnWithoutAnyFetchedDataPageFailsLoud() throws IOException {
        try (Arena arena = Arena.ofConfined();
                ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            Fixture fixture = Fixture.open(source);
            MemorySegment dictionaryPage = fixture.readDictionaryPrefix(B, arena);

            assertThatIllegalStateException()
                    .isThrownBy(() ->
                            ColumnChunkSlicer.slice(Optional.of(dictionaryPage), List.of(), fixture.meta(B), B, SCHEMA))
                    .withMessageContaining(B.dot());
        }
    }

    // -------------------------------------------------------------------------
    // Assertions
    // -------------------------------------------------------------------------

    private static void assertWholeChunk(FetchPlan plan, ColumnMetaData meta, ColumnPath path) {
        ColumnSlices slices = plan.slices().get(path);
        assertThat(slices.dictionaryPrefix())
                .as("a whole-chunk column reaches its dictionary through the head of its single run, %s", path.dot())
                .isEmpty();
        assertThat(slices.runs()).as("whole-chunk column %s", path.dot()).hasSize(1);
        RunSlice run = slices.runs().get(0);
        assertThat(run.firstPageOrdinal()).isZero();
        assertThat(run.length()).isEqualTo(Math.toIntExact(meta.totalCompressedSize()));
        assertThat(absoluteOffset(plan, run.rangeIndex(), run.offsetWithinRange()))
                .isEqualTo(chunkStartOf(meta));
    }

    private static void assertDictionaryPrefix(FetchPlan plan, Fixture fixture, ColumnPath path) {
        ColumnMetaData meta = fixture.meta(path);
        assertThat(meta.dictionaryPageOffset())
                .as("the fixture's column %s must be dictionary-encoded for the prefix to exist", path.dot())
                .isPresent();
        ColumnSlice prefix = plan.slices().get(path).dictionaryPrefix().orElseThrow();
        long firstDataPageOffset =
                fixture.offsetIndex(path).pageLocations().get(0).offset();
        assertThat(absoluteOffset(plan, prefix.rangeIndex(), prefix.offsetWithinRange()))
                .isEqualTo(chunkStartOf(meta));
        assertThat(prefix.length()).isEqualTo(Math.toIntExact(firstDataPageOffset - chunkStartOf(meta)));
    }

    private static void assertPageFetched(FetchPlan plan, PageLocation page, ColumnPath path) {
        boolean covered = plan.ranges().stream()
                .anyMatch(range ->
                        range.fileOffset() <= page.offset() && pageEnd(page) <= range.fileOffset() + range.length());
        assertThat(covered)
                .as("surviving page at %s of column %s must be fetched whole", page.offset(), path.dot())
                .isTrue();
    }

    private static void assertPageNotFetched(FetchPlan plan, PageLocation page, ColumnPath path) {
        boolean touched = plan.ranges().stream()
                .anyMatch(range ->
                        range.fileOffset() < pageEnd(page) && page.offset() < range.fileOffset() + range.length());
        assertThat(touched)
                .as("skipped page at %s of column %s must not be read at all", page.offset(), path.dot())
                .isFalse();
    }

    private static long pageEnd(PageLocation page) {
        return page.offset() + page.compressedPageSize();
    }

    private static List<Integer> runOrdinals(FetchPlan plan, ColumnPath path) {
        List<Integer> ordinals = new ArrayList<>();
        for (RunSlice run : plan.slices().get(path).runs()) {
            ordinals.add(Integer.valueOf(run.firstPageOrdinal()));
        }
        return ordinals;
    }

    private static long absoluteOffset(FetchPlan plan, int rangeIndex, int offsetWithinRange) {
        return plan.ranges().get(rangeIndex).fileOffset() + offsetWithinRange;
    }

    /**
     * The chunk's first byte as the read path computes it: the dictionary page when the writer recorded a positive
     * offset ahead of the first data page, the first data page otherwise.
     */
    private static long chunkStartOf(ColumnMetaData meta) {
        long dictionaryPageOffset = meta.dictionaryPageOffset().orElse(0L);
        if (dictionaryPageOffset > 0 && dictionaryPageOffset < meta.dataPageOffset()) {
            return dictionaryPageOffset;
        }
        return meta.dataPageOffset();
    }

    /** The byte just past the chunk's last, the exclusive end of the extent the read path trusts an offset index in. */
    private static long chunkEndOf(ColumnMetaData meta) {
        return chunkStartOf(meta) + meta.totalCompressedSize();
    }

    // -------------------------------------------------------------------------
    // Fixture
    // -------------------------------------------------------------------------

    /** The fixture file's single row group, its chunk view, and a fetcher that coalesces nothing but abutting runs. */
    private record Fixture(ByteRangeSource source, RowGroup rowGroup, RowGroupChunks chunks, RowGroupFetcher fetcher) {

        static Fixture open(ByteRangeSource source) {
            RowGroup rowGroup = ParquetFormat.readFooter(source).rowGroups().get(0);
            RowGroupChunks chunks = RowGroupChunks.of(rowGroup, SCHEMA, indexLoader(source));
            RowGroupFetcher fetcher = TestFetchers.over(
                    source,
                    SCHEMA,
                    SCHEMA,
                    SegmentPool.getDefault(),
                    FetchAccumulator.NONE,
                    NO_COALESCE_GAP,
                    AMPLE_COALESCED_SPAN);
            return new Fixture(source, rowGroup, chunks, fetcher);
        }

        ColumnMetaData meta(ColumnPath path) {
            return chunks.meta(path).orElseThrow();
        }

        OffsetIndex offsetIndex(ColumnPath path) {
            return chunks.offsetIndex(path).orElseThrow();
        }

        RowGroupSurvivor survivor() {
            return RowGroupSurvivor.full(chunks);
        }

        FetchPlan plan(Optional<RowMask> mask) {
            return fetcher.planFor(survivor(), mask);
        }

        RowMask maskOver(RowRanges surviving) {
            return new RowMask(surviving, Map.of(A, offsetIndex(A), B, offsetIndex(B)));
        }

        /** A whole-chunk plan whose ranges still cover both columns but whose slice map forgot {@code path}. */
        FetchPlan planWithoutSlicesFor(ColumnPath path) {
            FetchPlan whole = plan(Optional.empty());
            Map<ColumnPath, ColumnSlices> slices = new HashMap<>(whole.slices());
            slices.remove(path);
            return new FetchPlan(whole.ranges(), slices);
        }

        /**
         * A survivor whose {@code path} column has its dictionary offset unset and its data-page offset moved onto the
         * dictionary page, the shape Impala and the Hadoop LZ4 fixtures write.
         */
        RowGroupSurvivor survivorWithDataPageOffsetAtTheDictionaryPage(ColumnPath path) {
            List<ColumnChunk> columns = new ArrayList<>(rowGroup.columns().size());
            for (ColumnChunk chunk : rowGroup.columns()) {
                ColumnMetaData meta = chunk.metaData().orElseThrow();
                boolean isTarget = ColumnPath.of(meta.pathInSchema()).equals(path);
                columns.add(isTarget ? withDataPageOffsetAtTheDictionaryPage(chunk, meta) : chunk);
            }
            RowGroup moved = new RowGroup(
                    columns,
                    rowGroup.totalByteSize(),
                    rowGroup.numRows(),
                    rowGroup.sortingColumns(),
                    rowGroup.fileOffset(),
                    rowGroup.totalCompressedSize(),
                    rowGroup.ordinal());
            return RowGroupSurvivor.full(RowGroupChunks.of(moved, SCHEMA, indexLoader(source)));
        }

        /** Reads the bytes of {@code path}'s first data page (its header plus payload) into {@code arena}. */
        MemorySegment readFirstDataPage(ColumnPath path, Arena arena) {
            PageLocation first = offsetIndex(path).pageLocations().get(0);
            MemorySegment page = arena.allocate(first.compressedPageSize());
            source.readFully(first.offset(), page);
            return page.asReadOnly();
        }

        /** Reads the bytes between {@code path}'s chunk start and its first data page: the dictionary page. */
        MemorySegment readDictionaryPrefix(ColumnPath path, Arena arena) {
            long start = chunkStartOf(meta(path));
            long firstDataPageOffset = offsetIndex(path).pageLocations().get(0).offset();
            MemorySegment prefix = arena.allocate(firstDataPageOffset - start);
            source.readFully(start, prefix);
            return prefix.asReadOnly();
        }
    }

    private static ColumnChunk withDataPageOffsetAtTheDictionaryPage(ColumnChunk chunk, ColumnMetaData meta) {
        long dictionaryPageOffset = meta.dictionaryPageOffset().orElseThrow();
        ColumnMetaData moved = new ColumnMetaData(
                meta.type(),
                meta.encodings(),
                meta.pathInSchema(),
                meta.codec(),
                meta.numValues(),
                meta.totalUncompressedSize(),
                meta.totalCompressedSize(),
                meta.keyValueMetadata(),
                dictionaryPageOffset,
                meta.indexPageOffset(),
                OptionalLong.empty(),
                meta.statistics(),
                meta.encodingStats(),
                meta.bloomFilterOffset(),
                meta.bloomFilterLength(),
                meta.sizeStatistics(),
                meta.geospatialStatistics());
        return new ColumnChunk(
                chunk.filePath(),
                chunk.fileOffset(),
                Optional.of(moved),
                chunk.offsetIndexOffset(),
                chunk.offsetIndexLength(),
                chunk.columnIndexOffset(),
                chunk.columnIndexLength(),
                chunk.cryptoMetadata(),
                chunk.encryptedColumnMetadata());
    }

    /** The same page locations with the first one moved to {@code offset}, which the chunk cannot contain. */
    private static OffsetIndex withFirstPageOffset(OffsetIndex offsetIndex, long offset) {
        return withPageOffset(offsetIndex, 0, offset);
    }

    /** The same page locations with the one at {@code index} moved to {@code offset}, every other left in place. */
    private static OffsetIndex withPageOffset(OffsetIndex offsetIndex, int index, long offset) {
        List<PageLocation> pages = new ArrayList<>(offsetIndex.pageLocations());
        PageLocation page = pages.get(index);
        pages.set(index, new PageLocation(offset, page.compressedPageSize(), page.firstRowIndex()));
        return new OffsetIndex(pages, offsetIndex.unencodedByteArrayDataBytes());
    }

    /** The same page locations with the last one stretched to end at {@code end}, leaving every offset in place. */
    private static OffsetIndex withLastPageEndingAt(OffsetIndex offsetIndex, long end) {
        List<PageLocation> pages = new ArrayList<>(offsetIndex.pageLocations());
        int lastIndex = pages.size() - 1;
        PageLocation last = pages.get(lastIndex);
        int stretched = Math.toIntExact(end - last.offset());
        pages.set(lastIndex, new PageLocation(last.offset(), stretched, last.firstRowIndex()));
        return new OffsetIndex(pages, offsetIndex.unencodedByteArrayDataBytes());
    }

    private static RowRanges firstAndLastRows() {
        return new RowRanges(List.of(new Range(0, 9), new Range(ROW_COUNT - 10, ROW_COUNT - 1)));
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

    // -------------------------------------------------------------------------
    // File writing
    // -------------------------------------------------------------------------

    /**
     * Writes one row group of {@value #ROW_COUNT} rows over two long columns, {@value #PAGE_VALUE_LIMIT} rows per page.
     * Column {@code a} counts up and is pinned to {@code PLAIN}; column {@code b} cycles over four values and is pinned
     * to dictionary encoding, which gives it a dictionary page ahead of its first data page.
     */
    private static Path writeTwentyPagesPerColumn() throws IOException {
        Path target = tempDir.resolve("page-narrowed-plan.parquet");
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(PAGE_VALUE_LIMIT)
                .defaultCompression(Compression.uncompressed())
                .rowGroupSize(RowGroupSize.rows(ROW_COUNT))
                .encodingPolicy(A.dot(), EncodingPolicy.FORCE_PLAIN)
                .encodingPolicy(B.dot(), EncodingPolicy.FORCE_DICTIONARY)
                .build();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(ROW_COUNT);
        for (int i = 0; i < ROW_COUNT; i++) {
            Map<ColumnPath, Object> row = new HashMap<>();
            row.put(A, Long.valueOf(i));
            row.put(B, Long.valueOf(i % 4));
            rows.add(row);
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(target), SCHEMA, options)) {
            writer.writeBatch(WriteFixtures.batch(SCHEMA, rows));
        }
        return target;
    }

    private static ParquetSchema twoLongColumnsSchema() {
        List<SchemaNode> leaves = List.of(requiredLong(A.name()), requiredLong(B.name()));
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, leaves, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredLong(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }
}
