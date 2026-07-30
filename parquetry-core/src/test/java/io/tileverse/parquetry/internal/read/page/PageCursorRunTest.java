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
package io.tileverse.parquetry.internal.read.page;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.LevelMaxima;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Verifies that walking a column chunk as several per-page runs decodes exactly the pages a single whole-chunk segment
 * walk decodes, with the same row alignment. Per-run ordinal seeding is what keeps
 * {@link PageSelection#isSurviving(int)} and {@link PageSelection#firstRowIndex(int)} correct once the pages between
 * runs are never fetched.
 */
class PageCursorRunTest {

    private static final ColumnPath VALUE = ColumnPath.of("v");
    private static final int ROW_COUNT = 300;
    private static final LevelMaxima FLAT_REQUIRED = new LevelMaxima(0, 0);

    @TempDir
    Path tempDir;

    /** One page as observed by a walk: where its rows start in the row group, and how many values it yielded. */
    private record DecodedPageSummary(long firstRowIndex, int valueCount) {}

    /** The chunk's data-page region as fetched whole, with the metadata a {@link PageCursor} walk needs. */
    private record ChunkFixture(
            MemorySegment dataPages, OffsetIndex offsetIndex, long numValues, Compression codec, long regionOffset) {}

    @Test
    void runWalkMatchesSingleSegmentWalkAndSeedsOrdinals() throws IOException {
        Path file = writeSmallPagesFile();
        try (Arena fetchArena = Arena.ofConfined();
                ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ChunkFixture fixture = fetchWholeChunk(source, fetchArena);
            PageSelection selection = firstAndLastRowsSelection(fixture);

            assertThat(selection.survivingPageCount())
                    .as("two separated surviving pages are what makes the walk cross a run boundary")
                    .isGreaterThanOrEqualTo(2)
                    .isLessThan(selection.pageCount());

            List<DecodedPageSummary> whole = walk(cursorOverSingleSegment(fixture, selection), fixture.codec());
            List<DecodedPageSummary> runs = walk(cursorOverPerPageRuns(fixture, selection), fixture.codec());

            assertThat(runs).isNotEmpty();
            assertThat(runs).isEqualTo(whole);
        }
    }

    @Test
    void multiPageRunWalksOrdinalsUpFromItsBase() throws IOException {
        Path file = writeSmallPagesFile();
        try (Arena fetchArena = Arena.ofConfined();
                ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ChunkFixture fixture = fetchWholeChunk(source, fetchArena);
            PageSelection selection = firstPageAndAdjacentPairSelection(fixture);

            assertThat(survivingOrdinals(selection))
                    .as("the second run must hold two adjacent pages starting at a non-zero ordinal")
                    .containsExactly(0, 2, 3);

            List<DecodedPageSummary> whole = walk(cursorOverSingleSegment(fixture, selection), fixture.codec());
            List<DecodedPageSummary> runs = walk(cursorOverAdjacentPageRuns(fixture, selection), fixture.codec());

            assertThat(runs).hasSize(3);
            assertThat(runs).isEqualTo(whole);
        }
    }

    @Test
    void multiRunWalkWithoutASelectionIsRejected() {
        MemorySegment bytes = MemorySegment.ofArray(new byte[16]).asReadOnly();
        List<DataPageRun> runs = List.of(new DataPageRun(bytes, 0), new DataPageRun(bytes, 4));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new PageCursor(runs, VALUE, null))
                .withMessageContaining("PageSelection");
    }

    @Test
    void emptyRunListYieldsNoPage() throws IOException {
        PageCursor cursor = new PageCursor(List.of(), VALUE, null);
        assertThat(cursor.hasRemaining()).isFalse();
        try (Arena pageArena = Arena.ofConfined()) {
            assertThat(cursor.nextDataPage(FLAT_REQUIRED, Compression.uncompressed(), pageArena))
                    .isNull();
        }
    }

    // -------------------------------------------------------------------------
    // Cursors under test
    // -------------------------------------------------------------------------

    /** Today's shape: the whole data-page region as one segment, ordinals counted from zero. */
    private static PageCursor cursorOverSingleSegment(ChunkFixture fixture, PageSelection selection) {
        return new PageCursor(fixture.dataPages(), VALUE, selection);
    }

    /**
     * The narrowed shape: one run per surviving page, each seeded with that page's offset-index ordinal, with the
     * non-surviving pages absent from the runs entirely.
     */
    private static PageCursor cursorOverPerPageRuns(ChunkFixture fixture, PageSelection selection) {
        List<PageLocation> locations = fixture.offsetIndex().pageLocations();
        List<DataPageRun> runs = new ArrayList<>();
        for (int ordinal = 0; ordinal < locations.size(); ordinal++) {
            if (!selection.isSurviving(ordinal)) {
                continue;
            }
            runs.add(new DataPageRun(sliceOfPage(fixture, locations, ordinal), ordinal));
        }
        return new PageCursor(runs, VALUE, selection);
    }

    /**
     * The production shape: adjacent surviving pages merged into one byte-contiguous run seeded with the stretch's
     * first offset-index ordinal, which is what {@code PageRun.runsFor} emits.
     */
    private static PageCursor cursorOverAdjacentPageRuns(ChunkFixture fixture, PageSelection selection) {
        List<PageLocation> locations = fixture.offsetIndex().pageLocations();
        List<DataPageRun> runs = new ArrayList<>();
        int ordinal = 0;
        while (ordinal < locations.size()) {
            if (!selection.isSurviving(ordinal)) {
                ordinal++;
                continue;
            }
            int runStart = ordinal;
            while (ordinal + 1 < locations.size() && selection.isSurviving(ordinal + 1)) {
                ordinal++;
            }
            runs.add(new DataPageRun(sliceOfPages(fixture, locations, runStart, ordinal), runStart));
            ordinal++;
        }
        return new PageCursor(runs, VALUE, selection);
    }

    /** The bytes of one page (its header plus payload) as a view into the fetched region. */
    private static MemorySegment sliceOfPage(ChunkFixture fixture, List<PageLocation> locations, int ordinal) {
        return sliceOfPages(fixture, locations, ordinal, ordinal);
    }

    /**
     * The bytes of the pages {@code firstOrdinal..lastOrdinal} (headers plus payloads) as a view into the fetched
     * region. The stretch ends where the page after it begins, or at the region end when it runs to the last page.
     */
    private static MemorySegment sliceOfPages(
            ChunkFixture fixture, List<PageLocation> locations, int firstOrdinal, int lastOrdinal) {
        long stretchStart = locations.get(firstOrdinal).offset();
        boolean endsAtLastPage = lastOrdinal + 1 == locations.size();
        long stretchEnd = endsAtLastPage
                ? fixture.regionOffset() + fixture.dataPages().byteSize()
                : locations.get(lastOrdinal + 1).offset();
        long segmentOffset = stretchStart - fixture.regionOffset();
        return fixture.dataPages()
                .asSlice(segmentOffset, stretchEnd - stretchStart)
                .asReadOnly();
    }

    private static PageSelection firstAndLastRowsSelection(ChunkFixture fixture) {
        long numValues = fixture.numValues();
        RowRanges surviving =
                new RowRanges(List.of(new RowRanges.Range(0, 9), new RowRanges.Range(numValues - 10, numValues - 1)));
        return PageSelection.forColumn(fixture.offsetIndex(), numValues, surviving);
    }

    /**
     * Survives page 0 plus pages 2 and 3, addressed by the offset index's own row boundaries rather than by an assumed
     * page size. Page 1 falls out to leave a hole ahead of the adjacent pair.
     */
    private static PageSelection firstPageAndAdjacentPairSelection(ChunkFixture fixture) {
        List<PageLocation> locations = fixture.offsetIndex().pageLocations();
        assertThat(locations.size())
                .as("the fixture must hold enough pages for a hole and a following adjacent pair")
                .isGreaterThanOrEqualTo(5);
        long firstPageLastRow = locations.get(1).firstRowIndex() - 1;
        long pairFirstRow = locations.get(2).firstRowIndex();
        long pairLastRow = locations.get(4).firstRowIndex() - 1;
        RowRanges surviving = new RowRanges(
                List.of(new RowRanges.Range(0, firstPageLastRow), new RowRanges.Range(pairFirstRow, pairLastRow)));
        return PageSelection.forColumn(fixture.offsetIndex(), fixture.numValues(), surviving);
    }

    private static List<Integer> survivingOrdinals(PageSelection selection) {
        List<Integer> ordinals = new ArrayList<>();
        for (int ordinal = 0; ordinal < selection.pageCount(); ordinal++) {
            if (selection.isSurviving(ordinal)) {
                ordinals.add(Integer.valueOf(ordinal));
            }
        }
        return ordinals;
    }

    private static List<DecodedPageSummary> walk(PageCursor cursor, Compression codec) throws IOException {
        List<DecodedPageSummary> decoded = new ArrayList<>();
        while (true) {
            try (Arena pageArena = Arena.ofConfined()) {
                DecodedPage page = cursor.nextDataPage(FLAT_REQUIRED, codec, pageArena);
                if (page == null) {
                    return decoded;
                }
                decoded.add(new DecodedPageSummary(cursor.currentPageFirstRowIndex(), page.valueCount()));
            }
        }
    }

    // -------------------------------------------------------------------------
    // Fetching the chunk whole
    // -------------------------------------------------------------------------

    /**
     * Reads the whole data-page region of the file's single column chunk into {@code arena}. The region spans the first
     * page location through the end of the last one, which excludes any dictionary page just as
     * {@code ColumnChunkSlicer} does.
     */
    private static ChunkFixture fetchWholeChunk(ByteRangeSource source, Arena arena) {
        FileMetaData footer = ParquetFormat.readFooter(source);
        RowGroup rowGroup = footer.rowGroups().get(0);
        ColumnChunk chunk = rowGroup.columns().get(0);
        ColumnMetaData metadata = chunk.metaData()
                .orElseThrow(() -> new IllegalStateException("ColumnChunk missing inline ColumnMetaData"));
        OffsetIndex offsetIndex = readOffsetIndex(source, chunk);
        List<PageLocation> locations = offsetIndex.pageLocations();
        PageLocation last = locations.get(locations.size() - 1);
        long regionOffset = locations.get(0).offset();
        long regionLength = last.offset() + last.compressedPageSize() - regionOffset;
        MemorySegment dataPages = arena.allocate(regionLength);
        source.readFully(regionOffset, dataPages);
        assertThat(metadata.codec())
                .as("the fixture is written uncompressed; the walk decompresses accordingly")
                .isEqualTo(CompressionCodec.UNCOMPRESSED);
        return new ChunkFixture(
                dataPages.asReadOnly(), offsetIndex, metadata.numValues(), Compression.uncompressed(), regionOffset);
    }

    private static OffsetIndex readOffsetIndex(ByteRangeSource source, ColumnChunk chunk) {
        long offset = chunk.offsetIndexOffset()
                .orElseThrow(() -> new IllegalStateException("writer did not emit an OffsetIndex"));
        int length = chunk.offsetIndexLength()
                .orElseThrow(() -> new IllegalStateException("writer did not emit an OffsetIndex length"));
        return ParquetFormat.readOffsetIndex(source, offset, length);
    }

    // -------------------------------------------------------------------------
    // File writing
    // -------------------------------------------------------------------------

    /** Writes one row group of {@value #ROW_COUNT} rows over a single long column, split into many small pages. */
    private Path writeSmallPagesFile() throws IOException {
        Path file = tempDir.resolve("small-pages.parquet");
        ParquetSchema schema = singleLongColumnSchema();
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .pageByteLimit(256)
                .defaultCompression(Compression.uncompressed())
                .rowGroupSize(RowGroupSize.rows(1_000))
                .build();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int i = 0; i < ROW_COUNT; i++) {
            Map<ColumnPath, Object> row = new HashMap<>();
            row.put(VALUE, Long.valueOf(i));
            rows.add(row);
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }
        return file;
    }

    private static ParquetSchema singleLongColumnSchema() {
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                VALUE.name(), Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(value), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
