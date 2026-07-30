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

import static io.tileverse.parquetry.filter.Pred.and;
import static io.tileverse.parquetry.filter.Pred.col;
import static io.tileverse.parquetry.filter.Pred.or;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.EncodingPolicy;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.LatencyModel;
import io.tileverse.parquetry.io.RecordingByteRangeSource;
import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * End-to-end behavior of the page-narrowed row-group fetch, driven through the public read API against a real file and
 * measured with a {@link RecordingByteRangeSource}: a narrowed read returns exactly the rows a whole-chunk read
 * returns, it moves strictly fewer bytes, it never touches a page the selection rejected, and it falls back to whole
 * chunks in every shape where the decode side has no page-skip mask.
 *
 * <p>The fixture is one file of {@value #ROW_COUNT} rows in two row groups of {@value #ROWS_PER_ROW_GROUP}, over three
 * flat columns: {@code id} (a long counting up, {@code PLAIN}), {@code name} (four distinct strings,
 * dictionary-encoded, hence a dictionary page ahead of its first data page), and {@code payload} (a long,
 * {@code PLAIN}). A {@value #PAGE_BYTE_LIMIT}-byte page limit and no compression split each long column into many small
 * pages per row group, which lets an {@code id} range name the pages that must survive.
 *
 * <p>Byte-exact assertions build the runtime with a zero coalesce gap, since the default 1 MiB gap re-merges the holes
 * between surviving runs. The parity and alignment cases deliberately keep a gap wider than any hole, exercising the
 * merged-range path where a mis-seeded page ordinal would silently return the wrong rows.
 */
class PageNarrowedFetchReadTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath NAME = ColumnPath.of("name");
    private static final ColumnPath PAYLOAD = ColumnPath.of("payload");

    private static final int ROWS_PER_ROW_GROUP = 1_000;
    private static final int ROW_COUNT = 2 * ROWS_PER_ROW_GROUP;
    private static final int PAGE_BYTE_LIMIT = 256;

    private static final int NO_COALESCE_GAP = 0;
    private static final int GAP_WIDER_THAN_ANY_HOLE = 1 << 20;

    /** A file whose footer records no offset index, hence no page can be located and no fetch can be narrowed. */
    private static final String CORPUS_FILE_WITHOUT_PAGE_INDEX = "alltypes_plain.parquet";

    /** A file whose only non-trivial column is a three-deep repeated group, hence the scan is not flat. */
    private static final String NESTED_CORPUS_FILE = "nested_lists.snappy.parquet";

    /**
     * The rows {@link #FIRST_AND_LAST_PAGES} leaves alive in the first row group: {@code [0, 10)} and {@code [990,
     * 1000)}. Both the predicate and the search for a page holding no surviving row are stated in terms of these two
     * bounds.
     */
    private static final long FIRST_RUN_END_ROW = 10L;

    private static final long LAST_RUN_START_ROW = 990L;

    /**
     * Ids 0-9 plus 990-1009: two surviving runs in the first row group (its first and its last page) and one in the
     * second (its first page). The two-run group is what run slicing and per-run ordinal seeding exist for.
     */
    private static final Predicate FIRST_AND_LAST_PAGES = or(
            col("id").lt(FIRST_RUN_END_ROW),
            and(col("id").gtEq(LAST_RUN_START_ROW), col("id").lt(ROWS_PER_ROW_GROUP + FIRST_RUN_END_ROW)));

    /** Ids 500-509, all inside one page of the first row group; no id in the second row group can match. */
    private static final Predicate ONE_MID_FILE_PAGE =
            and(col("id").gtEq(500L), col("id").lt(510L));

    /**
     * The round trip of a slow link, the regime the narrowed fetch is meant for. The bandwidth that makes the saved
     * bytes outweigh the added round trips is derived from the legs themselves, since the crossover depends on the
     * ratio between the two and not on absolute size: this fixture saves tens of kilobytes for a handful of extra
     * requests, which needs a slow link to pay off, while a production row group saves tens of megabytes and pays off
     * at object-store bandwidths.
     */
    private static final long BYTE_BOUND_RTT_NANOS = 50_000_000L;

    /** The opposite regime: round trips are expensive and bytes are nearly free, where narrowing can lose. */
    private static final long REQUEST_BOUND_RTT_NANOS = 200_000_000L;

    private static final long REQUEST_BOUND_BYTES_PER_SECOND = 1_000_000_000L;

    private static final ParquetSchema SCHEMA = idNamePayloadSchema();

    @TempDir
    static Path tempDir;

    private static Path file;

    @BeforeAll
    static void writeFixtureFile() throws IOException {
        file = writeTwoRowGroupsOfManyPages();
    }

    // -------------------------------------------------------------------------
    // Parity: the narrowed read and the whole-chunk read return the same rows
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("parityCases")
    void narrowedReadReturnsExactlyTheRowsOfTheWholeChunkRead(
            String shape, Predicate predicate, Projection projection) {
        Leg<List<Object>> narrowed = readRows(file, predicate, projection, narrowing(true), GAP_WIDER_THAN_ANY_HOLE);
        Leg<List<Object>> baseline = readRows(file, predicate, projection, narrowing(false), GAP_WIDER_THAN_ANY_HOLE);

        assertThat(baseline.rows())
                .as("the predicate must match rows for a parity check to mean anything: %s", shape)
                .isNotEmpty();
        assertThat(narrowed.rows())
                .as("narrowing changes which bytes are fetched, never which rows come out: %s", shape)
                .isEqualTo(baseline.rows());
        assertThat(narrowed.bytesRead())
                .as("equal rows prove nothing unless the narrowed leg really fetched less: %s", shape)
                .isLessThan(baseline.bytesRead());
    }

    /**
     * Shapes whose narrowed leg was measured to fetch strictly less than its whole-chunk leg even at
     * {@link #GAP_WIDER_THAN_ANY_HOLE}, where coalescing gives back most of the saving: each one keeps at least a fifth
     * of it, which is what lets the parity test double as an engagement guard.
     */
    private static Stream<Arguments> parityCases() {
        return Stream.of(
                Arguments.of("two surviving runs in one row group, every column", FIRST_AND_LAST_PAGES, Projection.ALL),
                Arguments.of("one surviving page mid-file, every column", ONE_MID_FILE_PAGE, Projection.ALL),
                Arguments.of(
                        "one surviving page mid-file, the dictionary-encoded column projected",
                        ONE_MID_FILE_PAGE,
                        Projection.ofPhysical(List.of(ID, NAME))));
    }

    @Test
    void multiRunSurvivorsWithAHoleSmallerThanTheCoalesceGapStayAligned() {
        Leg<List<Object>> narrowed =
                readRows(file, FIRST_AND_LAST_PAGES, Projection.ALL, narrowing(true), GAP_WIDER_THAN_ANY_HOLE);
        Leg<List<Object>> baseline =
                readRows(file, FIRST_AND_LAST_PAGES, Projection.ALL, narrowing(false), GAP_WIDER_THAN_ANY_HOLE);
        ByteExtent deadPage = firstPageWithoutASurvivingRow(ID);

        assertThat(idsOf(narrowed))
                .as("a run seeded with the wrong page ordinal decodes the wrong rows without failing")
                .isEqualTo(idsMatchingFirstAndLastPages());
        assertThat(narrowed.bytesRead())
                .as("the merged range must still be narrower than the whole chunks it was cut from, or nothing here"
                        + " exercises the merged-range path this test exists for")
                .isLessThan(baseline.bytesRead());
        assertThat(narrowed.bytesInRange(deadPage.start(), deadPage.end()))
                .as("a gap wider than the hole between the two runs merges them into one range that spans dead pages")
                .isPositive();
    }

    // -------------------------------------------------------------------------
    // Bytes: the narrowed read skips the pages the selection rejected
    // -------------------------------------------------------------------------

    @Test
    void narrowedFetchReadsFewerBytesAndSkipsDeadPages() {
        Leg<List<Object>> narrowed =
                readRows(file, FIRST_AND_LAST_PAGES, Projection.ALL, narrowing(true), NO_COALESCE_GAP);
        Leg<List<Object>> baseline =
                readRows(file, FIRST_AND_LAST_PAGES, Projection.ALL, narrowing(false), NO_COALESCE_GAP);
        ByteExtent deadPage = firstPageWithoutASurvivingRow(ID);

        assertThat(narrowed.rows()).isEqualTo(baseline.rows());
        assertThat(narrowed.bytesRead())
                .as("thirty surviving rows must not cost the whole file's column data")
                .isLessThan(baseline.bytesRead());
        assertThat(baseline.bytesInRange(deadPage.start(), deadPage.end()))
                .as("the whole-chunk read pays for the dead page at [%d, %d)", deadPage.start(), deadPage.end())
                .isPositive();
        assertThat(narrowed.bytesInRange(deadPage.start(), deadPage.end()))
                .as(
                        "the narrowed read must not read one byte of the dead page at [%d, %d)",
                        deadPage.start(), deadPage.end())
                .isZero();
    }

    @Test
    void dictionaryPrefixIsFetchedAndDecodes() {
        Projection withDictionaryColumn = Projection.ofPhysical(List.of(ID, NAME));
        Leg<List<Object>> narrowed =
                readRows(file, ONE_MID_FILE_PAGE, withDictionaryColumn, narrowing(true), NO_COALESCE_GAP);
        Leg<List<Object>> baseline =
                readRows(file, ONE_MID_FILE_PAGE, withDictionaryColumn, narrowing(false), NO_COALESCE_GAP);
        ByteExtent prefix = dictionaryPrefixOfFirstRowGroup(NAME);

        assertThat(narrowed.rows())
                .as("the dictionary-encoded column decodes only if its dictionary page came along")
                .isEqualTo(baseline.rows());
        assertThat(narrowed.bytesInRange(prefix.start(), prefix.end()))
                .as("the narrowed plan fetches the dictionary prefix at [%d, %d)", prefix.start(), prefix.end())
                .isPositive();
    }

    // -------------------------------------------------------------------------
    // Fallback: no decode-side mask means no narrowing
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("wholeChunkCases")
    void narrowingDisablesExactlyWhenTheMaskDoes(String why, Corpus corpus, Predicate predicate, ReadOptions base) {
        Path target = pathOf(corpus);

        Leg<List<Object>> narrowed =
                readRows(target, predicate, Projection.ALL, narrowing(base, true), NO_COALESCE_GAP);
        Leg<List<Object>> baseline =
                readRows(target, predicate, Projection.ALL, narrowing(base, false), NO_COALESCE_GAP);

        assertThat(baseline.rows())
                .as("the read must produce rows for the byte comparison to mean anything")
                .isNotEmpty();
        assertThat(narrowed.rows()).isEqualTo(baseline.rows());
        assertThat(narrowed.bytesRead())
                .as("the flag can only narrow what the decode side already skips: %s", why)
                .isEqualTo(baseline.bytesRead());
    }

    private static Stream<Arguments> wholeChunkCases() {
        ReadOptions withoutColumnIndexTier =
                ReadOptions.DEFAULTS.toBuilder().useColumnIndexFilter(false).build();
        return Stream.of(
                Arguments.of(
                        "the column-index tier is off, hence no row group is narrowed to a page selection",
                        Corpus.WRITTEN_FIXTURE,
                        FIRST_AND_LAST_PAGES,
                        withoutColumnIndexTier),
                Arguments.of(
                        "a file without offset indexes cannot locate its pages",
                        Corpus.WITHOUT_PAGE_INDEX,
                        col("id").gtEq(3),
                        ReadOptions.DEFAULTS),
                Arguments.of(
                        "a full scan keeps every page alive",
                        Corpus.WRITTEN_FIXTURE,
                        Predicate.ALWAYS_TRUE,
                        ReadOptions.DEFAULTS));
    }

    @Test
    void nestedFileReadsWholeChunksUnderTheFlag() {
        Path nested = CorpusFixtures.parquetTestingData().resolve(NESTED_CORPUS_FILE);
        long expectedRows = rowCountOf(nested);

        // A nested shape's cell values are views that do not outlive the stream; this leg asserts row counts and bytes.
        Leg<Integer> narrowed = read(
                nested,
                Predicate.ALWAYS_TRUE,
                Projection.ALL,
                narrowing(true),
                NO_COALESCE_GAP,
                ParquetRecord::columnCount);
        Leg<Integer> baseline = read(
                nested,
                Predicate.ALWAYS_TRUE,
                Projection.ALL,
                narrowing(false),
                NO_COALESCE_GAP,
                ParquetRecord::columnCount);

        assertThat(narrowed.rows()).hasSize(Math.toIntExact(expectedRows));
        assertThat(narrowed.bytesRead())
                .as("a repeated column has no page-skip mask, hence nothing to narrow to")
                .isEqualTo(baseline.bytesRead());
    }

    // -------------------------------------------------------------------------
    // What the win looks like from outside: projected wall clock and analyze
    // -------------------------------------------------------------------------

    @Test
    void latencyModelProjectsTheNarrowedWinOnSlowLinks() {
        Leg<List<Object>> narrowed =
                readRows(file, FIRST_AND_LAST_PAGES, Projection.ALL, narrowing(true), NO_COALESCE_GAP);
        Leg<List<Object>> baseline =
                readRows(file, FIRST_AND_LAST_PAGES, Projection.ALL, narrowing(false), NO_COALESCE_GAP);

        long savedBytes = baseline.bytesRead() - narrowed.bytesRead();
        long extraRequests = narrowed.requestCount() - baseline.requestCount();

        assertThat(extraRequests)
                .as("narrowing trades round trips for bytes; with no extra trip there is no trade to project")
                .isPositive();

        long byteBoundBytesPerSecond = byteBoundBandwidth(savedBytes, extraRequests);
        long narrowedWhenBytesCost = projectedNanos(narrowed, BYTE_BOUND_RTT_NANOS, byteBoundBytesPerSecond);
        long baselineWhenBytesCost = projectedNanos(baseline, BYTE_BOUND_RTT_NANOS, byteBoundBytesPerSecond);

        assertThat(narrowedWhenBytesCost)
                .as(
                        "where bytes dominate, fetching a fraction of the chunk wins: %d vs %d nanos over %d vs %d"
                                + " requests",
                        narrowedWhenBytesCost, baselineWhenBytesCost, narrowed.requestCount(), baseline.requestCount())
                .isLessThan(baselineWhenBytesCost);

        // The opposite regime, where round trips dominate and the extra per-run requests can outweigh the bytes saved.
        // The trade is real; the test records both projections rather than asserting a winner in either direction.
        assertThat(projectedNanos(narrowed, REQUEST_BOUND_RTT_NANOS, REQUEST_BOUND_BYTES_PER_SECOND))
                .isPositive();
        assertThat(projectedNanos(baseline, REQUEST_BOUND_RTT_NANOS, REQUEST_BOUND_BYTES_PER_SECOND))
                .isPositive();
    }

    @Test
    void analyzeReportsTheNarrowedBytes() {
        QueryStats narrowed = analyze(FIRST_AND_LAST_PAGES, Projection.ALL, narrowing(true));
        QueryStats baseline = analyze(FIRST_AND_LAST_PAGES, Projection.ALL, narrowing(false));

        assertThat(narrowed.totalFetch().pageBytes())
                .as("analyze reports the bytes the fetch really moved, not the whole-chunk plan")
                .isLessThan(baseline.totalFetch().pageBytes());
        assertThat(narrowed.pagesDecoded())
                .as("the same pages decode either way; narrowing changes only what is fetched")
                .isEqualTo(baseline.pagesDecoded());
        assertThat(narrowed.pagesPruned())
                .as("a page never fetched is never header-walked, hence counted by neither page counter")
                .isLessThanOrEqualTo(baseline.pagesPruned());
    }

    // -------------------------------------------------------------------------
    // Reading
    // -------------------------------------------------------------------------

    /** One recorded read leg: what the read produced, in row order, and the source that recorded its byte ranges. */
    private record Leg<T>(List<T> rows, RecordingByteRangeSource io) {

        long bytesRead() {
            return io.bytesRead();
        }

        int requestCount() {
            return io.requestCount();
        }

        long bytesInRange(long lo, long hi) {
            return io.bytesInRange(lo, hi);
        }
    }

    private static Leg<List<Object>> readRows(
            Path target, Predicate predicate, Projection projection, ReadOptions options, int coalesceGap) {
        return read(target, predicate, projection, options, coalesceGap, PageNarrowedFetchReadTest::stableCellsOf);
    }

    /**
     * Reads {@code target} through a recording source, mapping each row with {@code cells} while the row's backing
     * buffers are still live, and returns the mapped rows together with the recording.
     */
    private static <T> Leg<T> read(
            Path target,
            Predicate predicate,
            Projection projection,
            ReadOptions options,
            int coalesceGap,
            Function<ParquetRecord, T> cells) {
        ParquetRuntime runtime =
                ParquetRuntime.builder().maxCoalesceGap(coalesceGap).build();
        RecordingByteRangeSource recording = new RecordingByteRangeSource(ByteRangeSource.ofFile(target));
        List<T> rows = new ArrayList<>();
        try (recording) {
            ParquetFileReader reader = ParquetFileReader.open(recording, runtime, Optional.empty());
            try (Stream<ParquetRecord> stream = reader.read(predicate, projection, options)) {
                stream.forEach(record -> rows.add(cells.apply(record)));
            }
        }
        return new Leg<>(rows, recording);
    }

    /**
     * One row's cells as values that outlive the stream. A record is a flyweight over the fetched buffers: a binary
     * cell hands out a {@link MemorySegment} view whose identity, not content, decides equality, and whose buffer is
     * released when the stream closes. Hex-formatting the bytes keeps such a cell comparable across two legs.
     */
    private static List<Object> stableCellsOf(ParquetRecord record) {
        List<Object> cells = new ArrayList<>(record.columnCount());
        for (int col = 0; col < record.columnCount(); col++) {
            cells.add(stableCell(record, col));
        }
        return cells;
    }

    private static Object stableCell(ParquetRecord record, int col) {
        Object value = record.get(col);
        if (value instanceof MemorySegment segment) {
            return HexFormat.of().formatHex(segment.toArray(ValueLayout.JAVA_BYTE));
        }
        return value;
    }

    private static QueryStats analyze(Predicate predicate, Projection projection, ReadOptions options) {
        ParquetRuntime runtime =
                ParquetRuntime.builder().maxCoalesceGap(NO_COALESCE_GAP).build();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ExplainPlan plan = ParquetFileReader.open(source, runtime, Optional.empty())
                    .explainAnalyze(predicate, projection, options);
            return plan.execution().orElseThrow();
        }
    }

    private static long projectedNanos(Leg<?> leg, long rttNanos, long bytesPerSecond) {
        return LatencyModel.wallNanos(leg.requestCount(), leg.bytesRead(), rttNanos, bytesPerSecond);
    }

    /**
     * Half the bandwidth at which the two legs cost the same wall clock. At the crossover, transferring
     * {@code savedBytes} takes exactly as long as {@code extraRequests} round trips; halving it doubles what those
     * bytes cost and leaves the narrowed leg a factor-of-two margin, whatever the fixture's absolute size.
     */
    private static long byteBoundBandwidth(long savedBytes, long extraRequests) {
        long crossoverNanos = 2 * extraRequests * BYTE_BOUND_RTT_NANOS;
        return savedBytes * 1_000_000_000L / crossoverNanos;
    }

    private static ReadOptions narrowing(boolean on) {
        return narrowing(ReadOptions.DEFAULTS, on);
    }

    private static ReadOptions narrowing(ReadOptions base, boolean on) {
        return base.toBuilder().usePageNarrowedFetch(on).build();
    }

    /** The {@code id} cell of a row read under {@link Projection#ALL}, which keeps the schema's column order. */
    private static List<Object> idsOf(Leg<List<Object>> leg) {
        return leg.rows().stream().map(row -> row.get(0)).toList();
    }

    /** The ids {@link #FIRST_AND_LAST_PAGES} matches, in file order: an oracle that no read took part in. */
    private static List<Object> idsMatchingFirstAndLastPages() {
        List<Object> ids = new ArrayList<>();
        for (long id = 0; id < FIRST_RUN_END_ROW; id++) {
            ids.add(Long.valueOf(id));
        }
        for (long id = LAST_RUN_START_ROW; id < ROWS_PER_ROW_GROUP + FIRST_RUN_END_ROW; id++) {
            ids.add(Long.valueOf(id));
        }
        return ids;
    }

    // -------------------------------------------------------------------------
    // Footer inspection
    // -------------------------------------------------------------------------

    /** A half-open byte extent of the fixture file. */
    private record ByteExtent(long start, long end) {}

    /**
     * The extent of the first page of {@code path}'s chunk in the first row group that holds no row
     * {@link #FIRST_AND_LAST_PAGES} matches, read from the file's own offset index. A page whose rows fall entirely
     * between the two surviving runs is one the narrowed fetch must never touch.
     */
    private static ByteExtent firstPageWithoutASurvivingRow(ColumnPath path) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            List<PageLocation> pages = pageLocations(source, firstRowGroupChunk(source, path));
            for (int i = 0; i < pages.size(); i++) {
                PageLocation page = pages.get(i);
                long endRow = i + 1 < pages.size() ? pages.get(i + 1).firstRowIndex() : ROWS_PER_ROW_GROUP;
                if (page.firstRowIndex() >= FIRST_RUN_END_ROW && endRow <= LAST_RUN_START_ROW) {
                    return new ByteExtent(page.offset(), page.offset() + page.compressedPageSize());
                }
            }
            throw new IllegalStateException("Fixture column " + path.dot()
                    + " has no page between the surviving runs; the byte assertions need one");
        }
    }

    /** The extent between {@code path}'s chunk start and its first data page in the first row group. */
    private static ByteExtent dictionaryPrefixOfFirstRowGroup(ColumnPath path) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ColumnChunk chunk = firstRowGroupChunk(source, path);
            ColumnMetaData meta = chunk.metaData().orElseThrow();
            long dictionaryPageOffset = meta.dictionaryPageOffset()
                    .orElseThrow(() ->
                            new IllegalStateException("Fixture column " + path.dot() + " must be dictionary-encoded"));
            long firstDataPageOffset = pageLocations(source, chunk).get(0).offset();
            return new ByteExtent(dictionaryPageOffset, firstDataPageOffset);
        }
    }

    private static ColumnChunk firstRowGroupChunk(ByteRangeSource source, ColumnPath path) {
        RowGroup rowGroup = ParquetFormat.readFooter(source).rowGroups().get(0);
        for (ColumnChunk chunk : rowGroup.columns()) {
            ColumnMetaData meta = chunk.metaData().orElseThrow();
            if (ColumnPath.of(meta.pathInSchema()).equals(path)) {
                return chunk;
            }
        }
        throw new IllegalStateException("Fixture row group has no column " + path.dot());
    }

    private static List<PageLocation> pageLocations(ByteRangeSource source, ColumnChunk chunk) {
        OffsetIndex offsetIndex = ParquetFormat.readOffsetIndex(
                source,
                chunk.offsetIndexOffset().orElseThrow(),
                chunk.offsetIndexLength().orElseThrow());
        return offsetIndex.pageLocations();
    }

    private static long rowCountOf(Path target) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(target)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            return footer.numRows();
        }
    }

    // -------------------------------------------------------------------------
    // Files under test
    // -------------------------------------------------------------------------

    /** Which file a fallback case reads; resolved inside the test because the fixture is written per class run. */
    private enum Corpus {
        WRITTEN_FIXTURE,
        WITHOUT_PAGE_INDEX
    }

    private static Path pathOf(Corpus corpus) {
        return switch (corpus) {
            case WRITTEN_FIXTURE -> file;
            case WITHOUT_PAGE_INDEX -> corpusFileWithoutPageIndex();
        };
    }

    /** The corpus file the fallback case needs, checked to really lack the offset indexes narrowing depends on. */
    private static Path corpusFileWithoutPageIndex() {
        Path target = CorpusFixtures.parquetTestingData().resolve(CORPUS_FILE_WITHOUT_PAGE_INDEX);
        try (ByteRangeSource source = ByteRangeSource.ofFile(target)) {
            for (RowGroup rowGroup : ParquetFormat.readFooter(source).rowGroups()) {
                for (ColumnChunk chunk : rowGroup.columns()) {
                    if (chunk.offsetIndexOffset().isPresent()) {
                        throw new IllegalStateException(CORPUS_FILE_WITHOUT_PAGE_INDEX
                                + " now records an offset index; the fallback case needs a file without one");
                    }
                }
            }
        }
        return target;
    }

    /**
     * Writes {@value #ROW_COUNT} rows as two row groups of {@value #ROWS_PER_ROW_GROUP}. {@code id} counts up over the
     * whole file, giving every page a distinct id range to filter on; {@code name} cycles over four values and is
     * pinned to dictionary encoding; {@code payload} is a second long column that must ride along untouched.
     */
    private static Path writeTwoRowGroupsOfManyPages() throws IOException {
        Path target = tempDir.resolve("page-narrowed-fetch-read.parquet");
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .pageByteLimit(PAGE_BYTE_LIMIT)
                .defaultCompression(Compression.uncompressed())
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_ROW_GROUP))
                .encodingPolicy(ID.dot(), EncodingPolicy.FORCE_PLAIN)
                .encodingPolicy(NAME.dot(), EncodingPolicy.FORCE_DICTIONARY)
                .encodingPolicy(PAYLOAD.dot(), EncodingPolicy.FORCE_PLAIN)
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(target), SCHEMA, options)) {
            writer.writeBatch(WriteFixtures.batch(SCHEMA, rows(0, ROWS_PER_ROW_GROUP)));
            writer.writeBatch(WriteFixtures.batch(SCHEMA, rows(ROWS_PER_ROW_GROUP, ROW_COUNT)));
        }
        return target;
    }

    private static List<Map<ColumnPath, Object>> rows(int fromId, int toId) {
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(toId - fromId);
        for (int id = fromId; id < toId; id++) {
            rows.add(Map.of(ID, Long.valueOf(id), NAME, "name-" + (id % 4), PAYLOAD, Long.valueOf(id * 7L)));
        }
        return rows;
    }

    private static ParquetSchema idNamePayloadSchema() {
        List<SchemaNode> leaves =
                List.of(requiredLong(ID.name()), requiredString(NAME.name()), requiredLong(PAYLOAD.name()));
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, leaves, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredLong(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredString(String name) {
        return new SchemaNode.Primitive(
                name,
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
    }
}
