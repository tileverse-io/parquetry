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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeRequest;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.EncodingPolicy;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.EncodingStats;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.internal.filter.PageIndex;
import io.tileverse.parquetry.internal.footer.ChunkMeta;
import io.tileverse.parquetry.internal.read.PageIndexLearner.WholeChunk;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.io.SegmentPool.Pooled;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.observe.FetchStats;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.DiskBudget;
import io.tileverse.parquetry.runtime.FetchBudget;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaBuilder;
import io.tileverse.parquetry.testsupport.CorpusFixtures;
import io.tileverse.parquetry.testsupport.CountingByteRangeSource;
import io.tileverse.parquetry.testsupport.FooterRewrite;
import io.tileverse.parquetry.testsupport.RecordingByteRangeSource;

/**
 * Read-through behavioral test for the mandatory row-group fetch routing through the {@link FetchBufferAllocator}
 * valve. Constructing a {@link RowGroupFetcher} in isolation needs a footer-derived {@code RowGroupChunks}, a
 * {@code FetchPlan}, and an {@code IndexSectionLoader}, which only the read pipeline assembles. A read against a real
 * file with a tiny {@link FetchBudget} exercises the exact wiring this test guards: the prefetcher's inline path calls
 * {@code RowGroupFetcher.fetch}, which now allocates one buffer per row group through the valve. The test pins a
 * tiny-RAM/ample-disk runtime and asserts both that the mandatory fetch spilled to a disk mapping and that the rows
 * read are byte-identical to a default RAM runtime.
 *
 * <p>To check the learner, the tests fetch one row group of two long columns in pages of {@value #PAGE_VALUES} values.
 * A fetch planned without a mask hands its whole chunks to the learner of the read, and a fetch through a mask hands
 * nothing.
 *
 * <p>To check the dictionary prefix read on demand, the tests fetch one row group of {@value #MIXED_ROWS} rows in pages
 * of {@value #MIXED_PAGE_VALUES} values. Column {@code a} is PLAIN. Columns {@code b} and {@code c} have a dictionary
 * page, and their first {@value #DICTIONARY_PAGES} pages use it. Their later pages are PLAIN. An edited footer gives
 * the count of dictionary-encoded pages, because parquetry's writer records none.
 *
 * <p>To check the decode ahead, the tests fetch {@value #GROUPS} row groups of {@value #GROUP_ROWS} rows in turn with
 * one fetcher, as a read does. Column {@code a} is PLAIN. Columns {@code b} and {@code c} repeat
 * {@value #GROUP_DISTINCT_VALUES} values and have a dictionary page in each row group. Each chunk holds two pages, and
 * a mask keeping the first page narrows its fetch. The pool of the fetcher counts the fetch buffers and the scratch of
 * each decode ahead. A second pool counts the scratch of the decodes by readers.
 */
class RowGroupFetcherTest {

    private static final Path FILE = CorpusFixtures.parquetTestingData().resolve("alltypes_plain.parquet");

    private static final ColumnPath A = ColumnPath.of("a");
    private static final ColumnPath B = ColumnPath.of("b");
    private static final ColumnPath C = ColumnPath.of("c");
    private static final int PAGED_ROWS = 300;
    private static final int PAGE_VALUES = 100;

    private static final int MIXED_ROWS = 100;
    private static final int MIXED_PAGE_VALUES = 10;
    private static final int MIXED_PAGES = MIXED_ROWS / MIXED_PAGE_VALUES;
    private static final int DICTIONARY_PAGES = 6;
    private static final int DICTIONARY_ROWS = DICTIONARY_PAGES * MIXED_PAGE_VALUES;
    private static final int DICTIONARY_BYTE_LIMIT = 40;

    private static final int GROUPS = 2;
    private static final int GROUP_ROWS = 40;
    private static final int GROUP_PAGE_VALUES = GROUP_ROWS / 2;
    private static final int GROUP_DISTINCT_VALUES = 4;

    @TempDir
    Path tempDir;

    @Test
    @Timeout(30)
    void mandatoryFetchSpillsToDiskWhenRamIsExhaustedAndReadsCorrectly() {
        long ampleDisk = 256L << 20;
        ParquetRuntime ramRuntime = ParquetRuntime.builder().build();
        DiskBudget spillDisk = DiskBudget.ofBytes(ampleDisk);
        ParquetRuntime spillingRuntime = ParquetRuntime.builder()
                // below the bytes requested by any row group's plan, forcing the mandatory fetch to spill
                .fetchBudget(FetchBudget.ofBytes(1))
                .diskBudget(spillDisk)
                .build();

        List<Map<ColumnPath, Object>> baseline = readAllRows(ramRuntime);
        List<Map<ColumnPath, Object>> spilled = readAllRows(spillingRuntime);

        assertThat(spilled).as("rows read via the spilled mandatory fetch").isEqualTo(baseline);
        // The spill file is unmapped (and its disk reservation returned) as soon as the row group is decoded, which
        // can happen before any row is pulled. Assert against the low-water mark, which records the reservation even
        // after it has been released, rather than the live available bytes, which race the release.
        assertThat(spillDisk.minAvailable())
                .as("the mandatory fetch reserved disk for a spill mapping")
                .isLessThan(spillDisk.capacity());
    }

    /** Reads every row into a list of column-to-value maps. */
    private static List<Map<ColumnPath, Object>> readAllRows(ParquetRuntime runtime) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(FILE)) {
            ParquetFileReader dataset = ParquetFileReader.open(source, runtime, Optional.empty());
            List<ColumnPath> leaves = dataset.schema().leafColumns();
            try (Stream<ParquetRecord> rows =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                return collectRows(rows, leaves);
            }
        }
    }

    private static List<Map<ColumnPath, Object>> collectRows(Stream<ParquetRecord> rows, List<ColumnPath> leaves) {
        List<Map<ColumnPath, Object>> collected = new ArrayList<>();
        for (ParquetRecord row : (Iterable<ParquetRecord>) rows::iterator) {
            collected.add(snapshot(row, leaves));
        }
        return collected;
    }

    /** Materializes every leaf value for one row, copying binary leaves to bytes so equality compares by content. */
    private static Map<ColumnPath, Object> snapshot(ParquetRecord row, List<ColumnPath> leaves) {
        Map<ColumnPath, Object> values = new LinkedHashMap<>();
        for (ColumnPath leaf : leaves) {
            values.put(leaf, valueOf(row, leaf));
        }
        return values;
    }

    private static Object valueOf(ParquetRecord row, ColumnPath leaf) {
        if (row.isNull(leaf)) {
            return null;
        }
        Object raw = row.get(leaf);
        if (raw instanceof MemorySegment segment) {
            return Arrays.toString(segment.toArray(ValueLayout.JAVA_BYTE));
        }
        return raw;
    }

    // the first fetch of whole chunks leaves the dictionary pages undecoded, and its buffer is the only borrow
    @Test
    void fetchDecodesNoDictionary() {
        Path file = CorpusFixtures.parquetTestingData().resolve("alltypes_dictionary.parquet");
        SegmentPool pool = SegmentPool.create();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            ParquetSchema schema = SchemaBuilder.build(footer.schema());
            RowGroupSurvivor survivor =
                    RowGroupSurvivor.full(TestRowGroupChunks.of(footer, 0, schema, TestRowGroupChunks.reading(source)));
            RowGroupFetcher fetcher = TestFetchers.over(source, schema, schema, pool);
            FetchPlan plan = fetcher.planFor(survivor, Optional.empty());
            try (RowGroupFetch fetch = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                long dictionaryColumns = fetch.columns().stream()
                        .filter(column -> column.dictionaryPage().isPresent())
                        .count();
                assertThat(dictionaryColumns)
                        .as("the fixture is dictionary-encoded")
                        .isPositive();
                assertThat(pool.stats().totalBorrows())
                        .as("one borrow for the fetch buffer")
                        .isEqualTo(1L);
            }
            assertThat(pool.stats().outstandingBorrows()).isZero();
        } finally {
            pool.close();
        }
    }

    @Test
    // S7466: the project style declares explicit local types rather than var
    @SuppressWarnings("java:S7466")
    void wholeFetchHandsChunksOver() throws IOException {
        RecordingLearner learner = new RecordingLearner(0);
        try (ByteRangeSource source = ByteRangeSource.ofFile(writePagedColumns())) {
            RowGroupSurvivor survivor = pagedSurvivor(source);
            RowGroupFetcher fetcher = TestFetchers.learning(source, pagedSchema(), SegmentPool.getDefault(), learner);
            FetchPlan plan = fetcher.planFor(survivor, Optional.empty());

            try (RowGroupFetch _ = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                assertThat(plan.unmasked()).isTrue();
            }

            assertThat(learner.calls()).isEqualTo(1);
            assertThat(learner.chunks()).extracting(WholeChunk::path).containsExactlyInAnyOrder(A, B);
            for (WholeChunk chunk : learner.chunks()) {
                String column = chunk.path().dot();
                assertThat(chunk.bytes().toArray(ValueLayout.JAVA_BYTE))
                        .as("the bytes of column %s from its first byte", column)
                        .isEqualTo(bytesOf(source, chunk.meta()));
            }
        }
    }

    @Test
    // S7466: the project style declares explicit local types rather than var
    @SuppressWarnings("java:S7466")
    void maskedFetchLearnsNothing() throws IOException {
        RecordingLearner learner = new RecordingLearner(0);
        try (ByteRangeSource source = ByteRangeSource.ofFile(writePagedColumns())) {
            RowGroupSurvivor survivor = pagedSurvivor(source);
            RowGroupFetcher fetcher = TestFetchers.learning(source, pagedSchema(), SegmentPool.getDefault(), learner);
            FetchPlan plan = fetcher.planFor(survivor, Optional.of(firstPageOf(survivor)));

            try (RowGroupFetch _ = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                assertThat(plan.unmasked()).isFalse();
            }

            assertThat(learner.calls()).isZero();
        }
    }

    @Test
    // S7466: the project style declares explicit local types rather than var
    @SuppressWarnings("java:S7466")
    void refusedGroupIsNotHandedOver() throws IOException {
        RecordingLearner learner = new RecordingLearner(1);
        try (ByteRangeSource source = ByteRangeSource.ofFile(writePagedColumns())) {
            RowGroupSurvivor survivor = pagedSurvivor(source);
            RowGroupFetcher fetcher = TestFetchers.learning(source, pagedSchema(), SegmentPool.getDefault(), learner);
            FetchPlan plan = fetcher.planFor(survivor, Optional.empty());

            try (RowGroupFetch _ = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                assertThat(plan.unmasked()).isTrue();
            }

            assertThat(learner.calls()).isZero();
        }
    }

    @Test
    void plainPagesLeaveThePrefixUnread() throws IOException {
        try (RecordingByteRangeSource source = new RecordingByteRangeSource(ByteRangeSource.ofFile(writeMixedFile()))) {
            RowGroupSurvivor survivor = mixedSurvivor(source, DICTIONARY_PAGES);
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), SegmentPool.getDefault());
            FetchPlan plan = fetcher.planFor(survivor, Optional.of(pagesOf(survivor, 7, MIXED_PAGES)));
            ChunkMeta meta = survivor.chunks().chunk(B).orElseThrow();
            source.reset();

            try (RowGroupFetch fetch = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                assertThat(columnOf(fetch, B).dictionaryPage()).isEmpty();
            }

            assertThat(source.sourceCalls()).isEqualTo(1);
            assertThat(source.readAnyByteIn(meta.chunkStart(), meta.dataPageOffset()))
                    .as("the bytes of the dictionary page")
                    .isFalse();
        }
    }

    // The pool gives back the buffer of an earlier fetch, and the room of the prefix still holds a dictionary page.
    @Test
    void stalePrefixIsNoDictionary() throws IOException {
        SegmentPool pool = SegmentPool.create();
        try (ByteRangeSource source = ByteRangeSource.ofFile(writeMixedFile())) {
            RowGroupSurvivor survivor = mixedSurvivor(source, DICTIONARY_PAGES);
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), pool);
            FetchPlan plan = fetcher.planFor(survivor, Optional.of(pagesOf(survivor, 7, MIXED_PAGES)));
            long staleBuffer = leaveAnEarlierFetchInThePool(pool, plan, source);

            try (RowGroupFetch fetch = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                FetchedColumnChunk column = columnOf(fetch, B);
                long firstRun = column.dataPageRuns().get(0).segment().address();
                assertThat(firstRun - staleBuffer)
                        .as("the fetch reuses the stale buffer")
                        .isBetween(0L, plan.requestedBytes());
                assertThat(column.dictionaryPage()).isEmpty();
            }
        } finally {
            pool.close();
        }
    }

    // The count stops at page 2, and page 4 uses the dictionary.
    @Test
    void wrongCountReadsThePrefix() throws IOException {
        try (RecordingByteRangeSource source = new RecordingByteRangeSource(ByteRangeSource.ofFile(writeMixedFile()))) {
            RowGroupSurvivor survivor = mixedSurvivor(source, 2);
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), SegmentPool.getDefault());
            FetchPlan plan = fetcher.planFor(survivor, Optional.of(pagesOf(survivor, 4, 5)));
            ChunkMeta meta = survivor.chunks().chunk(B).orElseThrow();
            DecodeBufferAllocator allocator = TestDecodeBuffers.ample();
            source.reset();

            try (RowGroupFetch fetch = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                DictionaryPage page = columnOf(fetch, B).dictionaryPage().orElseThrow();
                assertThat(page.decode(allocator).size()).isGreaterThanOrEqualTo(4);
            }

            assertThat(source.sourceCalls()).isEqualTo(2);
            long prefixLength = meta.dataPageOffset() - meta.chunkStart();
            assertThat(source.bytesReadIn(meta.chunkStart(), meta.dataPageOffset()))
                    .isEqualTo(prefixLength);
        }
    }

    @Test
    void prefixesShareTheSecondCall() throws IOException {
        try (RecordingByteRangeSource source = new RecordingByteRangeSource(ByteRangeSource.ofFile(writeMixedFile()))) {
            RowGroupSurvivor survivor = mixedSurvivor(source, 2);
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), SegmentPool.getDefault());
            FetchPlan plan = fetcher.planFor(survivor, Optional.of(pagesOf(survivor, 4, 5)));
            source.reset();

            try (RowGroupFetch fetch = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                assertThat(columnOf(fetch, B).dictionaryPage()).isPresent();
                assertThat(columnOf(fetch, C).dictionaryPage()).isPresent();
            }

            assertThat(source.sourceCalls()).isEqualTo(2);
        }
    }

    // Page 0 abuts the prefix. A read joining the two would fetch the prefix, and the check would read it again.
    @Test
    void abuttingPrefixStaysUnread() throws IOException {
        try (CountingByteRangeSource source =
                CountingByteRangeSource.unnamed(ByteRangeSource.ofFile(writeMixedFile()))) {
            RowGroupSurvivor survivor = mixedSurvivor(source, 0);
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), SegmentPool.getDefault());
            FetchPlan plan = fetcher.planFor(survivor, Optional.of(pagesOf(survivor, 0, 1)));
            source.reset();

            try (RowGroupFetch fetch = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                assertThat(columnOf(fetch, B).dictionaryPage()).isPresent();
            }

            assertThat(source.bytes()).isEqualTo(plan.requestedBytes());
            assertThat(source.requests()).isEqualTo(plan.units().size());
        }
    }

    @Test
    // S7466: the project style declares explicit local types rather than var
    @SuppressWarnings("java:S7466")
    void secondRequestCountsAsPages() throws IOException {
        FetchAccumulator accumulator = FetchAccumulator.active();
        try (CountingByteRangeSource source =
                CountingByteRangeSource.unnamed(ByteRangeSource.ofFile(writeMixedFile()))) {
            RowGroupSurvivor survivor = mixedSurvivor(source, 2);
            RowGroupFetcher fetcher =
                    TestFetchers.over(source, mixedSchema(), mixedSchema(), SegmentPool.getDefault(), accumulator);
            FetchPlan plan = fetcher.planFor(survivor, Optional.of(pagesOf(survivor, 4, 5)));
            source.reset();

            try (RowGroupFetch _ = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                FetchStats stats = accumulator.snapshot();
                assertThat(stats.pageBytes()).isEqualTo(source.bytes());
                assertThat(stats.requestCount()).isEqualTo(source.requests());
                assertThat(stats.fetchCount()).isEqualTo(2);
            }
        }
    }

    @Test
    // S7466: the project style declares explicit local types rather than var
    @SuppressWarnings("java:S7466")
    void unreadPrefixCountsNoBytes() throws IOException {
        FetchAccumulator accumulator = FetchAccumulator.active();
        try (CountingByteRangeSource source =
                CountingByteRangeSource.unnamed(ByteRangeSource.ofFile(writeMixedFile()))) {
            RowGroupSurvivor survivor = mixedSurvivor(source, DICTIONARY_PAGES);
            RowGroupFetcher fetcher =
                    TestFetchers.over(source, mixedSchema(), mixedSchema(), SegmentPool.getDefault(), accumulator);
            FetchPlan plan = fetcher.planFor(survivor, Optional.of(pagesOf(survivor, 7, MIXED_PAGES)));
            source.reset();

            try (RowGroupFetch _ = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                assertThat(accumulator.snapshot().pageBytes()).isEqualTo(source.bytes());
                assertThat(source.bytes()).isLessThan(plan.requestedBytes());
            }
        }
    }

    // The walk of the headers fails on the fetch thread, and the fetch gives back what it holds.
    @Test
    void unreadableHeaderFailsTheFetch() throws IOException {
        SegmentPool pool = SegmentPool.create();
        FetchBudget budget = FetchBudget.ofBytes(1L << 20);
        Path corrupt = withUnreadableHeaderOfPage(writeMixedFile(), B, 7);
        try (ByteRangeSource source = ByteRangeSource.ofFile(corrupt)) {
            RowGroupSurvivor survivor = mixedSurvivor(source, DICTIONARY_PAGES);
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), pool);
            FetchPlan plan = fetcher.planFor(survivor, Optional.of(pagesOf(survivor, 7, MIXED_PAGES)));
            long span = plan.spanBytes();
            assertThat(budget.tryReserve(span)).isTrue();
            BudgetReservation reservation = new BudgetReservation(budget, span);

            assertThatThrownBy(() -> fetcher.fetch(survivor, plan, reservation))
                    .isInstanceOf(ParquetFormatException.class);

            assertThat(pool.stats().outstandingBorrows()).isZero();
            assertThat(budget.available()).isEqualTo(budget.capacity());
        } finally {
            pool.close();
        }
    }

    @Test
    void failedSecondRequestCleansUp() throws IOException {
        SegmentPool pool = SegmentPool.create();
        FetchBudget budget = FetchBudget.ofBytes(1L << 20);
        try (ByteRangeSource source = new FailingSecondBatch(ByteRangeSource.ofFile(writeMixedFile()))) {
            RowGroupSurvivor survivor = mixedSurvivor(source, 2);
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), pool);
            FetchPlan plan = fetcher.planFor(survivor, Optional.of(pagesOf(survivor, 4, 5)));
            long span = plan.spanBytes();
            assertThat(budget.tryReserve(span)).isTrue();
            BudgetReservation reservation = new BudgetReservation(budget, span);

            assertThatThrownBy(() -> fetcher.fetch(survivor, plan, reservation))
                    .isInstanceOf(UncheckedIOException.class)
                    .hasMessageContaining(FailingSecondBatch.MESSAGE);

            assertThat(pool.stats().outstandingBorrows()).isZero();
            assertThat(budget.available()).isEqualTo(budget.capacity());
        } finally {
            pool.close();
        }
    }

    // A footer with overlapping chunks puts a prefix between two abutting runs, and one read would shift the second.
    @Test
    void roomOfAPrefixEndsARead() throws IOException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(writeMixedFile())) {
            RowGroupSurvivor survivor = mixedSurvivor(source, DICTIONARY_PAGES);
            RowGroupChunks chunks = survivor.chunks();
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), pagedSchema(), SegmentPool.getDefault());
            FetchUnit fifthPage = pageUnit(chunks, A, 5);
            FetchUnit sixthPage = pageUnit(chunks, A, 6);
            long insideTheFifthPage = fifthPage.fileOffset() + 1;
            FetchUnit prefix = onDemandPrefix(B, insideTheFifthPage, insideTheFifthPage + 2);
            FetchUnit plainPageOfB = pageUnit(chunks, B, 8);
            FetchPlan plan = new FetchPlan(List.of(fifthPage, prefix, sixthPage, plainPageOfB), false, Set.of());

            try (RowGroupFetch fetch = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                MemorySegment secondRun =
                        columnOf(fetch, A).dataPageRuns().get(1).segment();
                assertThat(secondRun.toArray(ValueLayout.JAVA_BYTE)).isEqualTo(bytesOf(source, sixthPage));
            }
        }
    }

    // A footer with overlapping chunks makes two prefixes abut, and one read of both would overwrite the run between.
    @Test
    void abuttingPrefixesReadApart() throws IOException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(writeMixedFile())) {
            RowGroupSurvivor survivor = mixedSurvivor(source, 2);
            RowGroupChunks chunks = survivor.chunks();
            ParquetSchema projected = WriteFixtures.schemaOf(
                    WriteFixtures.requiredLeaf("b", PrimitiveKind.INT64),
                    WriteFixtures.requiredLeaf("c", PrimitiveKind.INT64));
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), projected, SegmentPool.getDefault());
            ChunkMeta metaOfC = chunks.chunk(C).orElseThrow();
            long startOfB = chunks.chunk(B).orElseThrow().chunkStart();
            long startOfC = metaOfC.chunkStart();
            FetchUnit pageOfB = pageUnit(chunks, B, 4);
            FetchUnit pageOfC = pageUnit(chunks, C, 4);
            FetchUnit prefixOfB = onDemandPrefix(B, startOfB, startOfC);
            FetchUnit prefixOfC = onDemandPrefix(C, startOfC, metaOfC.dataPageOffset());
            FetchPlan plan = new FetchPlan(List.of(prefixOfB, pageOfB, prefixOfC, pageOfC), false, Set.of());

            try (RowGroupFetch fetch = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                MemorySegment runOfB = columnOf(fetch, B).dataPageRuns().get(0).segment();
                assertThat(runOfB.toArray(ValueLayout.JAVA_BYTE)).isEqualTo(bytesOf(source, pageOfB));
                assertThat(columnOf(fetch, B).dictionaryPage()).isPresent();
                assertThat(columnOf(fetch, C).dictionaryPage()).isPresent();
            }
        }
    }

    // The first fetch knows of no use. A reader then uses the dictionary of b, and the next fetch decodes b alone.
    @Test
    void usedColumnDecodesAhead() throws IOException {
        SegmentPool pool = SegmentPool.create();
        SegmentPool readerPool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample(readerPool);
        try (ByteRangeSource source = ByteRangeSource.ofFile(writeRowGroups())) {
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), pool);

            try (RowGroupFetch first = fetchWhole(fetcher, source, 0)) {
                assertThat(pool.stats().totalBorrows()).as("the fetch buffer").isEqualTo(1L);
                dictionaryOf(first, B).decode(allocator);
            }
            try (RowGroupFetch second = fetchWhole(fetcher, source, 1)) {
                assertThat(pool.stats().totalBorrows())
                        .as("the fetch buffers and one scratch")
                        .isEqualTo(3L);
                assertThat(pool.stats().outstandingBorrows())
                        .as("the fetch buffer")
                        .isEqualTo(1L);
                Dictionary<?> dictionary = dictionaryOf(second, B).decode(allocator);
                assertThat(dictionary.size()).isEqualTo(GROUP_DISTINCT_VALUES);
                assertThat(readerPool.stats().totalBorrows())
                        .as("the decode by the first reader")
                        .isEqualTo(1L);
            }
            assertThat(pool.stats().outstandingBorrows()).isZero();
        } finally {
            pool.close();
            readerPool.close();
        }
    }

    @Test
    void unusedColumnStaysLazy() throws IOException {
        SegmentPool readerPool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample(readerPool);
        try (ByteRangeSource source = ByteRangeSource.ofFile(writeRowGroups())) {
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), SegmentPool.getDefault());
            try (RowGroupFetch first = fetchWhole(fetcher, source, 0)) {
                dictionaryOf(first, B).decode(allocator);
            }

            try (RowGroupFetch second = fetchWhole(fetcher, source, 1)) {
                dictionaryOf(second, C).decode(allocator);
            }

            assertThat(readerPool.stats().totalBorrows())
                    .as("a decode by each reader")
                    .isEqualTo(2L);
        } finally {
            readerPool.close();
        }
    }

    // Two fetchers over one source stand for two reads.
    @Test
    void readsKeepTheirOwnColumns() throws IOException {
        SegmentPool pool = SegmentPool.getDefault();
        SegmentPool readerPool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample(readerPool);
        try (ByteRangeSource source = ByteRangeSource.ofFile(writeRowGroups())) {
            RowGroupFetcher firstRead = TestFetchers.over(source, mixedSchema(), mixedSchema(), pool);
            RowGroupFetcher secondRead = TestFetchers.over(source, mixedSchema(), mixedSchema(), pool);
            try (RowGroupFetch first = fetchWhole(firstRead, source, 0)) {
                dictionaryOf(first, B).decode(allocator);
            }

            try (RowGroupFetch second = fetchWhole(secondRead, source, 1)) {
                dictionaryOf(second, B).decode(allocator);
            }

            assertThat(readerPool.stats().totalBorrows())
                    .as("a decode by each reader")
                    .isEqualTo(2L);
        } finally {
            readerPool.close();
        }
    }

    // The fetch survives the corrupt page and gives its scratch back, and the reader of the page raises the failure.
    @Test
    void failedAheadFailsTheReader() throws IOException {
        SegmentPool pool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample();
        Path corrupt = withCorruptDictionary(writeRowGroups(), B, 1);
        try (ByteRangeSource source = ByteRangeSource.ofFile(corrupt)) {
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), pool);
            try (RowGroupFetch first = fetchWhole(fetcher, source, 0)) {
                dictionaryOf(first, B).decode(allocator);
            }

            try (RowGroupFetch second = fetchWhole(fetcher, source, 1)) {
                DictionaryPage page = dictionaryOf(second, B);
                assertThat(pool.stats().totalBorrows())
                        .as("the fetch buffers and one scratch")
                        .isEqualTo(3L);
                assertThat(pool.stats().outstandingBorrows())
                        .as("the fetch buffer")
                        .isEqualTo(1L);
                assertThatThrownBy(() -> page.decode(allocator))
                        .isInstanceOf(MalformedFileException.class)
                        .hasMessageContaining("Column b dictionary page cannot be decompressed");
            }

            assertThat(pool.stats().outstandingBorrows()).isZero();
        } finally {
            pool.close();
        }
    }

    // A footer can give a column a negative width, and the decoder of the page rejects it with a plain runtime failure.
    @Test
    void anyFailureStaysOutOfTheFetch() {
        SegmentPool pool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample(pool);
        MemorySegment payload = MemorySegment.ofArray(new byte[Long.BYTES]).asReadOnly();
        DictionaryPage page = new DictionaryPage(
                payload,
                Long.BYTES,
                1,
                Compression.uncompressed(),
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                OptionalInt.of(-1),
                A,
                ConcurrentHashMap.newKeySet());
        try {
            DictionaryPage afterTheFetch = page.decodeAhead(pool);

            assertThat(afterTheFetch).isSameAs(page);
            assertThat(pool.stats().outstandingBorrows()).isZero();
            assertThatThrownBy(() -> afterTheFetch.decode(allocator))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("length must be non-negative");
        } finally {
            pool.close();
        }
    }

    static Stream<Arguments> fetchedPrefixes() {
        // each vector is the count of dictionary-encoded pages in the footer, and the pages kept by the mask
        return Stream.of(Arguments.of(DICTIONARY_PAGES, 0, 2), Arguments.of(2, 4, 5));
    }

    // The first vector reads the prefixes with the pages, and the second reads them on demand.
    @ParameterizedTest
    @MethodSource("fetchedPrefixes")
    void fetchedPrefixDecodesAhead(int count, int firstPage, int endPage) throws IOException {
        SegmentPool pool = SegmentPool.create();
        SegmentPool readerPool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample(readerPool);
        try (ByteRangeSource source = ByteRangeSource.ofFile(writeMixedFile())) {
            RowGroupSurvivor survivor = mixedSurvivor(source, count);
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), pool);
            FetchPlan plan = fetcher.planFor(survivor, Optional.of(pagesOf(survivor, firstPage, endPage)));

            try (RowGroupFetch fetch = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                assertThat(pool.stats().totalBorrows())
                        .as("the fetch buffer and two scratches")
                        .isEqualTo(3L);
                assertThat(pool.stats().outstandingBorrows())
                        .as("the fetch buffer")
                        .isEqualTo(1L);
                dictionaryOf(fetch, B).decode(allocator);
                assertThat(readerPool.stats().totalBorrows())
                        .as("no decode by a reader")
                        .isZero();
            }
            assertThat(pool.stats().outstandingBorrows()).isZero();
        } finally {
            pool.close();
            readerPool.close();
        }
    }

    // A fetched page using the prefix counts as a use of its column, and the next whole chunks decode ahead.
    @Test
    void fetchedPrefixMarksItsColumn() throws IOException {
        SegmentPool pool = SegmentPool.create();
        try (ByteRangeSource source = ByteRangeSource.ofFile(writeRowGroups())) {
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), pool);
            try (RowGroupFetch _ = fetchFirstPage(fetcher, source, 0)) {
                assertThat(pool.stats().totalBorrows())
                        .as("the fetch buffer and two scratches")
                        .isEqualTo(3L);
            }

            try (RowGroupFetch _ = fetchWhole(fetcher, source, 1)) {
                assertThat(pool.stats().totalBorrows())
                        .as("the fetch buffers and four scratches")
                        .isEqualTo(6L);
            }

            assertThat(pool.stats().outstandingBorrows()).isZero();
        } finally {
            pool.close();
        }
    }

    // No count rules the prefix out, and the pages after the fallback to PLAIN leave it undecoded.
    @Test
    void plainPagesKeepPrefixLazy() throws IOException {
        SegmentPool pool = SegmentPool.create();
        try (ByteRangeSource source = ByteRangeSource.ofFile(writeMixedFile())) {
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), pool);

            try (RowGroupFetch fetch = fetchPages(fetcher, source, 7, MIXED_PAGES)) {
                assertThat(columnOf(fetch, B).dictionaryPage()).isPresent();
                assertThat(pool.stats().totalBorrows()).as("the fetch buffer").isEqualTo(1L);
            }

            assertThat(pool.stats().outstandingBorrows()).isZero();
        } finally {
            pool.close();
        }
    }

    // The column stays unmarked, and a whole chunk fetched after the plain pages stays lazy.
    @Test
    void plainPagesMarkNoColumn() throws IOException {
        SegmentPool pool = SegmentPool.create();
        try (ByteRangeSource source = ByteRangeSource.ofFile(writeMixedFile())) {
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), pool);
            try (RowGroupFetch _ = fetchPages(fetcher, source, 7, MIXED_PAGES)) {
                assertThat(pool.stats().totalBorrows()).as("the fetch buffer").isEqualTo(1L);
            }

            try (RowGroupFetch _ = fetchWhole(fetcher, source, 0)) {
                assertThat(pool.stats().totalBorrows()).as("the fetch buffers").isEqualTo(2L);
            }

            assertThat(pool.stats().outstandingBorrows()).isZero();
        } finally {
            pool.close();
        }
    }

    // The read has used the dictionary of the column, and a fetch of plain pages still has no use for it.
    @Test
    void usedColumnKeepsPrefixLazy() throws IOException {
        SegmentPool pool = SegmentPool.create();
        try (ByteRangeSource source = ByteRangeSource.ofFile(writeMixedFile())) {
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), pool);
            try (RowGroupFetch _ = fetchPages(fetcher, source, 0, 2)) {
                assertThat(pool.stats().totalBorrows())
                        .as("the fetch buffer and two scratches")
                        .isEqualTo(3L);
            }

            try (RowGroupFetch _ = fetchPages(fetcher, source, 7, MIXED_PAGES)) {
                assertThat(pool.stats().totalBorrows())
                        .as("one more fetch buffer")
                        .isEqualTo(4L);
            }

            assertThat(pool.stats().outstandingBorrows()).isZero();
        } finally {
            pool.close();
        }
    }

    // The fetch survives the unreadable header and leaves the prefix undecoded.
    @Test
    void unreadableHeaderKeepsPrefixLazy() throws IOException {
        SegmentPool pool = SegmentPool.create();
        Path corrupt = withUnreadableHeaderOfPage(writeMixedFile(), B, 7);
        try (ByteRangeSource source = ByteRangeSource.ofFile(corrupt)) {
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), pool);

            try (RowGroupFetch fetch = fetchPages(fetcher, source, 7, MIXED_PAGES)) {
                assertThat(columnOf(fetch, B).dictionaryPage()).isPresent();
                assertThat(pool.stats().totalBorrows()).as("the fetch buffer").isEqualTo(1L);
            }

            assertThat(pool.stats().outstandingBorrows()).isZero();
        } finally {
            pool.close();
        }
    }

    // A mask keeping each page of a column fetches its whole chunk, and no prefix stands apart.
    @Test
    void maskKeepingEachPageStaysLazy() throws IOException {
        SegmentPool pool = SegmentPool.create();
        try (ByteRangeSource source = ByteRangeSource.ofFile(writeMixedFile())) {
            RowGroupSurvivor survivor = mixedSurvivor(source, DICTIONARY_PAGES);
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), pool);
            FetchPlan plan = fetcher.planFor(survivor, Optional.of(pagesOf(survivor, 0, MIXED_PAGES)));

            try (RowGroupFetch fetch = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                assertThat(columnOf(fetch, B).dictionaryPage()).isPresent();
                assertThat(pool.stats().totalBorrows()).as("the fetch buffer").isEqualTo(1L);
            }

            assertThat(pool.stats().outstandingBorrows()).isZero();
        } finally {
            pool.close();
        }
    }

    // The fetch survives the corrupt prefix and gives its scratch back, and the reader of the page raises the failure.
    @Test
    void corruptPrefixFailsTheReader() throws IOException {
        SegmentPool pool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample();
        Path corrupt = withCorruptDictionary(writeRowGroups(), B, 0);
        try (ByteRangeSource source = ByteRangeSource.ofFile(corrupt)) {
            RowGroupFetcher fetcher = TestFetchers.over(source, mixedSchema(), mixedSchema(), pool);

            try (RowGroupFetch fetch = fetchFirstPage(fetcher, source, 0)) {
                DictionaryPage page = dictionaryOf(fetch, B);
                assertThat(pool.stats().totalBorrows())
                        .as("the fetch buffer and two scratches")
                        .isEqualTo(3L);
                assertThat(pool.stats().outstandingBorrows())
                        .as("the fetch buffer")
                        .isEqualTo(1L);
                assertThatThrownBy(() -> page.decode(allocator))
                        .isInstanceOf(MalformedFileException.class)
                        .hasMessageContaining("Column b dictionary page cannot be decompressed");
            }

            assertThat(pool.stats().outstandingBorrows()).isZero();
        } finally {
            pool.close();
        }
    }

    /** One row group of {@value #PAGED_ROWS} rows over two long columns, in pages of {@value #PAGE_VALUES} values. */
    private Path writePagedColumns() throws IOException {
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(PAGE_VALUES)
                .build();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(PAGED_ROWS);
        for (long row = 0; row < PAGED_ROWS; row++) {
            rows.add(Map.of(A, row, B, 2 * row));
        }
        return WriteFixtures.writeRows(tempDir.resolve("paged.parquet"), pagedSchema(), options, rows);
    }

    private static ParquetSchema pagedSchema() {
        return WriteFixtures.schemaOf(
                WriteFixtures.requiredLeaf("a", PrimitiveKind.INT64),
                WriteFixtures.requiredLeaf("b", PrimitiveKind.INT64));
    }

    private static RowGroupSurvivor pagedSurvivor(ByteRangeSource source) {
        return survivorOf(source, 0);
    }

    private static RowGroupSurvivor survivorOf(ByteRangeSource source, int rowGroup) {
        FileMetaData footer = ParquetFormat.readFooter(source);
        ParquetSchema schema = SchemaBuilder.build(footer.schema());
        return RowGroupSurvivor.full(
                TestRowGroupChunks.of(footer, rowGroup, schema, TestRowGroupChunks.reading(source)));
    }

    /** A mask keeping the rows of the first page of both columns. */
    private static RowMask firstPageOf(RowGroupSurvivor survivor) {
        RowGroupChunks chunks = survivor.chunks();
        PageIndex pagesOfA = chunks.pageLocations(A).orElseThrow();
        PageIndex pagesOfB = chunks.pageLocations(B).orElseThrow();
        RowRanges firstPage = new RowRanges(List.of(new Range(0, PAGE_VALUES - 1L)));
        return new RowMask(firstPage, Map.of(A, pagesOfA, B, pagesOfB));
    }

    private Path writeMixedFile() throws IOException {
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(MIXED_PAGE_VALUES)
                .dictionaryByteLimit(DICTIONARY_BYTE_LIMIT)
                .defaultCompression(Compression.uncompressed())
                .encodingPolicy(A.dot(), EncodingPolicy.FORCE_PLAIN)
                .build();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(MIXED_ROWS);
        for (long row = 0; row < MIXED_ROWS; row++) {
            long repeatedThenNew = row < DICTIONARY_ROWS ? row % 4 : row;
            rows.add(Map.of(A, row, B, repeatedThenNew, C, repeatedThenNew));
        }
        return WriteFixtures.writeRows(tempDir.resolve("mixed.parquet"), mixedSchema(), options, rows);
    }

    private static ParquetSchema mixedSchema() {
        return WriteFixtures.schemaOf(
                WriteFixtures.requiredLeaf("a", PrimitiveKind.INT64),
                WriteFixtures.requiredLeaf("b", PrimitiveKind.INT64),
                WriteFixtures.requiredLeaf("c", PrimitiveKind.INT64));
    }

    /** The row group of a mixed file, under a footer counting {@code count} dictionary-encoded pages per column. */
    private static RowGroupSurvivor mixedSurvivor(ByteRangeSource source, int count) {
        List<EncodingStats> stats = List.of(
                new EncodingStats(PageType.DICTIONARY_PAGE, Encoding.PLAIN, 1),
                new EncodingStats(PageType.DATA_PAGE_V2, Encoding.RLE_DICTIONARY, count),
                new EncodingStats(PageType.DATA_PAGE_V2, Encoding.PLAIN, MIXED_PAGES - count));
        FileMetaData written = ParquetFormat.readFooter(source);
        FileMetaData countingB = FooterRewrite.encodingStats(B, 0, stats).apply(written);
        FileMetaData footer = FooterRewrite.encodingStats(C, 0, stats).apply(countingB);
        return RowGroupSurvivor.full(
                TestRowGroupChunks.of(footer, 0, mixedSchema(), TestRowGroupChunks.reading(source)));
    }

    /** A mask keeping the rows of the pages from {@code firstPage} to {@code endPage}, exclusive, of each column. */
    private static RowMask pagesOf(RowGroupSurvivor survivor, int firstPage, int endPage) {
        long firstRow = (long) firstPage * MIXED_PAGE_VALUES;
        long lastRow = (long) endPage * MIXED_PAGE_VALUES - 1;
        return rowsOf(survivor, firstRow, lastRow);
    }

    /** A mask keeping the rows from {@code firstRow} to {@code lastRow}, inclusive, of each column. */
    private static RowMask rowsOf(RowGroupSurvivor survivor, long firstRow, long lastRow) {
        Map<ColumnPath, PageIndex> pages = new HashMap<>();
        for (ColumnPath column : List.of(A, B, C)) {
            pages.put(column, survivor.chunks().pageLocations(column).orElseThrow());
        }
        return new RowMask(new RowRanges(List.of(new Range(firstRow, lastRow))), pages);
    }

    private Path writeRowGroups() throws IOException {
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(GROUP_ROWS))
                .pageValueLimit(GROUP_PAGE_VALUES)
                .encodingPolicy(A.dot(), EncodingPolicy.FORCE_PLAIN)
                .build();
        int rowCount = GROUPS * GROUP_ROWS;
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(rowCount);
        for (long row = 0; row < rowCount; row++) {
            long repeated = row % GROUP_DISTINCT_VALUES;
            rows.add(Map.of(A, row, B, repeated, C, repeated));
        }
        return WriteFixtures.writeRows(tempDir.resolve("row-groups.parquet"), mixedSchema(), options, rows);
    }

    private static RowGroupFetch fetchWhole(RowGroupFetcher fetcher, ByteRangeSource source, int rowGroup) {
        RowGroupSurvivor survivor = survivorOf(source, rowGroup);
        FetchPlan plan = fetcher.planFor(survivor, Optional.empty());
        return fetcher.fetch(survivor, plan, BudgetReservation.NONE);
    }

    private static RowGroupFetch fetchFirstPage(RowGroupFetcher fetcher, ByteRangeSource source, int rowGroup) {
        RowGroupSurvivor survivor = survivorOf(source, rowGroup);
        RowMask firstPage = rowsOf(survivor, 0, GROUP_PAGE_VALUES - 1L);
        FetchPlan plan = fetcher.planFor(survivor, Optional.of(firstPage));
        return fetcher.fetch(survivor, plan, BudgetReservation.NONE);
    }

    /** Fetches the pages from {@code firstPage} to {@code endPage}, exclusive, of a mixed file with no count. */
    private static RowGroupFetch fetchPages(
            RowGroupFetcher fetcher, ByteRangeSource source, int firstPage, int endPage) {
        RowGroupSurvivor survivor = survivorOf(source, 0);
        FetchPlan plan = fetcher.planFor(survivor, Optional.of(pagesOf(survivor, firstPage, endPage)));
        return fetcher.fetch(survivor, plan, BudgetReservation.NONE);
    }

    private static DictionaryPage dictionaryOf(RowGroupFetch fetch, ColumnPath path) {
        return columnOf(fetch, path).dictionaryPage().orElseThrow();
    }

    private static FetchedColumnChunk columnOf(RowGroupFetch fetch, ColumnPath path) {
        return fetch.columns().stream()
                .filter(column -> column.path().equals(path))
                .findFirst()
                .orElseThrow();
    }

    /**
     * Returns to {@code pool} a buffer holding each unit of {@code plan} at its place in the fetch buffer, as left by
     * an earlier fetch reading them all. The result is the address of that buffer.
     */
    private static long leaveAnEarlierFetchInThePool(SegmentPool pool, FetchPlan plan, ByteRangeSource source) {
        try (Pooled buffer = pool.borrow(plan.requestedBytes())) {
            long position = 0;
            for (FetchUnit unit : plan.units()) {
                source.readFully(unit.fileOffset(), buffer.segment().asSlice(position, unit.length()));
                position += unit.length();
            }
            return buffer.segment().address();
        }
    }

    /** A copy of {@code file} with the first bytes of page {@code page} of {@code column} overwritten. */
    private Path withUnreadableHeaderOfPage(Path file, ColumnPath column, int page) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            RowGroupChunks chunks = pagedSurvivor(source).chunks();
            PageLocation location =
                    chunks.pageLocations(column).orElseThrow().locations().get(page);
            int start = Math.toIntExact(location.offset());
            Arrays.fill(bytes, start, start + Long.BYTES, (byte) 0xFF);
        }
        return Files.write(tempDir.resolve("unreadable-header.parquet"), bytes);
    }

    /** A copy of {@code file} with the head of a dictionary page payload overwritten, in {@code rowGroup}. */
    private Path withCorruptDictionary(Path file, ColumnPath column, int rowGroup) throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ChunkMeta meta = survivorOf(source, rowGroup).chunks().chunk(column).orElseThrow();
            int pageStart = Math.toIntExact(meta.chunkStart());
            ByteArrayInputStream page = new ByteArrayInputStream(bytes, pageStart, bytes.length - pageStart);
            ParquetFormat.readPageHeader(page);
            int payload = bytes.length - page.available();
            Arrays.fill(bytes, payload, payload + Long.BYTES, (byte) 0xFF);
        }
        return Files.write(tempDir.resolve("corrupt-dictionary.parquet"), bytes);
    }

    /** A unit of a plan built by hand, reading page {@code page} of {@code column}. */
    private static FetchUnit pageUnit(RowGroupChunks chunks, ColumnPath column, int page) {
        PageLocation location =
                chunks.pageLocations(column).orElseThrow().locations().get(page);
        return new FetchUnit(column, location.offset(), location.compressedPageSize(), page, false, false);
    }

    /** A unit of a plan built by hand, holding the file bytes from {@code start} up to {@code end} as a prefix. */
    private static FetchUnit onDemandPrefix(ColumnPath column, long start, long end) {
        return new FetchUnit(column, start, Math.toIntExact(end - start), 0, true, true);
    }

    private static byte[] bytesOf(ByteRangeSource source, ChunkMeta meta) {
        return bytesAt(source, meta.chunkStart(), meta.totalCompressedSize());
    }

    private static byte[] bytesOf(ByteRangeSource source, FetchUnit unit) {
        return bytesAt(source, unit.fileOffset(), unit.length());
    }

    private static byte[] bytesAt(ByteRangeSource source, long offset, int length) {
        MemorySegment bytes = MemorySegment.ofArray(new byte[length]);
        source.readFully(offset, bytes);
        return bytes.toArray(ValueLayout.JAVA_BYTE);
    }

    /** Fails its second batch of ranges, the read of a dictionary prefix after the read of the pages. */
    private static final class FailingSecondBatch implements ByteRangeSource {

        static final String MESSAGE = "the second batch failed";

        private final ByteRangeSource delegate;
        private int batches;

        FailingSecondBatch(ByteRangeSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public BatchReadResult readFully(List<RangeRequest> requests) {
            batches++;
            if (batches == 2) {
                throw new UncheckedIOException(new IOException(MESSAGE));
            }
            return delegate.readFully(requests);
        }

        @Override
        public int read(long offset, MemorySegment dst) {
            return delegate.read(offset, dst);
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    /** A learner allowed one row group. It keeps a copy of each chunk handed to it, made while the fetch holds it. */
    private static final class RecordingLearner implements PageIndexLearner {

        private final int allowedRowGroup;
        private final List<WholeChunk> chunks = new ArrayList<>();
        private int calls;

        RecordingLearner(int allowedRowGroup) {
            this.allowedRowGroup = allowedRowGroup;
        }

        @Override
        public boolean learns(int rowGroup) {
            return rowGroup == allowedRowGroup;
        }

        @Override
        public void learn(RowGroupSurvivor survivor, List<WholeChunk> handed) {
            calls++;
            for (WholeChunk chunk : handed) {
                MemorySegment copy = MemorySegment.ofArray(chunk.bytes().toArray(ValueLayout.JAVA_BYTE));
                chunks.add(new WholeChunk(chunk.path(), chunk.meta(), copy));
            }
        }

        int calls() {
            return calls;
        }

        List<WholeChunk> chunks() {
            return chunks;
        }
    }
}
