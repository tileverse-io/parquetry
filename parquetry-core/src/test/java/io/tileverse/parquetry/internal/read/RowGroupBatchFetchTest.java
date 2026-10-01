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

import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeRequest;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.EncodingPolicy;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.observe.FetchStats;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * How a row group reaches its byte source: the whole plan in one call, asking for its bytes and no others, what that
 * call cost reported back, and the several calls issued for a plan too wide for one of them.
 */
class RowGroupBatchFetchTest {

    private static final ColumnPath A = ColumnPath.of("a");
    private static final ColumnPath B = ColumnPath.of("b");
    private static final ColumnPath C = ColumnPath.of("c");

    private static final long REPORTED_FETCHES = 2;
    private static final long REPORTED_TRANSFERRED = 9_999;
    private static final long REPORTED_FROM_CACHE = 111;

    private static final int ROW_COUNT = 256;

    /** The largest number of ranges accepted by one call, mirroring the cap enforced by the fetch. */
    private static final int RANGES_PER_CALL = 256;

    /** Rows per page of the wide fixture, small enough to leave every column with hundreds of pages. */
    private static final int WIDE_PAGE_VALUE_LIMIT = 10;

    /** Rows of the wide fixture: 300 pages per column, of which a mask keeps every other one. */
    private static final int WIDE_ROW_COUNT = 3_000;

    @TempDir
    Path tempDir;

    @Test
    void aWholeRowGroupIsAskedForInOneCall() throws Exception {
        Path file = writeThreeColumns();
        ParquetSchema schema = threeIntColumns();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            CountingBatchSource counting = new CountingBatchSource(source);
            RowGroupSurvivor survivor = survivorFor(source, schema);
            RowGroupFetcher fetcher =
                    TestFetchers.over(counting, schema, schema, SegmentPool.getDefault(), FetchAccumulator.NONE);
            FetchPlan plan = fetcher.planFor(survivor, Optional.empty());

            try (RowGroupFetch fetch = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                assertThat(fetch.columns()).hasSize(3);
            }

            assertThat(counting.calls())
                    .as("one batch call takes the whole row group")
                    .isEqualTo(1);
            assertThat(counting.lengths())
                    .as("three abutting chunks merge into one request for exactly the bytes of the plan")
                    .containsExactly(Long.valueOf(plan.requestedBytes()));
        }
    }

    @Test
    void theTransportCostOfEachCallReachesTheFetchStats() throws Exception {
        Path file = writeThreeColumns();
        ParquetSchema schema = threeIntColumns();
        FetchAccumulator accumulator = FetchAccumulator.active();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            RowGroupSurvivor survivor = survivorFor(source, schema);
            RowGroupFetcher fetcher = TestFetchers.over(
                    new ReportingBatchSource(source), schema, schema, SegmentPool.getDefault(), accumulator);
            FetchPlan plan = fetcher.planFor(survivor, Optional.empty());

            try (RowGroupFetch _ = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                FetchStats stats = accumulator.snapshot();
                assertThat(stats.pageBytes()).isEqualTo(plan.requestedBytes());
                assertThat(stats.requestCount())
                        .as("one range asked for, the three abutting chunks merged")
                        .isEqualTo(1);
                assertThat(stats.backendFetches()).isEqualTo(REPORTED_FETCHES);
                assertThat(stats.bytesTransferred()).isEqualTo(REPORTED_TRANSFERRED);
                assertThat(stats.bytesFromCache()).isEqualTo(REPORTED_FROM_CACHE);
            }
        }
    }

    @Test
    void aPlanTooWideForOneCallGoesOutAsSeveral() throws Exception {
        Path file = writeThreeColumnsOfManyPages();
        ParquetSchema schema = threeIntColumns();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            CountingBatchSource counting = new CountingBatchSource(source);
            RowGroupSurvivor survivor = survivorReadingIndexSections(source, schema);
            RowGroupFetcher fetcher =
                    TestFetchers.over(counting, schema, schema, SegmentPool.getDefault(), FetchAccumulator.NONE);
            FetchPlan plan = fetcher.planFor(survivor, Optional.of(everyOtherPageOf(survivor)));
            assertThat(plan.units())
                    .as("the fixture must plan more ranges than one call takes")
                    .hasSizeGreaterThan(RANGES_PER_CALL);

            try (RowGroupFetch fetch = fetcher.fetch(survivor, plan, BudgetReservation.NONE)) {
                assertThat(fetch.columns()).hasSize(3);
            }

            assertThat(counting.rangesPerCall())
                    .as("a plan beyond the cap goes out as several calls, none of them over it")
                    .hasSizeGreaterThan(1)
                    .allSatisfy(ranges -> assertThat(ranges).isLessThanOrEqualTo(RANGES_PER_CALL));
            assertThat(counting.rangesPerCall().getFirst())
                    .as("a call is filled to the cap before the next one starts")
                    .isEqualTo(RANGES_PER_CALL);
            assertThat(totalOf(counting.lengths()))
                    .as("the calls together ask for the plan's bytes, none left out and none asked for twice")
                    .isEqualTo(plan.requestedBytes());
        }
    }

    private static long totalOf(List<Long> lengths) {
        long total = 0;
        for (Long length : lengths) {
            total += length.longValue();
        }
        return total;
    }

    /** Reads through to the file and then reports a known transport cost, standing in for a merging backend. */
    private static final class ReportingBatchSource implements ByteRangeSource {

        private final ByteRangeSource delegate;

        ReportingBatchSource(ByteRangeSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public int read(long offset, MemorySegment dst) {
            return delegate.read(offset, dst);
        }

        @Override
        public BatchReadResult readFully(List<RangeRequest> batch) {
            BatchReadResult below = delegate.readFully(batch);
            int[] counts = new int[below.requests()];
            for (int index = 0; index < counts.length; index++) {
                counts[index] = below.bytesRead(index);
            }
            return BatchReadResult.of(batch, counts, REPORTED_FETCHES, REPORTED_TRANSFERRED, REPORTED_FROM_CACHE);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    /** Records each call to the batch verb and the length of every range in it, delegating the work to the file. */
    private static final class CountingBatchSource implements ByteRangeSource {

        private final ByteRangeSource delegate;
        private final AtomicInteger calls = new AtomicInteger();
        private final List<Long> lengths = new ArrayList<>();
        private final List<Integer> rangesPerCall = new ArrayList<>();

        CountingBatchSource(ByteRangeSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public int read(long offset, MemorySegment dst) {
            return delegate.read(offset, dst);
        }

        @Override
        public BatchReadResult readFully(List<RangeRequest> batch) {
            calls.incrementAndGet();
            rangesPerCall.add(Integer.valueOf(batch.size()));
            for (RangeRequest request : batch) {
                lengths.add(Long.valueOf(request.range().length()));
            }
            return delegate.readFully(batch);
        }

        @Override
        public void close() {
            delegate.close();
        }

        int calls() {
            return calls.get();
        }

        List<Long> lengths() {
            return List.copyOf(lengths);
        }

        /** How many ranges each call asked for, in call order. */
        List<Integer> rangesPerCall() {
            return List.copyOf(rangesPerCall);
        }
    }

    // -------------------------------------------------------------------------
    // Fixture
    // -------------------------------------------------------------------------

    private static RowGroupSurvivor survivorFor(ByteRangeSource source, ParquetSchema schema) {
        FileMetaData footer = ParquetFormat.readFooter(source);
        RowGroupChunks chunks = TestRowGroupChunks.of(footer, 0, schema, indexLoaderNotUsed());
        return RowGroupSurvivor.full(chunks);
    }

    private static RowGroupSurvivor survivorReadingIndexSections(ByteRangeSource source, ParquetSchema schema) {
        FileMetaData footer = ParquetFormat.readFooter(source);
        RowGroupChunks chunks = TestRowGroupChunks.of(footer, 0, schema, TestRowGroupChunks.reading(source));
        return RowGroupSurvivor.full(chunks);
    }

    /** A mask keeping every other page of every column, which leaves a skipped page between two surviving runs. */
    private static RowMask everyOtherPageOf(RowGroupSurvivor survivor) {
        Map<ColumnPath, OffsetIndex> offsetIndexes = new HashMap<>();
        for (ColumnPath path : List.of(A, B, C)) {
            offsetIndexes.put(path, survivor.chunks().offsetIndex(path).orElseThrow());
        }
        return new RowMask(rowsOfEveryOtherPage(), offsetIndexes);
    }

    /** The rows of the even-numbered pages: page {@code p} holds the {@value #WIDE_PAGE_VALUE_LIMIT} rows after it. */
    private static RowRanges rowsOfEveryOtherPage() {
        List<Range> ranges = new ArrayList<>();
        for (int page = 0; page * WIDE_PAGE_VALUE_LIMIT < WIDE_ROW_COUNT; page += 2) {
            long firstRow = (long) page * WIDE_PAGE_VALUE_LIMIT;
            ranges.add(new Range(firstRow, firstRow + WIDE_PAGE_VALUE_LIMIT - 1));
        }
        return new RowRanges(ranges);
    }

    private static IndexSectionLoader indexLoaderNotUsed() {
        return new IndexSectionLoader() {
            @Override
            public OffsetIndex readOffsetIndex(long offset, int length) {
                throw new UnsupportedOperationException("index sections are not read in this test");
            }

            @Override
            public ColumnIndex readColumnIndex(long offset, int length) {
                throw new UnsupportedOperationException("index sections are not read in this test");
            }

            @Override
            public SplitBlockBloomFilter readBloom(long offset, int length) {
                throw new UnsupportedOperationException("index sections are not read in this test");
            }
        };
    }

    private Path writeThreeColumns() throws Exception {
        Path file = tempDir.resolve("batch-fetch.parquet");
        ParquetSchema schema = threeIntColumns();
        WriteOptions options = WriteOptions.builder().tempDir(tempDir).build();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(ROW_COUNT);
        for (int value = 0; value < ROW_COUNT; value++) {
            rows.add(Map.of(A, value, B, value * 2, C, value * 3));
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }
        return file;
    }

    /**
     * Writes one row group of {@value #WIDE_ROW_COUNT} rows over three int columns, {@value #WIDE_PAGE_VALUE_LIMIT}
     * rows per page, which leaves each column with hundreds of pages. Every column is pinned to {@code PLAIN} and left
     * uncompressed, hence a page-narrowed plan holds one unit per surviving run and no dictionary prefix.
     */
    private Path writeThreeColumnsOfManyPages() throws Exception {
        Path file = tempDir.resolve("batch-fetch-many-pages.parquet");
        ParquetSchema schema = threeIntColumns();
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(WIDE_PAGE_VALUE_LIMIT)
                .defaultCompression(Compression.uncompressed())
                .encodingPolicy(A.dot(), EncodingPolicy.FORCE_PLAIN)
                .encodingPolicy(B.dot(), EncodingPolicy.FORCE_PLAIN)
                .encodingPolicy(C.dot(), EncodingPolicy.FORCE_PLAIN)
                .build();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(WIDE_ROW_COUNT);
        for (int value = 0; value < WIDE_ROW_COUNT; value++) {
            rows.add(Map.of(A, value, B, value * 2, C, value * 3));
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }
        return file;
    }

    private static ParquetSchema threeIntColumns() {
        List<SchemaNode> leaves = List.of(requiredInt(A.name()), requiredInt(B.name()), requiredInt(C.name()));
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, leaves, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }
}
