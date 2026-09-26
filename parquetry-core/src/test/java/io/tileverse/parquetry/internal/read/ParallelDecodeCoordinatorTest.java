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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.observe.SpillAccumulator;
import io.tileverse.parquetry.runtime.ComputeExecutor;
import io.tileverse.parquetry.runtime.DecodeBudget;
import io.tileverse.parquetry.runtime.DiskBudget;
import io.tileverse.parquetry.runtime.FetchBudget;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaBuilder;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.RecordingByteRangeSource;

/**
 * Drives {@link ParallelDecodeCoordinator} directly over a real multi-row-group fixture to prove file-order delivery,
 * deadlock freedom under a budget too small for any batch, the inline serial path, producer-failure propagation, and
 * how a spatial plan keeps a dropped row group and the skipped pages of a narrowed one off the wire.
 */
class ParallelDecodeCoordinatorTest {

    private static final int DEFAULT_PREFETCH_DEPTH = 2;
    private static final int NO_COALESCE_GAP = 0;
    private static final int MAX_SPAN = 8 << 20;

    private static final ColumnPath PAGED_COLUMN = ColumnPath.of("id");
    private static final int PAGED_ROWS_PER_GROUP = 16;
    private static final int PAGED_ROWS_PER_PAGE = 4;
    private static final int PAGED_GROUPS = 2;

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void emitsAllRowGroupsInFileOrder(@TempDir Path tmp) throws Exception {
        Fixture fixture = Fixture.write(tmp, 4_000);
        assertThat(fixture.rowGroupCount)
                .as("the fixture spans several row groups")
                .isGreaterThanOrEqualTo(3);

        ComputeExecutor pool = ComputeExecutor.ofParallelism(4);
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixture.file)) {
            ParallelDecodeCoordinator coordinator =
                    fixture.coordinator(source, pool, DecodeBudget.defaultBudget(), /*decodeAhead*/ 2);
            try (coordinator) {
                List<Long> perRowGroup = drainPerRowGroupCounts(coordinator);
                assertThat(perRowGroup.stream().mapToLong(Long::longValue).sum())
                        .as("every fixture row is delivered")
                        .isEqualTo(4_000L);
                assertThat(perRowGroup)
                        .as("one entry per row group, in file order")
                        .hasSize(fixture.rowGroupCount)
                        .containsExactlyElementsOf(fixture.rowsPerRowGroup);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void completesUnderATinyDecodeBudget(@TempDir Path tmp) throws Exception {
        Fixture fixture = Fixture.write(tmp, 4_000);

        ComputeExecutor pool = ComputeExecutor.ofParallelism(4);
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixture.file)) {
            ParallelDecodeCoordinator coordinator =
                    fixture.coordinator(source, pool, DecodeBudget.ofBytes(1), /*decodeAhead*/ 2);
            try (coordinator) {
                long total = drainTotalRows(coordinator);
                assertThat(total)
                        .as("in-order consume completes under a budget too small for any batch")
                        .isEqualTo(4_000L);
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void serialModeDecodesInlineWithNoSpeculation(@TempDir Path tmp) throws Exception {
        Fixture fixture = Fixture.write(tmp, 4_000);

        try (ByteRangeSource source = ByteRangeSource.ofFile(fixture.file)) {
            ParallelDecodeCoordinator coordinator = fixture.coordinator(
                    source, ComputeExecutor.shared(), DecodeBudget.defaultBudget(), /*decodeAhead*/ 0);
            try (coordinator) {
                long total = drainTotalRows(coordinator);
                assertThat(total)
                        .as("serial mode (no decode-ahead) delivers every row inline")
                        .isEqualTo(4_000L);
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void decodeFailurePropagatesToConsumer() {
        ComputeExecutor pool = ComputeExecutor.ofParallelism(1);
        try (DecodedRowGroup rowGroup = failingStreamingRowGroup(pool)) {
            assertThatThrownBy(() -> drainRowGroup(rowGroup))
                    .as("a worker decode failure reaches the consumer that drains the row group")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("decode blew up");
        } finally {
            pool.shutdownNow();
        }
    }

    /**
     * The spatial plan, wired through the coordinator and the prefetcher: a dropped row group is neither fetched nor
     * decoded, and a narrowed one fetches only the pages of its planned rows. Each case runs on the speculative path
     * (prefetch and decode ahead) and on the serial one.
     */
    @Nested
    class Planned {

        @TempDir
        Path tmp;

        @ParameterizedTest(name = "prefetch and decode ahead = {0}")
        @ValueSource(ints = {0, 2})
        @Timeout(value = 30, unit = TimeUnit.SECONDS)
        void aDroppedRowGroupIsNeverFetchedNorDecoded(int ahead) throws Exception {
            Fixture fixture = Fixture.write(tmp, 4_000);
            ByteSpan droppedGroup = fixture.columnChunkSpan(1);
            long expectedRows = 4_000L - fixture.rowsPerRowGroup().get(1);

            ComputeExecutor pool = ComputeExecutor.ofParallelism(4);
            try (RecordingByteRangeSource source = recording(fixture)) {
                List<RowGroupSurvivor> survivors = fixture.survivors(source);
                RowGroupPlans plans = fixture.plannedBy(survivors, drops(1));
                source.reset();
                ParallelDecodeCoordinator coordinator =
                        fixture.coordinator(source, pool, DecodeBudget.defaultBudget(), ahead, ahead, survivors, plans);
                try (coordinator) {
                    assertThat(drainTotalRows(coordinator))
                            .as("every row of the kept row groups is delivered")
                            .isEqualTo(expectedRows);
                }
                assertThat(source.readAnyByteIn(droppedGroup.start(), droppedGroup.end()))
                        .as("no byte of the dropped row group was fetched")
                        .isFalse();
            } finally {
                pool.shutdownNow();
            }
        }

        @ParameterizedTest(name = "prefetch and decode ahead = {0}")
        @ValueSource(ints = {0, 2})
        @Timeout(value = 30, unit = TimeUnit.SECONDS)
        void aNarrowedRowGroupFetchesOnlyItsKeptPages(int ahead) throws Exception {
            Fixture fixture = Fixture.writePaged(tmp);
            long expectedRows = PAGED_ROWS_PER_PAGE + (long) PAGED_ROWS_PER_GROUP * (PAGED_GROUPS - 1);

            ComputeExecutor pool = ComputeExecutor.ofParallelism(4);
            try (RecordingByteRangeSource source = recording(fixture)) {
                List<RowGroupSurvivor> survivors = fixture.survivors(source);
                RowGroupPlans plans = fixture.plannedBy(survivors, narrowsToTheFirstPage(0));
                ByteSpan skippedPages = fixture.pagesAfterTheFirst(survivors, 0);
                source.reset();
                ParallelDecodeCoordinator coordinator =
                        fixture.coordinator(source, pool, DecodeBudget.defaultBudget(), ahead, ahead, survivors, plans);
                try (coordinator) {
                    assertThat(drainTotalRows(coordinator))
                            .as("the narrowed row group delivers only its kept page")
                            .isEqualTo(expectedRows);
                }
                assertThat(source.readAnyByteIn(skippedPages.start(), skippedPages.end()))
                        .as("no byte past the kept page of the narrowed row group was fetched")
                        .isFalse();
            } finally {
                pool.shutdownNow();
            }
        }

        private RecordingByteRangeSource recording(Fixture fixture) {
            return new RecordingByteRangeSource(ByteRangeSource.ofFile(fixture.file()));
        }

        private RowGroupPlanner drops(int position) {
            return planned -> planned == position ? RowGroupNarrowing.dropped() : RowGroupNarrowing.whole();
        }

        private RowGroupPlanner narrowsToTheFirstPage(int position) {
            RowRanges firstPage = new RowRanges(List.of(new Range(0, PAGED_ROWS_PER_PAGE - 1L)));
            return planned -> planned == position ? RowGroupNarrowing.narrowedTo(firstPage) : RowGroupNarrowing.whole();
        }
    }

    /** A half-open byte span of the file under test. */
    private record ByteSpan(long start, long end) {}

    private static DecodedRowGroup failingStreamingRowGroup(ComputeExecutor pool) {
        RowGroupBatchDriver driver = new FailingDriver(new IllegalStateException("decode blew up"));
        BatchHandoff handoff = new BatchHandoff(2);
        BatchSpillStore spillStore = new BatchSpillStore(
                Path.of(System.getProperty("java.io.tmpdir")),
                DiskBudget.defaultBudget(),
                TestBatches.singleIntColumnSchema(),
                SpillAccumulator.NONE);
        StreamingBatchSource source =
                new StreamingBatchSource(handoff, driver, DecodeBudget.defaultBudget(), spillStore, true);
        source.attachProducerTask(pool.submitAcquired(() -> {
            source.runProducer();
            return null;
        }));
        return new DecodedRowGroup(source, /*recordEvalRequired*/ true);
    }

    private static List<Long> drainPerRowGroupCounts(ParallelDecodeCoordinator coordinator) throws IOException {
        List<Long> counts = new ArrayList<>();
        for (DecodedRowGroup rowGroup = coordinator.next(); rowGroup != null; rowGroup = coordinator.next()) {
            try (DecodedRowGroup current = rowGroup) {
                counts.add(drainRowGroup(current));
            }
        }
        return counts;
    }

    private static long drainTotalRows(ParallelDecodeCoordinator coordinator) throws IOException {
        long total = 0;
        for (DecodedRowGroup rowGroup = coordinator.next(); rowGroup != null; rowGroup = coordinator.next()) {
            try (DecodedRowGroup current = rowGroup) {
                total += drainRowGroup(current);
            }
        }
        return total;
    }

    private static long drainRowGroup(DecodedRowGroup rowGroup) {
        long rows = 0;
        while (rowGroup.hasNext()) {
            try (ParquetRecordBatch batch = rowGroup.next()) {
                rows += batch.rowCount();
            }
        }
        return rows;
    }

    /** The schema of the paged fixture: one REQUIRED INT64 leaf named {@code id}. */
    private static ParquetSchema singleLongColumnSchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /** A driver that fails on the first batch, modelling a worker-side decode failure. */
    private static final class FailingDriver implements RowGroupBatchDriver {

        private final RuntimeException failure;

        private FailingDriver(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public boolean hasMore() {
            return true;
        }

        @Override
        public ParquetRecordBatch nextBatch() {
            throw failure;
        }

        @Override
        public void close() {
            // nothing to release; this fixture holds no fetch
        }
    }

    /** A written multi-row-group fixture plus the machinery to build a coordinator over it. */
    private record Fixture(
            Path file, ParquetSchema schema, FileMetaData footer, List<Long> rowsPerRowGroup, int rowGroupCount) {

        static Fixture write(Path dir, int rows) throws IOException {
            return open(TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(dir, rows));
        }

        /**
         * A one-column file of {@link #PAGED_GROUPS} row groups, each of {@link #PAGED_ROWS_PER_GROUP} rows cut into
         * pages of {@link #PAGED_ROWS_PER_PAGE} values, which gives a row mask whole pages to skip.
         */
        static Fixture writePaged(Path dir) throws IOException {
            ParquetSchema schema = singleLongColumnSchema();
            WriteOptions options = WriteOptions.builder()
                    .tempDir(dir)
                    .rowGroupSize(RowGroupSize.rows(PAGED_ROWS_PER_GROUP))
                    .pageValueLimit(PAGED_ROWS_PER_PAGE)
                    .build();
            Path file = dir.resolve("paged-long-column.parquet");
            try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
                ParquetRecordBatchBuilder appender = writer.appender(PAGED_ROWS_PER_GROUP);
                for (long id = 0; id < (long) PAGED_ROWS_PER_GROUP * PAGED_GROUPS; id++) {
                    WriteFixtures.appendRow(appender, schema, Map.of(PAGED_COLUMN, id));
                }
                appender.flush();
            }
            return open(file);
        }

        private static Fixture open(Path file) {
            try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
                FileMetaData footer = ParquetFormat.readFooter(source);
                ParquetSchema schema = SchemaBuilder.build(footer.schema());
                List<RowGroup> rowGroups = footer.rowGroups();
                List<Long> rowsPerRowGroup =
                        rowGroups.stream().map(RowGroup::numRows).toList();
                return new Fixture(file, schema, footer, rowsPerRowGroup, rowGroups.size());
            }
        }

        /** The on-disk byte span covering every column chunk of row group {@code index}. */
        ByteSpan columnChunkSpan(int index) {
            long start = Long.MAX_VALUE;
            long end = Long.MIN_VALUE;
            for (ColumnChunk chunk : footer.rowGroups().get(index).columns()) {
                ColumnMetaData meta = chunk.metaData().orElseThrow();
                long chunkStart = meta.dictionaryPageOffset().orElse(meta.dataPageOffset());
                start = Math.min(start, chunkStart);
                end = Math.max(end, chunkStart + meta.totalCompressedSize());
            }
            return new ByteSpan(start, end);
        }

        /** The byte span of row group {@code index} from its column's second data page to the end of its chunk. */
        ByteSpan pagesAfterTheFirst(List<RowGroupSurvivor> survivors, int index) {
            ColumnPath leaf = schema.leafColumns().get(0);
            OffsetIndex offsetIndex =
                    survivors.get(index).chunks().offsetIndex(leaf).orElseThrow();
            long secondPageOffset = offsetIndex.pageLocations().get(1).offset();
            return new ByteSpan(secondPageOffset, columnChunkSpan(index).end());
        }

        ParallelDecodeCoordinator coordinator(
                ByteRangeSource source, ComputeExecutor executor, DecodeBudget budget, int decodeAhead) {
            List<RowGroupSurvivor> survivors = survivors(source);
            return coordinator(
                    source,
                    executor,
                    budget,
                    decodeAhead,
                    DEFAULT_PREFETCH_DEPTH,
                    survivors,
                    unplannedPlans(survivors));
        }

        @SuppressWarnings("java:S107") // test factory: every parameter is one knob of the coordinator under test
        ParallelDecodeCoordinator coordinator(
                ByteRangeSource source,
                ComputeExecutor executor,
                DecodeBudget budget,
                int decodeAhead,
                int prefetchDepth,
                List<RowGroupSurvivor> survivors,
                RowGroupPlans plans) {
            RowGroupPrefetcher prefetcher = prefetcher(source, survivors, plans, prefetchDepth);
            List<Boolean> recordEvalRequired =
                    survivors.stream().map(RowGroupSurvivor::recordEvalRequired).toList();
            return new ParallelDecodeCoordinator(
                    prefetcher,
                    executor,
                    budget,
                    TestDecodeBuffers.ample(),
                    DiskBudget.defaultBudget(),
                    Path.of(System.getProperty("java.io.tmpdir")),
                    true,
                    decodeAhead,
                    schema,
                    schema,
                    OptionalInt.empty(),
                    plans,
                    recordEvalRequired,
                    Optional.empty(),
                    BatchForm.LEVELS,
                    ParallelDecodeCoordinator.DecodeObservation.NONE,
                    List.of(),
                    Optional.empty());
        }

        List<RowGroupSurvivor> survivors(ByteRangeSource source) {
            IndexSectionLoader loader = indexLoader(source);
            return TestRowGroupChunks.allOf(footer, schema, loader).stream()
                    .map(RowGroupSurvivor::full)
                    .toList();
        }

        /** Plans that drop nothing and read each row group as the filter pipeline left it. */
        RowGroupPlans unplannedPlans(List<RowGroupSurvivor> survivors) {
            return RowGroupPlans.unplanned(survivors, emptyMasks(survivors), /*pageNarrowedFetch*/ true);
        }

        /** Plans driven by {@code planner}, narrowing a row group through a mask over every scanned leaf. */
        RowGroupPlans plannedBy(List<RowGroupSurvivor> survivors, RowGroupPlanner planner) {
            RowGroupPlans.NarrowedMaskFactory maskFactory =
                    (survivor, rows) -> RowMasks.maskFor(survivor.chunks(), rows, schema.leafColumns());
            return RowGroupPlans.planned(
                    survivors, emptyMasks(survivors), /*pageNarrowedFetch*/ true, planner, maskFactory);
        }

        private static List<Optional<RowMask>> emptyMasks(List<RowGroupSurvivor> survivors) {
            return Collections.nCopies(survivors.size(), Optional.empty());
        }

        private RowGroupPrefetcher prefetcher(
                ByteRangeSource source, List<RowGroupSurvivor> survivors, RowGroupPlans plans, int prefetchDepth) {
            RowGroupFetcher fetcher = TestFetchers.over(
                    source, schema, schema, SegmentPool.create(), FetchAccumulator.NONE, NO_COALESCE_GAP, MAX_SPAN);
            ExecutorService executor = Executors.newThreadPerTaskExecutor(
                    Thread.ofVirtual().name("test-fetch-", 0).factory());
            return new RowGroupPrefetcher(
                    survivors,
                    plans,
                    fetcher,
                    FetchBudget.defaultBudget(),
                    executor,
                    prefetchDepth,
                    /*maxConcurrent*/ 2);
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
    }
}
