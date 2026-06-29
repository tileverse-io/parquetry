/*
 * Copyright (c) 2026 Multivers.io
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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaBuilder;

/**
 * Drives {@link BatchPipeline#rows} over a real multi-row-group fixture to prove the row stream keeps at most one
 * decoded batch live at a time (the regression guard against the {@code flatMap} buffering leak), preserves file order
 * and filter semantics, and releases its current batch and the coordinator when closed early.
 */
class BatchPipelineRowsTest {

    private static final ColumnPath YEAR = ColumnPath.of("year");

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void consumingRowsKeepsAtMostOneBatchOpen(@TempDir Path tmp) throws Exception {
        Fixture fixture = Fixture.write(tmp, 4_000);
        assertThat(fixture.rowGroupCount())
                .as("the fixture spans several row groups")
                .isGreaterThanOrEqualTo(3);

        OpenBatchTracker tracker = new OpenBatchTracker();
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixture.file())) {
            ParallelDecodeCoordinator coordinator = fixture.serialCoordinator(source, AllFull.INSTANCE);
            long rows = 0;
            try (Stream<Integer> stream = BatchPipeline.rows(
                    coordinator, yearMaterializer(), fixture.schema(), null, false, false, Optional.empty(), tracker)) {
                Iterator<Integer> it = stream.iterator();
                while (it.hasNext()) {
                    it.next();
                    rows++;
                    assertThat(tracker.openCount())
                            .as("at most one decoded batch is live while pulling rows one at a time")
                            .isLessThanOrEqualTo(1);
                }
            }
            assertThat(rows).as("every fixture row is delivered").isEqualTo(4_000L);
        }
        assertThat(tracker.openCount())
                .as("the last batch is closed once the stream is exhausted")
                .isZero();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void rowsAreEmittedInOrderWithFilterSemantics(@TempDir Path tmp) throws Exception {
        Fixture fixture = Fixture.write(tmp, 4_000);

        List<Integer> unfiltered = collectYears(fixture, AllFull.INSTANCE, null);
        assertThat(unfiltered)
                .as("a null filter passes every row through, in file order: year cycles 2020..2024")
                .hasSize(4_000)
                .startsWith(2020, 2021, 2022, 2023, 2024, 2020);

        Predicate yearIs2022 = new Predicate.Eq(YEAR, new Value.IntVal(2022));
        List<Integer> filtered = collectYears(fixture, AllFull.INSTANCE, yearIs2022);
        assertThat(filtered)
                .as("a non-null filter skips every non-matching row")
                .containsOnly(2022)
                .hasSize(unfiltered.stream().filter(y -> y == 2022).toList().size());

        List<Integer> matchedSkipsEval = collectYears(fixture, AllMatched.INSTANCE, alwaysFalseSpy());
        assertThat(matchedSkipsEval)
                .as("a MATCHED row group emits every row without testing the filter")
                .hasSize(4_000)
                .containsExactlyElementsOf(unfiltered);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void closingStreamEarlyClosesCurrentBatchAndCoordinator(@TempDir Path tmp) throws Exception {
        Fixture fixture = Fixture.write(tmp, 4_000);

        OpenBatchTracker tracker = new OpenBatchTracker();
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixture.file())) {
            ExecutorService fetchExecutor = newFetchExecutor();
            ParallelDecodeCoordinator coordinator = fixture.serialCoordinator(source, AllFull.INSTANCE, fetchExecutor);
            try (Stream<Integer> stream = BatchPipeline.rows(
                    coordinator, yearMaterializer(), fixture.schema(), null, false, false, Optional.empty(), tracker)) {
                Iterator<Integer> it = stream.iterator();
                assertThat(it.hasNext()).as("the stream has rows to consume").isTrue();
                it.next();
                assertThat(tracker.openCount())
                        .as("one batch is open after consuming the first row")
                        .isEqualTo(1);
            }
            assertThat(tracker.openCount())
                    .as("closing the stream early closes the current batch")
                    .isZero();
            assertThat(fetchExecutor.isShutdown())
                    .as("closing the stream cascades to the coordinator, which closes the prefetcher's executor")
                    .isTrue();
        }
    }

    private static List<Integer> collectYears(Fixture fixture, SurvivorMode mode, Predicate filter) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixture.file())) {
            ParallelDecodeCoordinator coordinator = fixture.serialCoordinator(source, mode);
            List<Integer> years = new ArrayList<>();
            try (Stream<Integer> stream = BatchPipeline.rows(
                    coordinator,
                    yearMaterializer(),
                    fixture.schema(),
                    filter,
                    false,
                    false,
                    Optional.empty(),
                    batch -> {})) {
                stream.forEach(years::add);
            }
            return years;
        }
    }

    /** Materializes each row to its {@code year} value, exercising the real {@link ParquetRecord} path. */
    private static Materializer<Integer> yearMaterializer() {
        return (projectedSchema, row) -> (Integer) row.get(YEAR);
    }

    private static ExecutorService newFetchExecutor() {
        return Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("test-fetch-", 0).factory());
    }

    /** A filter that would reject every row if evaluated; used to prove a MATCHED row group never tests it. */
    private static Predicate alwaysFalseSpy() {
        return new Predicate.Eq(YEAR, new Value.IntVal(Integer.MIN_VALUE));
    }

    /** Counts batches handed to the pipeline that are not yet closed; the open count must never exceed one. */
    private static final class OpenBatchTracker implements Consumer<ParquetRecordBatch> {

        private final AtomicInteger open = new AtomicInteger();

        @Override
        public void accept(ParquetRecordBatch batch) {
            open.incrementAndGet();
            batch.attachReleaseAction(open::decrementAndGet);
        }

        int openCount() {
            return open.get();
        }
    }

    /** Marks per-row-group survivors as either FULL (per-row eval required) or MATCHED (eval skipped). */
    private interface SurvivorMode {
        RowGroupSurvivor wrap(RowGroupChunks chunks);
    }

    private enum AllFull implements SurvivorMode {
        INSTANCE;

        @Override
        public RowGroupSurvivor wrap(RowGroupChunks chunks) {
            return RowGroupSurvivor.full(chunks);
        }
    }

    private enum AllMatched implements SurvivorMode {
        INSTANCE;

        @Override
        public RowGroupSurvivor wrap(RowGroupChunks chunks) {
            return RowGroupSurvivor.matched(chunks);
        }
    }

    /** A written multi-row-group fixture plus the machinery to build a serial coordinator over it. */
    private record Fixture(Path file, ParquetSchema schema, List<RowGroup> rowGroups, int rowGroupCount) {

        static Fixture write(Path dir, int rows) throws IOException {
            Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(dir, rows);
            try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
                FileMetaData footer = ParquetFormat.readFooter(source);
                ParquetSchema schema = SchemaBuilder.build(footer.schema());
                List<RowGroup> rowGroups = footer.rowGroups();
                return new Fixture(file, schema, rowGroups, rowGroups.size());
            }
        }

        ParallelDecodeCoordinator serialCoordinator(ByteRangeSource source, SurvivorMode mode) {
            return serialCoordinator(source, mode, newFetchExecutor());
        }

        ParallelDecodeCoordinator serialCoordinator(
                ByteRangeSource source, SurvivorMode mode, ExecutorService fetchExecutor) {
            List<RowGroupSurvivor> survivors = survivors(source, mode);
            RowGroupPrefetcher prefetcher = prefetcher(source, survivors, fetchExecutor);
            List<Optional<RowMask>> masks = Collections.nCopies(survivors.size(), Optional.empty());
            List<Boolean> recordEvalRequired =
                    survivors.stream().map(RowGroupSurvivor::recordEvalRequired).toList();
            return new ParallelDecodeCoordinator(
                    prefetcher,
                    DecodeExecutor.shared(),
                    DecodeBudget.defaultBudget(),
                    TestDecodeBuffers.ample(),
                    DiskBudget.defaultBudget(),
                    Path.of(System.getProperty("java.io.tmpdir")),
                    true,
                    /*decodeAhead*/ 0,
                    schema,
                    schema,
                    OptionalInt.empty(),
                    masks,
                    recordEvalRequired,
                    Optional.empty(),
                    BatchForm.LEVELS,
                    ParallelDecodeCoordinator.DecodeObservation.NONE,
                    List.of(),
                    Optional.empty());
        }

        private List<RowGroupSurvivor> survivors(ByteRangeSource source, SurvivorMode mode) {
            IndexSectionLoader loader = indexLoader(source);
            return rowGroups.stream()
                    .map(rg -> mode.wrap(RowGroupChunks.of(rg, schema, loader)))
                    .toList();
        }

        private RowGroupPrefetcher prefetcher(
                ByteRangeSource source, List<RowGroupSurvivor> survivors, ExecutorService fetchExecutor) {
            RowGroupFetcher fetcher = TestFetchers.over(source, schema, schema, SegmentPool.create());
            return new RowGroupPrefetcher(
                    survivors,
                    fetcher,
                    FetchBudget.defaultBudget(),
                    fetchExecutor,
                    /*prefetchDepth*/ 2,
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
