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
package io.tileverse.parquetry.data.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaBuilder;
import io.tileverse.parquetry.testsupport.CountingSegmentPool;

/**
 * Drives {@link ParallelDecodeCoordinator} directly over a real multi-row-group fixture to prove file-order delivery,
 * deadlock freedom under a budget too small for any batch, the inline serial path, and producer-failure propagation.
 */
class ParallelDecodeCoordinatorTest {

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void emitsAllRowGroupsInFileOrder(@TempDir Path tmp) throws Exception {
        Fixture fixture = Fixture.write(tmp, 4_000);
        assertThat(fixture.rowGroupCount)
                .as("the fixture spans several row groups")
                .isGreaterThanOrEqualTo(3);

        DecodeExecutor pool = DecodeExecutor.ofParallelism(4);
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

        DecodeExecutor pool = DecodeExecutor.ofParallelism(4);
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
                    source, DecodeExecutor.shared(), DecodeBudget.defaultBudget(), /*decodeAhead*/ 0);
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
        DecodeExecutor pool = DecodeExecutor.ofParallelism(1);
        try (DecodedRowGroup rowGroup = failingStreamingRowGroup(pool)) {
            assertThatThrownBy(() -> drainRowGroup(rowGroup))
                    .as("a worker decode failure reaches the consumer that drains the row group")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("decode blew up");
        } finally {
            pool.shutdownNow();
        }
    }

    private static DecodedRowGroup failingStreamingRowGroup(DecodeExecutor pool) {
        RowGroupBatchDriver driver = new FailingDriver(new IllegalStateException("decode blew up"));
        BatchHandoff handoff = new BatchHandoff(2);
        StreamingBatchSource source =
                new StreamingBatchSource(handoff, driver, DecodeBudget.defaultBudget(), /*exempt*/ false);
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
            Path file, ParquetSchema schema, List<RowGroup> rowGroups, List<Long> rowsPerRowGroup, int rowGroupCount) {

        static Fixture write(Path dir, int rows) throws IOException {
            Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(dir, rows);
            try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
                FileMetaData footer = ParquetFormat.readFooter(source);
                ParquetSchema schema = SchemaBuilder.build(footer.schema());
                List<RowGroup> rowGroups = footer.rowGroups();
                List<Long> rowsPerRowGroup =
                        rowGroups.stream().map(RowGroup::numRows).toList();
                return new Fixture(file, schema, rowGroups, rowsPerRowGroup, rowGroups.size());
            }
        }

        ParallelDecodeCoordinator coordinator(
                ByteRangeSource source, DecodeExecutor executor, DecodeBudget budget, int decodeAhead) {
            List<RowGroupSurvivor> survivors = survivors(source);
            RowGroupPrefetcher prefetcher = prefetcher(source, survivors);
            List<Optional<RowMask>> masks = Collections.nCopies(survivors.size(), Optional.empty());
            List<Boolean> recordEvalRequired =
                    survivors.stream().map(RowGroupSurvivor::recordEvalRequired).toList();
            return new ParallelDecodeCoordinator(
                    prefetcher,
                    executor,
                    budget,
                    decodeAhead,
                    schema,
                    schema,
                    OptionalInt.empty(),
                    masks,
                    recordEvalRequired,
                    Optional.empty());
        }

        private List<RowGroupSurvivor> survivors(ByteRangeSource source) {
            IndexSectionLoader loader = indexLoader(source);
            return rowGroups.stream()
                    .map(rg -> RowGroupSurvivor.full(RowGroupChunks.of(rg, schema, loader)))
                    .toList();
        }

        private RowGroupPrefetcher prefetcher(ByteRangeSource source, List<RowGroupSurvivor> survivors) {
            RowGroupFetcher fetcher =
                    new RowGroupFetcher(source, schema, schema, new CountingSegmentPool(), 1 << 20, 8 << 20);
            ExecutorService executor = Executors.newThreadPerTaskExecutor(
                    Thread.ofVirtual().name("test-fetch-", 0).factory());
            return new RowGroupPrefetcher(
                    survivors,
                    fetcher,
                    FetchBudget.defaultBudget(),
                    executor, /*prefetchDepth*/
                    2, /*maxConcurrent*/
                    2);
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
