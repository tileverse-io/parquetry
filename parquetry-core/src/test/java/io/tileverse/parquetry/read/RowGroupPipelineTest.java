/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.read;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.assembly.BasicColumnReader;
import io.tileverse.parquetry.assembly.ColumnReader;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.enums.CompressionCodec;
import io.tileverse.parquetry.format.enums.Encoding;
import io.tileverse.parquetry.format.enums.Type;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.page.LevelDecoder;
import io.tileverse.parquetry.page.plain.PlainInt32Decoder;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.Schema;

import io.tileverse.io.ByteBufferPool;

/**
 * Exercises the row-group prefetch pipeline against a fleet of synthetic single-column row groups: lazy record flow,
 * mid-iteration cancellation (pool drain + producer thread shutdown), backpressure on {@code prefetchWindow == 1}, and
 * failure propagation from the producer thread to the consumer iteration.
 *
 * <p>Real I/O is not exercised here; the tests substitute a stub {@link ColumnFetcher} that hands back pooled buffers
 * and a fixed column-reader factory that drives {@link BasicColumnReader} from a pre-built {@link PlainInt32Decoder},
 * exactly the way {@code RowGroupReaderTest} does. The pipeline composes {@link RowGroupReader}s through its
 * package-private constructor so the orchestration layer can be tested in isolation from the page-cursor work.
 */
class RowGroupPipelineTest {

    private static final int ROWS_PER_GROUP = 4;
    private static final int ROW_GROUPS = 5;

    private static final ColumnPath VALUE_PATH = ColumnPath.of("value");

    @Test
    void lazyStreamYieldsEveryRecordInRowGroupOrder() {
        Schema schema = singleColumnSchema();
        List<RowGroupSurvivor> survivors = makeSurvivors(ROW_GROUPS);
        ByteBufferPool pool = freshPool();

        RowGroupPipeline<ParquetRecord> pipeline = newPipeline(schema, survivors, pool, /*prefetchWindow*/ 2);
        List<Integer> values = new ArrayList<>();
        try (Stream<ParquetRecord> stream = pipeline.stream()) {
            stream.forEach(parquetRecord -> values.add(parquetRecord.getInt(VALUE_PATH)));
        }

        assertThat(values).hasSize(ROW_GROUPS * ROWS_PER_GROUP);
        for (int rg = 0; rg < ROW_GROUPS; rg++) {
            for (int row = 0; row < ROWS_PER_GROUP; row++) {
                int expected = rg * 100 + row;
                assertThat(values.get(rg * ROWS_PER_GROUP + row)).isEqualTo(expected);
            }
        }
        assertThat(outstandingBorrows(pool))
                .as("pool must drain after stream close")
                .isZero();
        awaitProducerExit(pipeline);
    }

    @Test
    void emptySurvivorsProducesZeroRecords() {
        Schema schema = singleColumnSchema();
        ByteBufferPool pool = freshPool();

        RowGroupPipeline<ParquetRecord> pipeline = newPipeline(schema, List.of(), pool, /*prefetchWindow*/ 2);
        List<ParquetRecord> records = new ArrayList<>();
        try (Stream<ParquetRecord> stream = pipeline.stream()) {
            stream.forEach(records::add);
        }

        assertThat(records).isEmpty();
        assertThat(outstandingBorrows(pool)).isZero();
        awaitProducerExit(pipeline);
    }

    @Test
    void singleSurvivorDrainsCleanly() {
        Schema schema = singleColumnSchema();
        ByteBufferPool pool = freshPool();

        RowGroupPipeline<ParquetRecord> pipeline = newPipeline(schema, makeSurvivors(1), pool, /*prefetchWindow*/ 2);
        List<ParquetRecord> records = new ArrayList<>();
        try (Stream<ParquetRecord> stream = pipeline.stream()) {
            stream.forEach(records::add);
        }

        assertThat(records).hasSize(ROWS_PER_GROUP);
        assertThat(outstandingBorrows(pool)).isZero();
        awaitProducerExit(pipeline);
    }

    @Test
    void earlyCloseShutsDownProducerAndReleasesBuffers() {
        Schema schema = singleColumnSchema();
        ByteBufferPool pool = freshPool();
        RowGroupPipeline<ParquetRecord> pipeline = newPipeline(schema, makeSurvivors(ROW_GROUPS), pool, 2);

        try (Stream<ParquetRecord> stream = pipeline.stream()) {
            // Take exactly one record from the first row group, then bail out via try-with-resources close.
            stream.limit(1)
                    .forEach(parquetRecord ->
                            assertThat(parquetRecord.getInt(VALUE_PATH)).isZero());
        }

        awaitProducerExit(pipeline);
        assertThat(pipeline.producerAlive())
                .as("producer thread must exit promptly after stream close")
                .isFalse();
        assertThat(outstandingBorrows(pool))
                .as("every pooled buffer must return - in-flight prefetch included")
                .isZero();
    }

    @Test
    void prefetchWindowOneStaysAtMostOneRowGroupAhead() {
        Schema schema = singleColumnSchema();
        ByteBufferPool pool = freshPool();
        AtomicInteger fetchCount = new AtomicInteger();

        ColumnFetcher counting = countingFetcher(pool, fetchCount);
        RowGroupPipeline<ParquetRecord> pipeline = new RowGroupPipeline<>(
                noopReader(),
                schema,
                schema,
                makeSurvivors(ROW_GROUPS),
                readOptions(pool, /*prefetchWindow*/ 1),
                counting,
                Materializer.defaultRecord(),
                intReaderFactory());

        try (Stream<ParquetRecord> stream = pipeline.stream()) {
            Iterator<ParquetRecord> iterator = stream.iterator();
            // Drain the first row group's 4 records. After that the producer may have:
            //   - row group 1's fetch sitting on the queue (one prefetched ahead = prefetchWindow=1)
            //   - row group 2's fetch in flight, blocking on queue.put because the queue is full
            // So the maximum is 3 fetches: row group 0 (consumed), 1 (queued), 2 (in flight).
            for (int i = 0; i < ROWS_PER_GROUP; i++) {
                assertThat(iterator.hasNext()).isTrue();
                iterator.next();
            }
            // Brief settle window so the prefetch has a chance to materialize; the assertion is then load-bearing.
            awaitFetchCountAtLeast(fetchCount, 2);
            assertThat(fetchCount.get())
                    .as("prefetchWindow=1: at most {consumed} + {queued} + {in-flight on put} = 3 fetches")
                    .isLessThanOrEqualTo(3);
        }

        awaitProducerExit(pipeline);
        assertThat(outstandingBorrows(pool)).isZero();
    }

    @Test
    void producerFailurePropagatesAsUncheckedIOException() {
        Schema schema = singleColumnSchema();
        ByteBufferPool pool = freshPool();
        AtomicInteger fetchCount = new AtomicInteger();
        ColumnFetcher delegate = borrowingFetcher(pool);

        // Fail on row group 2 (the third fetch since each row group has one column).
        ColumnFetcher failingOnThird = (chunk, path) -> {
            int n = fetchCount.incrementAndGet();
            if (n == 3) {
                throw new IOException("synthetic fetch failure on row group 2");
            }
            return delegate.fetch(chunk, path);
        };

        RowGroupPipeline<ParquetRecord> pipeline = new RowGroupPipeline<>(
                noopReader(),
                schema,
                schema,
                makeSurvivors(ROW_GROUPS),
                readOptions(pool, /*prefetchWindow*/ 0),
                failingOnThird,
                Materializer.defaultRecord(),
                intReaderFactory());

        try (Stream<ParquetRecord> stream = pipeline.stream()) {
            // The synthetic IOException surfaces here as an UncheckedIOException carrying the original cause
            // because RowGroupReader wraps fetch failures and the pipeline rethrows the wrapper verbatim.
            assertThatThrownBy(() -> stream.forEach(_ -> {}))
                    .isInstanceOf(UncheckedIOException.class)
                    .hasRootCauseInstanceOf(IOException.class)
                    .hasRootCauseMessage("synthetic fetch failure on row group 2");
        }

        awaitProducerExit(pipeline);
        assertThat(outstandingBorrows(pool))
                .as("buffers borrowed for successful row groups must still return on failure")
                .isZero();
    }

    @Test
    void prefetchWindowZeroIsSynchronousAndStillDrains() {
        Schema schema = singleColumnSchema();
        ByteBufferPool pool = freshPool();

        RowGroupPipeline<ParquetRecord> pipeline = new RowGroupPipeline<>(
                noopReader(),
                schema,
                schema,
                makeSurvivors(ROW_GROUPS),
                readOptions(pool, /*prefetchWindow*/ 0),
                borrowingFetcher(pool),
                Materializer.defaultRecord(),
                intReaderFactory());

        List<Integer> values = new ArrayList<>();
        try (Stream<ParquetRecord> stream = pipeline.stream()) {
            stream.forEach(parquetRecord -> values.add(parquetRecord.getInt(VALUE_PATH)));
        }

        assertThat(values).hasSize(ROW_GROUPS * ROWS_PER_GROUP);
        awaitProducerExit(pipeline);
        assertThat(outstandingBorrows(pool)).isZero();
    }

    @Test
    void doubleStreamCallThrows() {
        Schema schema = singleColumnSchema();
        ByteBufferPool pool = freshPool();
        RowGroupPipeline<ParquetRecord> pipeline = newPipeline(schema, makeSurvivors(1), pool, 2);

        try (Stream<ParquetRecord> first = pipeline.stream()) {
            first.forEach(_ -> {});
            assertThatThrownBy(pipeline::stream).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void closeIsIdempotent() {
        Schema schema = singleColumnSchema();
        ByteBufferPool pool = freshPool();
        RowGroupPipeline<ParquetRecord> pipeline = newPipeline(schema, makeSurvivors(1), pool, 2);

        try (Stream<ParquetRecord> stream = pipeline.stream()) {
            stream.forEach(_ -> {});
        }
        pipeline.close();
        pipeline.close();
        assertThat(outstandingBorrows(pool)).isZero();
    }

    // --- fixtures ---

    private RowGroupPipeline<ParquetRecord> newPipeline(
            Schema schema, List<RowGroupSurvivor> survivors, ByteBufferPool pool, int prefetchWindow) {
        return new RowGroupPipeline<>(
                noopReader(),
                schema,
                schema,
                survivors,
                readOptions(pool, prefetchWindow),
                borrowingFetcher(pool),
                Materializer.defaultRecord(),
                intReaderFactory());
    }

    private static ReadOptions readOptions(ByteBufferPool pool, int prefetchWindow) {
        return ReadOptions.builder()
                .concurrencyMode(ConcurrencyMode.SYNC)
                .prefetchWindow(prefetchWindow)
                .byteBufferPool(pool)
                .build();
    }

    private static Schema singleColumnSchema() {
        return new Schema(new Field.Group(
                "root",
                Repetition.REQUIRED,
                List.of(new Field.Primitive(
                        "value", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1)),
                Optional.empty(),
                -1));
    }

    private static List<RowGroupSurvivor> makeSurvivors(int count) {
        List<RowGroupSurvivor> survivors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            survivors.add(RowGroupSurvivor.full(singleColumnRowGroup(i)));
        }
        return survivors;
    }

    /**
     * Builds a synthetic single-column row group. The {@code dataPageOffset} encodes the row-group ordinal so the test
     * column-reader factory can recover it later and emit deterministic per-row-group values without inventing a real
     * page stream.
     */
    private static RowGroup singleColumnRowGroup(int ordinal) {
        ColumnMetaData meta = new ColumnMetaData(
                Type.INT32,
                List.of(Encoding.PLAIN),
                List.of("value"),
                CompressionCodec.UNCOMPRESSED,
                /*numValues*/ ROWS_PER_GROUP,
                /*totalUncompressedSize*/ ROWS_PER_GROUP * 4L,
                /*totalCompressedSize*/ ROWS_PER_GROUP * 4L,
                /*keyValueMetadata*/ List.of(),
                /*dataPageOffset*/ ordinal * 1000L,
                /*indexPageOffset*/ OptionalLong.empty(),
                /*dictionaryPageOffset*/ OptionalLong.empty(),
                /*statistics*/ Optional.empty(),
                /*encodingStats*/ List.of(),
                /*bloomFilterOffset*/ OptionalLong.empty(),
                /*bloomFilterLength*/ OptionalLong.empty(),
                /*sizeStatistics*/ Optional.empty());
        ColumnChunk chunk = new ColumnChunk(
                /*filePath*/ Optional.empty(),
                /*fileOffset*/ ordinal * 1000L,
                Optional.of(meta),
                OptionalLong.empty(),
                OptionalInt.empty(),
                OptionalLong.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                Optional.empty());
        return new RowGroup(
                List.of(chunk),
                /*totalByteSize*/ ROWS_PER_GROUP * 4L,
                /*numRows*/ ROWS_PER_GROUP,
                /*sortingColumns*/ Optional.empty(),
                /*fileOffset*/ OptionalLong.of(ordinal * 1000L),
                /*totalCompressedSize*/ OptionalLong.of(ROWS_PER_GROUP * 4L),
                /*ordinal*/ OptionalInt.of(ordinal));
    }

    // --- column-fetcher stubs ---

    private static ColumnFetcher borrowingFetcher(ByteBufferPool pool) {
        return (chunk, path) -> new FetchedColumnChunk(
                path, chunk.metaData().orElseThrow(), /*maxRep*/ 0, /*maxDef*/ 0, pool.borrowHeap(4), Optional.empty());
    }

    private static ColumnFetcher countingFetcher(ByteBufferPool pool, AtomicInteger counter) {
        ColumnFetcher delegate = borrowingFetcher(pool);
        return (chunk, path) -> {
            counter.incrementAndGet();
            return delegate.fetch(chunk, path);
        };
    }

    // --- column reader factory ---

    /**
     * Decodes the row group ordinal from {@code metadata.dataPageOffset} (which the fixture stamps for this exact
     * purpose) and emits {@code ordinal * 100 + i} for {@code i} in {@code [0, ROWS_PER_GROUP)}. This lets us check
     * that records flow in row-group order without inventing a real page stream.
     */
    private static BiFunction<FetchedColumnChunk, Field.Primitive, ColumnReader> intReaderFactory() {
        return (chunk, leaf) -> {
            int ordinal = (int) (chunk.metadata().dataPageOffset() / 1000L);
            int[] values = new int[ROWS_PER_GROUP];
            for (int i = 0; i < ROWS_PER_GROUP; i++) {
                values[i] = ordinal * 100 + i;
            }
            ByteBuffer page = ByteBuffer.allocate(values.length * 4).order(ByteOrder.LITTLE_ENDIAN);
            for (int v : values) {
                page.putInt(v);
            }
            page.flip();
            PlainInt32Decoder decoder = new PlainInt32Decoder();
            decoder.load(page, values.length);
            return new BasicColumnReader(
                    chunk.path(), 0, 0, new LevelDecoder(0), new LevelDecoder(0), decoder, values.length);
        };
    }

    // --- helpers ---

    private static ByteBufferPool freshPool() {
        return new ByteBufferPool();
    }

    private static long outstandingBorrows(ByteBufferPool pool) {
        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        return (stats.created() + stats.reused()) - (stats.returned() + stats.discarded());
    }

    /**
     * Builds a minimal {@link RangeReader} that never serves a read; the test fetcher bypasses I/O so any call would
     * indicate a bug in the test wiring.
     */
    private static RangeReader noopReader() {
        return new RangeReader() {
            @Override
            public int readRange(long offset, int length, ByteBuffer target) {
                throw new UnsupportedOperationException("test fetcher bypasses I/O");
            }

            @Override
            public OptionalLong size() {
                return OptionalLong.empty();
            }

            @Override
            public String getSourceIdentifier() {
                return "test://pipeline";
            }

            @Override
            public void close() {
                // No-op: the test stub holds no I/O resources to release.
            }
        };
    }

    /**
     * Polls until the producer thread exits or a small timeout elapses. The timeout exists because virtual-thread join
     * is otherwise indefinite; the pipeline already self-imposes a 5-second join timeout in
     * {@link RowGroupPipeline#close()}, so the actual exit is fast in practice.
     */
    private static void awaitProducerExit(RowGroupPipeline<?> pipeline) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (pipeline.producerAlive() && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
        }
    }

    private static void awaitFetchCountAtLeast(AtomicInteger counter, int target) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
        while (counter.get() < target && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(5));
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
        }
    }
}
