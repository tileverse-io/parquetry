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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Spliterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Bounded background pipeline that emits {@link ParquetRecordBatch} instances from a virtual-thread producer.
 *
 * <p>Parallels {@link RowGroupPipeline} in structure, replacing the per-row-group {@code RowGroupReader} with a
 * {@link BatchRowGroupReader}. The producer iterates each surviving row group, fetches the projected column chunks,
 * drains {@link BatchRowGroupReader#nextBatch()} into a bounded {@link LinkedBlockingQueue}, and then closes the reader
 * once the row group is exhausted. Backpressure is automatic: the producer blocks on the queue once
 * {@code prefetchWindow} batches are waiting.
 *
 * <p>Memory contract: at most {@code prefetchWindow} batches resident in the queue at once, plus the batch currently
 * being read by the consumer. The producer materializes all column vectors on the producer thread before enqueueing
 * each batch. Materialized vectors carry heap-backed primitive arrays (or {@code MemorySegment[]} with heap-backed
 * segments for binary kinds), so the consumer thread accesses them without the {@code WrongThreadException} that
 * {@code Arena.ofConfined()} enforces. After materialize-and-seal, the producer closes the batch's own Arena and the
 * per-page Arenas inside the column readers; queued batches no longer reference live Arena memory.
 * {@code Arena.ofShared()}-based zero-copy across threads is a future optimization.
 *
 * <h2>Cancellation</h2>
 *
 * <p>{@link #close()} (also invoked by the {@code onClose} hook on the stream from {@link #stream()}) sets a
 * cancellation flag, interrupts the producer thread, drains every queued batch (closing each {@link ParquetRecordBatch}
 * so its Arena is released), and joins the producer with a short timeout.
 *
 * <h2>Batch size</h2>
 *
 * <p>The {@code batchSizeCap} passed to {@link BatchRowGroupReader} defaults to {@link OptionalInt#empty()} - meaning
 * natural page-boundary-sized batches. A future enhancement will wire the cap from {@link ReadOptions} once that field
 * lands.
 */
public final class BatchRowGroupPipeline implements AutoCloseable {

    private static final AtomicLong PIPELINE_COUNTER = new AtomicLong();
    private static final long PRODUCER_JOIN_TIMEOUT_MILLIS = 5_000L;

    private final ParquetSchema fileSchema;
    private final ParquetSchema projectedSchema;
    private final List<RowGroupSurvivor> survivors;
    private final ColumnFetcher columnFetcher;

    private final LinkedBlockingQueue<BatchPipelineItem> queue;
    private final AtomicInteger state;
    private Thread producerThread;
    private boolean started;
    private BatchRecordSpliterator activeSpliterator;

    private static final int STATE_IDLE = 0;
    private static final int STATE_RUNNING = 1;
    private static final int STATE_CANCELLED = 2;

    /**
     * Builds a pipeline for {@code survivors} but does not start the producer; the producer starts on the first call to
     * {@link #stream()}.
     */
    public BatchRowGroupPipeline(
            RangeReader reader,
            ParquetSchema fileSchema,
            ParquetSchema projectedSchema,
            List<RowGroupSurvivor> survivors,
            ReadOptions options,
            ColumnFetcher columnFetcher) {
        Objects.requireNonNull(reader, "reader"); // Parameter validation; field is unused
        this.fileSchema = Objects.requireNonNull(fileSchema, "fileSchema");
        this.projectedSchema = Objects.requireNonNull(projectedSchema, "projectedSchema");
        this.survivors = List.copyOf(Objects.requireNonNull(survivors, "survivors"));
        Objects.requireNonNull(options, "options");
        this.columnFetcher = Objects.requireNonNull(columnFetcher, "columnFetcher");
        this.queue = new LinkedBlockingQueue<>(queueCapacity(options.prefetchWindow()));
        this.state = new AtomicInteger(STATE_IDLE);
    }

    /**
     * Returns a closeable stream over every batch in {@code survivors}. The stream's {@code onClose} hook cancels the
     * pipeline; callers must use try-with-resources to guarantee Arena release and producer thread shutdown.
     *
     * <p>The pipeline is single-use: calling {@code stream()} more than once throws {@link IllegalStateException}.
     */
    public Stream<ParquetRecordBatch> stream() {
        if (started) {
            throw new IllegalStateException("BatchRowGroupPipeline is single-use; stream() already called");
        }
        started = true;
        if (state.compareAndSet(STATE_IDLE, STATE_RUNNING)) {
            startProducer();
        }
        activeSpliterator = new BatchRecordSpliterator(queue);
        Stream<ParquetRecordBatch> stream = StreamSupport.stream(activeSpliterator, /*parallel*/ false);
        return stream.onClose(this::close);
    }

    /**
     * Cancels the pipeline, drains queued batches (closing each to release its Arena), and joins the producer thread.
     * Idempotent; subsequent calls return immediately. Closes any batches still in the queue. Each batch's underlying
     * Arena was already closed by the producer during materialize-and-seal; the close call here is an idempotent no-op
     * that respects the {@link AutoCloseable} contract.
     */
    @Override
    public void close() {
        int previous = state.getAndSet(STATE_CANCELLED);
        if (previous == STATE_CANCELLED) {
            return;
        }
        interruptProducer();
        drainQueueClosingBatches();
        joinProducer();
        drainQueueClosingBatches();
        releaseActiveSpliterator();
    }

    /** Signals the spliterator to stop pulling from the queue and closes any batch it was currently yielding. */
    private void releaseActiveSpliterator() {
        BatchRecordSpliterator sp = activeSpliterator;
        if (sp != null) {
            sp.releaseActive();
        }
    }

    /** True while the pipeline producer thread is alive; useful for tests that assert clean shutdown. */
    boolean producerAlive() {
        Thread t = producerThread;
        return t != null && t.isAlive();
    }

    // --- producer ---

    private void startProducer() {
        long pipelineId = PIPELINE_COUNTER.incrementAndGet();
        producerThread = Thread.ofVirtual()
                .name("parquetry-batch-pipeline-" + pipelineId)
                .start(this::runProducer);
    }

    private void runProducer() {
        try {
            for (RowGroupSurvivor survivor : survivors) {
                if (state.get() == STATE_CANCELLED) {
                    return;
                }
                if (!produceRowGroup(survivor)) {
                    return; // terminal failure pushed; stop fetching further row groups
                }
            }
            pushEndOfStream();
        } catch (PipelineCancelledException _) {
            // close() was called mid-fetch; nothing more to enqueue.
        }
    }

    /**
     * Fetches all projected columns for {@code survivor}, drains {@link BatchRowGroupReader#nextBatch()} into the
     * queue, and closes the reader when the row group is exhausted. Failures are turned into a {@link BatchFailure}
     * sentinel the spliterator rethrows on the consumer thread.
     *
     * <p>Each batch is prepared for cross-thread handoff before enqueuing: all vectors are materialized (heap-copied)
     * on the producer thread while the per-page confined Arenas are still valid, then the batch's own Arena is
     * released. This ensures the consumer thread can access vector values and call {@code batch.close()} without
     * touching any confined Arena created on the producer thread (see {@link BatchColumnReader} for the per-page Arena
     * lifecycle).
     *
     * @return {@code true} when all batches were enqueued successfully; {@code false} when a terminal failure was
     *     pushed
     */
    private boolean produceRowGroup(RowGroupSurvivor survivor) {
        List<FetchedColumnChunk> chunks;
        try {
            chunks = fetchProjectedColumns(survivor);
        } catch (RuntimeException e) {
            putItem(new BatchFailure(e));
            return false;
        }
        try (BatchRowGroupReader rgReader =
                new BatchRowGroupReader(chunks, projectedSchema, fileSchema, OptionalInt.empty())) {
            while (rgReader.hasMore()) {
                if (state.get() == STATE_CANCELLED) {
                    return false;
                }
                ParquetRecordBatch batch = rgReader.nextBatch();
                // Materialize all vectors on the producer thread while the per-page confined Arenas are live,
                // then seal the batch (releases the batch's own Arena on this thread). After this point the
                // vectors are heap-backed and safe to read from any thread; close() on the consumer is a no-op.
                materializeAndSeal(batch);
                if (!putItemSafely(new BatchReady(batch))) {
                    // Cancellation arrived while blocked on put; batch is already sealed, nothing to release.
                    return false;
                }
            }
        } catch (RuntimeException e) {
            // rgReader.close() (try-with-resources) already cleaned up column readers; propagate failure.
            putItem(new BatchFailure(e));
            return false;
        }
        return true;
    }

    /**
     * Materializes all vectors in {@code batch} on the calling thread, then closes the batch's own Arena. After this
     * call, the batch's vectors are heap-backed and the batch can safely be handed to a different thread.
     *
     * <p>This is required because {@link BatchColumnReader} creates per-page Arenas via
     * {@link java.lang.foreign.Arena#ofConfined()}, which binds the Arena's memory segments to the creating thread.
     * Without eager materialization, the consumer thread would trigger a {@link java.lang.foreign.WrongThreadException}
     * on first access. Once materialized, vectors hold heap arrays only - no Arena dependency. The batch's own Arena
     * (from {@link BatchRowGroupReader#nextBatch()}) is also confined, so we close it here rather than delegating to
     * the consumer thread.
     */
    private static void materializeAndSeal(ParquetRecordBatch batch) {
        try {
            for (ColumnVector vec : batch.columns().values()) {
                vec.materialize();
            }
        } finally {
            // Close the batch's confined Arena on this thread. The batch is now "sealed": all vectors are
            // heap-backed, and batch.close() on the consumer will be a safe no-op.
            batch.close();
        }
    }

    /**
     * Resolves the projected schema's leaf columns against the row group's column chunks, then fetches each chunk via
     * the {@link ColumnFetcher}. Mirrors {@link RowGroupReader#resolveProjectedColumns()} and
     * {@link RowGroupReader#fetchSequentially}.
     */
    private List<FetchedColumnChunk> fetchProjectedColumns(RowGroupSurvivor survivor) {
        io.tileverse.parquetry.format.RowGroup rowGroup = survivor.rowGroup();
        Map<List<String>, ColumnChunk> chunksByPath = indexChunksByPath(rowGroup);
        List<ColumnPath> projectedLeaves = projectedSchema.leafColumns();
        List<FetchedColumnChunk> fetched = new ArrayList<>(projectedLeaves.size());
        try {
            for (ColumnPath path : projectedLeaves) {
                ColumnChunk chunk = chunksByPath.get(path.parts());
                if (chunk == null) {
                    throw new IllegalStateException("Row group does not contain column " + path.dot());
                }
                fetched.add(columnFetcher.fetch(chunk, path));
            }
        } catch (IOException | RuntimeException e) {
            // Close any chunks already fetched before propagating to avoid leaking pooled buffers.
            for (FetchedColumnChunk c : fetched) {
                try {
                    c.close();
                } catch (RuntimeException suppressed) {
                    e.addSuppressed(suppressed);
                }
            }
            if (e instanceof IOException ioe) {
                throw new UncheckedIOException("Column fetch failed in row group", ioe);
            }
            throw (RuntimeException) e;
        }
        return fetched;
    }

    private static Map<List<String>, ColumnChunk> indexChunksByPath(io.tileverse.parquetry.format.RowGroup rowGroup) {
        Map<List<String>, ColumnChunk> index = new LinkedHashMap<>();
        for (ColumnChunk chunk : rowGroup.columns()) {
            ColumnMetaData meta = chunk.metaData()
                    .orElseThrow(
                            () -> new IllegalStateException("ColumnChunk without inline metadata is not supported"));
            index.put(meta.pathInSchema(), chunk);
        }
        return index;
    }

    private void pushEndOfStream() {
        putItem(BatchEndOfStream.INSTANCE);
    }

    /**
     * Puts {@code item} on the queue, blocking on backpressure. If cancellation arrives the item is returned and a
     * {@link PipelineCancelledException} is thrown so the producer loop can exit without leaking the batch.
     */
    private void putItem(BatchPipelineItem item) {
        if (!putItemSafely(item)) {
            if (item instanceof BatchReady(ParquetRecordBatch batch)) {
                batch.close();
            }
            throw new PipelineCancelledException();
        }
    }

    private boolean putItemSafely(BatchPipelineItem item) {
        while (true) {
            if (state.get() == STATE_CANCELLED) {
                return false;
            }
            try {
                if (queue.offer(item, 100, TimeUnit.MILLISECONDS)) {
                    return true;
                }
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    // --- cancellation ---

    private void interruptProducer() {
        Thread t = producerThread;
        if (t != null) {
            t.interrupt();
        }
    }

    private void joinProducer() {
        Thread t = producerThread;
        if (t == null) {
            return;
        }
        try {
            t.join(PRODUCER_JOIN_TIMEOUT_MILLIS);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Empties the queue, closing every {@link BatchReady} item so its Arena is released. {@link BatchFailure} and
     * {@link BatchEndOfStream} items carry no resources; we just discard them. Each batch's underlying Arena was
     * already closed by the producer during materialize-and-seal; the close call here is an idempotent no-op that
     * respects the {@link AutoCloseable} contract.
     */
    private void drainQueueClosingBatches() {
        while (true) {
            BatchPipelineItem item = queue.poll();
            if (item == null) {
                return;
            }
            if (item instanceof BatchReady(ParquetRecordBatch batch)) {
                try {
                    batch.close();
                } catch (RuntimeException _) {
                    // best-effort drain; swallow so we continue closing remaining items
                }
            }
        }
    }

    private static int queueCapacity(int prefetchWindow) {
        return prefetchWindow <= 0 ? 1 : prefetchWindow;
    }

    // --- pipeline item ADT ---

    /**
     * Sentinel pushed by the producer into the bounded queue. The spliterator pattern-matches on the three concrete
     * subtypes to yield a batch, propagate an error, or terminate iteration.
     */
    sealed interface BatchPipelineItem permits BatchReady, BatchFailure, BatchEndOfStream {}

    /** A batch ready for consumption. The caller is responsible for closing it when done. */
    record BatchReady(ParquetRecordBatch batch) implements BatchPipelineItem {}

    /** Producer-side failure: the spliterator rethrows {@code cause} on the consumer thread. */
    record BatchFailure(Throwable cause) implements BatchPipelineItem {}

    /** End-of-stream sentinel: the spliterator stops iterating after seeing this. */
    static final class BatchEndOfStream implements BatchPipelineItem {
        static final BatchEndOfStream INSTANCE = new BatchEndOfStream();

        private BatchEndOfStream() {}
    }

    /**
     * Internal marker thrown by the producer loop when it discovers cancellation while blocked on a queue put. Never
     * escapes the pipeline.
     */
    @SuppressWarnings("serial")
    private static final class PipelineCancelledException extends RuntimeException {
        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }

    // --- spliterator ---

    /**
     * Drains batch items from the pipeline queue and yields them one at a time to the caller's {@link Stream#forEach} /
     * iterator.
     *
     * <p>Each batch returned by {@link #tryAdvance} is the raw {@link ParquetRecordBatch} from the queue; the caller is
     * responsible for closing it after use. The spliterator itself only closes the active batch when
     * {@link #releaseActive()} is called (from the pipeline's {@code close()} via the stream's {@code onClose} hook).
     *
     * <h2>Characteristics</h2>
     *
     * <p>{@link Spliterator#ORDERED} and {@link Spliterator#NONNULL}. {@link #trySplit()} returns {@code null}:
     * parallelism is provided by the producer prefetch, not by {@code Stream.parallel()}.
     */
    static final class BatchRecordSpliterator implements Spliterator<ParquetRecordBatch> {

        private final LinkedBlockingQueue<BatchPipelineItem> queue;
        private ParquetRecordBatch activeBatch;
        private boolean exhausted;

        BatchRecordSpliterator(LinkedBlockingQueue<BatchPipelineItem> queue) {
            this.queue = Objects.requireNonNull(queue, "queue");
        }

        @Override
        public boolean tryAdvance(Consumer<? super ParquetRecordBatch> action) {
            Objects.requireNonNull(action, "action");
            if (exhausted) {
                return false;
            }
            BatchPipelineItem next = takeBlocking();
            return switch (next) {
                case BatchReady(ParquetRecordBatch batch) -> {
                    activeBatch = batch;
                    action.accept(activeBatch);
                    activeBatch = null; // consumer has taken ownership
                    yield true;
                }
                case BatchFailure failure -> rethrow(failure);
                case BatchEndOfStream _ -> finish();
            };
        }

        @Override
        public Spliterator<ParquetRecordBatch> trySplit() {
            return null;
        }

        @Override
        public long estimateSize() {
            return Long.MAX_VALUE;
        }

        @Override
        public int characteristics() {
            return ORDERED | NONNULL;
        }

        /**
         * Closes the active batch (if the consumer was interrupted mid-delivery) and marks the spliterator exhausted.
         * Called from the pipeline's {@code close()} via the stream's {@code onClose} hook.
         */
        void releaseActive() {
            exhausted = true;
            ParquetRecordBatch toClose = activeBatch;
            activeBatch = null;
            if (toClose != null) {
                toClose.close();
            }
        }

        private boolean rethrow(BatchFailure failure) {
            exhausted = true;
            Throwable cause = failure.cause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            if (cause instanceof IOException ioe) {
                throw new UncheckedIOException("Batch row-group pipeline failed", ioe);
            }
            throw new UncheckedIOException("Batch row-group pipeline failed", new IOException(cause));
        }

        private boolean finish() {
            exhausted = true;
            return false;
        }

        private BatchPipelineItem takeBlocking() {
            try {
                return queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                exhausted = true;
                throw new IllegalStateException("Interrupted while waiting for next batch", e);
            }
        }
    }
}
