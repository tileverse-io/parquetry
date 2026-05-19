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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.assembly.ColumnReader;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.Schema;

/**
 * Bounded background pipeline that fetches row group {@code N + k} while the consumer iterates {@code N}.
 *
 * <p>The pipeline owns a single virtual-thread producer that constructs a {@link RowGroupReader} for each surviving row
 * group, calls its {@code read(...)} to perform the column-chunk fan-out fetch + dictionary decode, and pushes the
 * resulting record {@link Stream} into a bounded {@link LinkedBlockingQueue} (the underlying reader's lifecycle is
 * bound to the stream via {@code onClose}). The {@link ParquetRecordSpliterator} the consumer drives drains that queue
 * one row group at a time. Backpressure is automatic: the producer blocks on the queue once {@code prefetchWindow}
 * prefetched row groups are waiting.
 *
 * <p>A prefetched row group holds its <em>compressed</em> column chunks (one pooled buffer per projected column) plus
 * its decoded dictionaries; data pages stay compressed until the consumer's iterator pulls. Memory amplification from
 * prefetch is therefore bounded by {@code prefetchWindow x Sigma_columns(compressed chunk bytes)} - decompressed page
 * bytes only resident for the currently-iterated row group. See the package documentation for the streaming memory
 * contract.
 *
 * <h2>Cancellation</h2>
 *
 * <p>{@link #close()} (also invoked by the {@code onClose} hook on the stream returned from {@link #stream()}) sets a
 * cancellation flag, interrupts the producer thread, drains every queued row group (closing each {@code RowGroupReader}
 * so its pooled buffers return to the pool), and joins the producer with a short timeout. In-flight
 * {@code RangeReader.readRange} calls receive {@code InterruptedException} per the cancellation contract (see the
 * package documentation).
 *
 * <h2>Prefetch window degenerate cases</h2>
 *
 * <ul>
 *   <li>{@code prefetchWindow == 0}: synchronous. The queue is sized at 1; the producer fetches one row group and
 *       blocks on the put until the consumer drains it. Resident memory matches a single {@link RowGroupReader}; the
 *       only overhead is one virtual thread alive (cheap).
 *   <li>{@code prefetchWindow == 1}: producer keeps one row group prefetched ahead of the consumer.
 *   <li>{@code prefetchWindow >= 2}: producer fills up to {@code prefetchWindow} prefetched row groups before blocking.
 * </ul>
 *
 * @param <T> the type returned by the materializer driving the consumer's stream
 */
public final class RowGroupPipeline<T> implements AutoCloseable {

    private static final AtomicLong PIPELINE_COUNTER = new AtomicLong();
    private static final long PRODUCER_JOIN_TIMEOUT_MILLIS = 5_000L;

    private final RangeReader reader;
    private final Schema fileSchema;
    private final Schema projectedSchema;
    private final List<RowGroupSurvivor> survivors;
    private final ReadOptions options;
    private final ColumnFetcher columnFetcher;
    private final Materializer<T> materializer;
    private final BiFunction<FetchedColumnChunk, Field.Primitive, ColumnReader> columnReaderFactory;

    private final LinkedBlockingQueue<PipelineItem<T>> queue;
    private final AtomicInteger state;
    private Thread producerThread;
    private boolean started;
    private ParquetRecordSpliterator<T> activeSpliterator;

    private static final int STATE_IDLE = 0;
    private static final int STATE_RUNNING = 1;
    private static final int STATE_CANCELLED = 2;

    /**
     * Builds a pipeline for {@code survivors} but does not start the producer; the producer starts on the first call to
     * {@link #stream()}.
     */
    public RowGroupPipeline(
            RangeReader reader,
            Schema fileSchema,
            Schema projectedSchema,
            List<RowGroupSurvivor> survivors,
            ReadOptions options,
            ColumnFetcher columnFetcher,
            Materializer<T> materializer) {
        this(reader, fileSchema, projectedSchema, survivors, options, columnFetcher, materializer, null);
    }

    /**
     * Test-friendly constructor mirroring {@link RowGroupReader}'s package-private hook: when
     * {@code columnReaderFactory} is non-null, each per-row-group {@link RowGroupReader} is built with that factory
     * instead of the production page-cursor factory (wired later). Tests use this to drive record flow end-to-end with
     * synthetic page bytes; production callers use the public constructor.
     */
    @SuppressWarnings("java:S107") // Test-only ctor; mirrors the public 7-arg ctor plus one factory hook.
    RowGroupPipeline(
            RangeReader reader,
            Schema fileSchema,
            Schema projectedSchema,
            List<RowGroupSurvivor> survivors,
            ReadOptions options,
            ColumnFetcher columnFetcher,
            Materializer<T> materializer,
            BiFunction<FetchedColumnChunk, Field.Primitive, ColumnReader> columnReaderFactory) {
        this.reader = Objects.requireNonNull(reader, "reader");
        this.fileSchema = Objects.requireNonNull(fileSchema, "fileSchema");
        this.projectedSchema = Objects.requireNonNull(projectedSchema, "projectedSchema");
        this.survivors = List.copyOf(Objects.requireNonNull(survivors, "survivors"));
        this.options = Objects.requireNonNull(options, "options");
        this.columnFetcher = Objects.requireNonNull(columnFetcher, "columnFetcher");
        this.materializer = Objects.requireNonNull(materializer, "materializer");
        this.columnReaderFactory = columnReaderFactory;
        this.queue = new LinkedBlockingQueue<>(queueCapacity(options.prefetchWindow()));
        this.state = new AtomicInteger(STATE_IDLE);
    }

    /**
     * Returns a closeable stream over every record in {@code survivors}. The stream's {@code onClose} hook cancels the
     * pipeline; callers must use try-with-resources to guarantee buffer return and producer thread shutdown.
     *
     * <p>The pipeline is single-use: calling {@code stream(...)} more than once throws {@link IllegalStateException}.
     */
    public Stream<T> stream() {
        if (started) {
            throw new IllegalStateException("RowGroupPipeline is single-use; stream() already called");
        }
        started = true;
        if (state.compareAndSet(STATE_IDLE, STATE_RUNNING)) {
            startProducer();
        }
        activeSpliterator = new ParquetRecordSpliterator<>(queue);
        Stream<T> stream = StreamSupport.stream(activeSpliterator, /*parallel*/ false);
        return stream.onClose(this::close);
    }

    /**
     * Cancels the pipeline, drains queued row groups (returning their pooled buffers), and joins the producer thread.
     * Idempotent; subsequent calls return immediately.
     */
    @Override
    public void close() {
        int previous = state.getAndSet(STATE_CANCELLED);
        if (previous == STATE_CANCELLED) {
            return;
        }
        interruptProducer();
        drainQueueClosingItems();
        joinProducer();
        drainQueueClosingItems();
        releaseActiveSpliterator();
    }

    /**
     * Closes whatever row group the spliterator was iterating when cancellation arrived. The spliterator may be null if
     * the pipeline was closed before {@link #stream()} was called.
     */
    private void releaseActiveSpliterator() {
        ParquetRecordSpliterator<T> sp = activeSpliterator;
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

    /**
     * Starts the single virtual-thread producer. Both {@code prefetchWindow == 0} and {@code prefetchWindow >= 1} use
     * the same producer shape: only the queue capacity differs. With capacity 1 the producer blocks on every
     * {@code put} until the consumer drains the queue, yielding the synchronous memory profile of a bare
     * {@link RowGroupReader}; with capacity {@code prefetchWindow} the producer fills ahead of the consumer.
     */
    private void startProducer() {
        long pipelineId = PIPELINE_COUNTER.incrementAndGet();
        producerThread =
                Thread.ofVirtual().name("parquetry-pipeline-" + pipelineId).start(this::runProducer);
    }

    private void runProducer() {
        try {
            for (RowGroupSurvivor survivor : survivors) {
                if (state.get() == STATE_CANCELLED) {
                    return;
                }
                if (!produceOne(survivor)) {
                    return; // terminal failure already pushed; no point fetching more
                }
            }
            pushEndOfStream();
        } catch (PipelineCancelledException _) {
            // close() was called mid-fetch; we already pushed nothing for this row group.
        }
    }

    /**
     * Builds one {@link RowGroupReader} per survivor, opens its stream, and pushes the pair into the queue. Failures
     * coming out of {@code RowGroupReader.read(...)} are turned into a {@link Failure} item the spliterator unwraps on
     * the consumer thread, preserving the original cause.
     *
     * @return {@code true} when the survivor was prepared and enqueued successfully (keep iterating); {@code false}
     *     when a terminal {@link Failure} sentinel was pushed and the producer should stop fetching further row groups
     *     whose results no consumer can ever drain.
     */
    private boolean produceOne(RowGroupSurvivor survivor) {
        RowGroupReader rgReader = newRowGroupReader(survivor);
        Stream<T> rgStream;
        try {
            rgStream = rgReader.read(materializer);
        } catch (RuntimeException e) {
            // RowGroupReader.read() closes itself on failure; just propagate to the consumer.
            putItem(new Failure<>(e));
            return false;
        }
        Prepared<T> prepared = new Prepared<>(rgStream);
        if (!putItemSafely(prepared)) {
            // Cancellation arrived while we were blocked on put; ensure the prepared row group is released.
            closePrepared(prepared);
        }
        return true;
    }

    private RowGroupReader newRowGroupReader(RowGroupSurvivor survivor) {
        // Pending: when RowGroupReader gains a survivingRows parameter (currently the column-index tier produces
        // narrowed row groups but the reader has no row-skipping path), pass survivor.survivingRows() here so
        // the column-index narrowing is honored for partial row groups. Harmless today.
        if (columnReaderFactory != null) {
            return new RowGroupReader(
                    fileSchema, projectedSchema, survivor.rowGroup(), options, columnFetcher, columnReaderFactory);
        }
        return new RowGroupReader(reader, fileSchema, projectedSchema, survivor.rowGroup(), options, columnFetcher);
    }

    private void pushEndOfStream() {
        putItem(new EndOfStream<>());
    }

    /**
     * Puts {@code item} on the queue, blocking on backpressure. Propagates cancellation as a sentinel exception so the
     * producer loop can exit cleanly without leaking the item it was trying to enqueue.
     */
    private void putItem(PipelineItem<T> item) {
        if (!putItemSafely(item)) {
            if (item instanceof Prepared<T> prepared) {
                closePrepared(prepared);
            }
            throw new PipelineCancelledException();
        }
    }

    private boolean putItemSafely(PipelineItem<T> item) {
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
     * Empties the queue, closing every {@link Prepared} item so its {@code RowGroupReader} releases pooled buffers.
     * {@link Failure} and {@link EndOfStream} items carry no resources; we just discard them.
     */
    private void drainQueueClosingItems() {
        while (true) {
            PipelineItem<T> item = queue.poll();
            if (item == null) {
                return;
            }
            if (item instanceof Prepared<T> prepared) {
                closePrepared(prepared);
            }
        }
    }

    private static <T> void closePrepared(Prepared<T> prepared) {
        try {
            prepared.stream().close();
        } catch (RuntimeException _) {
            // close() must be best-effort; swallow so we still drain the rest of the queue.
        }
    }

    private static int queueCapacity(int prefetchWindow) {
        return prefetchWindow <= 0 ? 1 : prefetchWindow;
    }

    // --- pipeline item ADT ---

    /**
     * Sentinel pushed by the producer into the bounded queue. The spliterator pattern-matches on the three concrete
     * subtypes to either yield records, propagate an error, or terminate iteration.
     */
    @SuppressWarnings("java:S2326") // T propagates through Prepared<T>(Stream<T>); used by the typed queue.
    sealed interface PipelineItem<T> permits Prepared, Failure, EndOfStream {}

    /**
     * A row group ready for record iteration: the stream is still iterating its assembler, and the underlying
     * {@link RowGroupReader}'s lifecycle is bound to the stream via {@code onClose}.
     */
    record Prepared<T>(Stream<T> stream) implements PipelineItem<T> {}

    /** Producer-side failure: the spliterator rethrows {@code cause} on the consumer thread. */
    record Failure<T>(Throwable cause) implements PipelineItem<T> {}

    /** End-of-stream sentinel: the spliterator returns {@code false} from {@code tryAdvance} after seeing this. */
    record EndOfStream<T>() implements PipelineItem<T> {}

    /**
     * Internal marker thrown by the producer loop when it discovers cancellation has happened while it was blocked on a
     * queue put. Never escapes the pipeline.
     */
    private static final class PipelineCancelledException extends RuntimeException {

        private static final long serialVersionUID = 1L;

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }
}
