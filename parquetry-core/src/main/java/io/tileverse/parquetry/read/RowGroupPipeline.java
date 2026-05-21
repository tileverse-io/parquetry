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
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.BatchRowAccessor;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Row-API entry point: flat-maps each {@link ParquetRecordBatch} emitted by an underlying {@link BatchRowGroupPipeline}
 * into the caller's {@code T} via the supplied {@link Materializer}.
 *
 * <p>The pipeline is a thin adapter; all prefetch, backpressure, virtual-thread orchestration, and cancellation live in
 * the batch pipeline this class wraps. Each batch arriving from the upstream is iterated row by row through a
 * {@link BatchRowAccessor}; the resulting per-row stream's {@link Stream#onClose} hook closes the batch so the batch
 * Arena and any per-page memory the consumer was still touching are released exactly once per row group worth of work.
 *
 * <h2>Streaming memory contract</h2>
 *
 * <p>Inherits the upstream pipeline's contract verbatim (see the package documentation): at most {@code prefetchWindow}
 * batches resident in the producer's queue, plus the batch currently being iterated by the consumer. The flat-map
 * closes each batch eagerly after its row stream ends, so the consumer never holds more than one open batch at a time.
 *
 * <h2>Cancellation</h2>
 *
 * <p>{@link #close()} delegates to the underlying batch pipeline's {@code close()}, which interrupts the producer,
 * drains queued batches, and joins the producer thread. {@link Stream#close()} on the stream returned by
 * {@link #stream()} cascades to {@link #close()} via the {@code onClose} hook.
 *
 * @param <T> the type returned by the materializer driving the consumer's stream
 */
public final class RowGroupPipeline<T> implements AutoCloseable {

    private final BatchRowGroupPipeline batchPipeline;
    private final Materializer<T> materializer;

    /**
     * Builds a row-API pipeline backed by a fresh {@link BatchRowGroupPipeline} over the same inputs. The batch
     * pipeline is not started until {@link #stream()} is called.
     */
    public RowGroupPipeline(
            @NonNull RangeReader reader,
            @NonNull ParquetSchema fileSchema,
            @NonNull ParquetSchema projectedSchema,
            @NonNull List<RowGroupSurvivor> survivors,
            @NonNull ReadOptions options,
            @NonNull ColumnFetcher columnFetcher,
            @NonNull Materializer<T> materializer) {
        this.batchPipeline =
                new BatchRowGroupPipeline(reader, fileSchema, projectedSchema, survivors, options, columnFetcher);
        this.materializer = materializer;
    }

    /**
     * Returns a closeable stream over every record produced by the underlying batch pipeline. The stream's
     * {@code onClose} hook cancels the pipeline; callers must use try-with-resources to guarantee Arena release and
     * producer thread shutdown. The pipeline is single-use; calling {@code stream()} more than once throws
     * {@link IllegalStateException}.
     */
    @MustBeClosed
    public Stream<T> stream() {
        Stream<ParquetRecordBatch> batches = batchPipeline.stream();
        return batches.flatMap(this::recordsFromBatch).onClose(this::close);
    }

    /**
     * Iterates one batch row-by-row, materializing each row through {@code materializer}, and closes the batch when the
     * row stream is exhausted (or short-circuited). The {@code onClose} hook handles the early-close case: any
     * {@code limit} / {@code findFirst} on the outer stream lets a single batch through, and the hook releases it.
     */
    private Stream<T> recordsFromBatch(ParquetRecordBatch batch) {
        ParquetSchema schema = batch.projectedSchema();
        Stream<T> rows = IntStream.range(0, batch.rowCount())
                .mapToObj(rowIndex -> materializer.materialize(schema, new BatchRowAccessor(batch, rowIndex)));
        return rows.onClose(batch::close);
    }

    /** Cancels the underlying batch pipeline. Idempotent; subsequent calls are no-ops. */
    @Override
    public void close() {
        batchPipeline.close();
    }

    /**
     * True while the underlying batch pipeline's producer thread is alive; useful for tests that assert clean shutdown.
     */
    boolean producerAlive() {
        return batchPipeline.producerAlive();
    }
}
