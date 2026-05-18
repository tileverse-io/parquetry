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
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

/**
 * Drains row-group results pushed by {@link RowGroupPipeline} and yields materialized records to the caller's
 * {@link java.util.stream.Stream}.
 *
 * <p>The spliterator walks the pipeline's bounded queue of {@link RowGroupPipeline.PipelineItem} values: a
 * {@link RowGroupPipeline.Prepared} carries an already-fetched {@code RowGroupReader} plus its stream of records, a
 * {@link RowGroupPipeline.Failure} carries a producer-side error that we rethrow on the consumer thread, and
 * {@link RowGroupPipeline.EndOfStream} marks the end of input. The spliterator drives the active row group's iterator
 * to exhaustion (one record per {@link #tryAdvance(Consumer)} call) before pulling the next pipeline item; when a row
 * group exhausts its underlying {@link RowGroupReader#close()} is called via the wrapped stream's {@code onClose} hook,
 * which returns every borrowed pooled buffer to the pool.
 *
 * <h2>Characteristics</h2>
 *
 * <p>{@link Spliterator#ORDERED} (row groups are consumed in the order the pipeline produced them, and within a row
 * group records flow in assembly order) and {@link Spliterator#NONNULL} (the materializer is expected to return
 * non-null values; nulls inside record fields are represented by the {@code ParquetRecord} ADT). NOT
 * {@link Spliterator#SIZED}: without record-level pruning we cannot promise the total record count is the sum of row
 * group {@code numRows}, and even with it the filter pipeline narrows after assembly. {@link #trySplit()} returns
 * {@code null}: parallelism is provided by the pipeline's prefetch, not by {@code Stream.parallel()}.
 */
public final class ParquetRecordSpliterator<T> implements Spliterator<T> {

    private final BlockingQueue<RowGroupPipeline.PipelineItem<T>> queue;
    private RowGroupPipeline.Prepared<T> active;
    private Iterator<T> activeIterator;
    private boolean exhausted;

    ParquetRecordSpliterator(BlockingQueue<RowGroupPipeline.PipelineItem<T>> queue) {
        this.queue = Objects.requireNonNull(queue, "queue");
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        Objects.requireNonNull(action, "action");
        if (exhausted) {
            return false;
        }
        while (true) {
            if (activeIterator != null && activeIterator.hasNext()) {
                action.accept(activeIterator.next());
                return true;
            }
            closeActive();
            if (!pullNext()) {
                return false;
            }
        }
    }

    @Override
    public Spliterator<T> trySplit() {
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
     * Closes the active row group (returning its borrowed pooled buffers) and marks the spliterator exhausted so any
     * subsequent {@code tryAdvance} returns {@code false} without blocking on the queue. Called from the pipeline's own
     * {@code close()} via the stream's {@code onClose} hook.
     */
    void releaseActive() {
        exhausted = true;
        closeActive();
    }

    /**
     * Pulls the next pipeline item, blocking until the producer pushes one. Returns {@code true} when a fresh row group
     * is active and ready to yield records; {@code false} after the end-of-stream sentinel arrives.
     */
    private boolean pullNext() {
        RowGroupPipeline.PipelineItem<T> next = takeBlocking();
        return switch (next) {
            case RowGroupPipeline.Prepared<T> prepared -> activate(prepared);
            case RowGroupPipeline.Failure<T> failure -> rethrow(failure);
            case RowGroupPipeline.EndOfStream<T> _ -> finish();
        };
    }

    private boolean activate(RowGroupPipeline.Prepared<T> prepared) {
        active = prepared;
        activeIterator = prepared.stream().iterator();
        return true;
    }

    private boolean rethrow(RowGroupPipeline.Failure<T> failure) {
        exhausted = true;
        Throwable cause = failure.cause();
        if (cause instanceof RuntimeException re) {
            throw re;
        }
        if (cause instanceof IOException ioe) {
            throw new UncheckedIOException("Row-group pipeline failed", ioe);
        }
        throw new UncheckedIOException("Row-group pipeline failed", new IOException(cause));
    }

    private boolean finish() {
        exhausted = true;
        return false;
    }

    /**
     * Closes the active row group's stream (which cascades to its {@code RowGroupReader} via {@code onClose}) so
     * borrowed pooled buffers return to the pool. Idempotent and safe to call before any row group has been activated.
     */
    private void closeActive() {
        if (active == null) {
            return;
        }
        RowGroupPipeline.Prepared<T> toClose = active;
        active = null;
        activeIterator = null;
        toClose.stream().close();
    }

    /**
     * Blocking take on the queue that converts an {@code InterruptedException} into an
     * {@link UncheckedIOException}-friendly runtime path: we restore the interrupt flag and throw so the consumer's
     * stream iteration ends with a cause it can reason about. The pipeline owns the only producer side of the queue, so
     * the only way we get interrupted here is the consumer thread itself being interrupted.
     */
    private RowGroupPipeline.PipelineItem<T> takeBlocking() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exhausted = true;
            throw new IllegalStateException("Interrupted while waiting for next row group", e);
        }
    }
}
