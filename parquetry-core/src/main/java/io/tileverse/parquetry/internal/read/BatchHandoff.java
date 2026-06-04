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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.tileverse.parquetry.batch.ParquetRecordBatch;

import lombok.NonNull;

/**
 * A bounded hand-off of decoded batches from one producing decode worker to the single consuming read thread. The
 * producer {@link #put(ParquetRecordBatch)}s batches one at a time and parks when the hand-off is full (backpressure),
 * which bounds a single row group's live batches. The consumer drains in order via {@link #hasNext()} /
 * {@link #next()}.
 *
 * <p>The producer signals end-of-stream with {@link #complete()} or a decode failure with {@link #fail(Throwable)}; the
 * consumer's {@link #hasNext()} returns {@code false} after the last batch, or rethrows the failure. {@link #close()}
 * cancels the hand-off: it closes every batch still queued or about to be queued (releasing each one's budget
 * reservation), and a producer parked in {@code put} stops by closing the batch it holds.
 */
final class BatchHandoff implements AutoCloseable {

    private static final Object COMPLETED = new Object();
    private static final long POLL_MILLIS = 50L;

    private final BlockingQueue<Object> queue;
    private final int capacity;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    // Written once by the producer before it enqueues the completion marker, read once by the consumer after it
    // dequeues that marker; the marker's enqueue/dequeue is the happens-before edge that publishes it safely.
    @SuppressWarnings("java:S3077")
    private volatile Throwable failure;

    private ParquetRecordBatch lookahead;
    private boolean ended;

    BatchHandoff(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0, got " + capacity);
        }
        this.capacity = capacity;
        // capacity + room for the completion marker, which keeps completion from blocking behind a full queue.
        this.queue = new LinkedBlockingQueue<>(capacity + 1);
    }

    void put(@NonNull ParquetRecordBatch batch) throws InterruptedException {
        while (!cancelled.get()) {
            if (queue.size() < capacity && queue.offer(batch, POLL_MILLIS, TimeUnit.MILLISECONDS)) {
                if (cancelled.get()) {
                    drainAndClose();
                }
                return;
            }
        }
        closeQuietly(batch);
    }

    void complete() {
        signalEndOfStream();
    }

    void fail(@NonNull Throwable cause) {
        this.failure = cause;
        signalEndOfStream();
    }

    private void signalEndOfStream() {
        // The backing queue reserves a dedicated slot for the single completion marker beyond capacity, which means
        // this offer always succeeds and never blocks behind queued batches.
        boolean enqueued = queue.offer(COMPLETED);
        if (!enqueued) {
            throw new IllegalStateException("Completion marker slot was unexpectedly full");
        }
    }

    boolean isCancelled() {
        return cancelled.get();
    }

    boolean hasNext() {
        if (lookahead != null) {
            return true;
        }
        if (ended) {
            return false;
        }
        Object taken = take();
        if (taken == COMPLETED) {
            ended = true;
            rethrowFailure();
            return false;
        }
        lookahead = (ParquetRecordBatch) taken;
        return true;
    }

    ParquetRecordBatch next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more batches in the hand-off");
        }
        ParquetRecordBatch batch = lookahead;
        lookahead = null;
        return batch;
    }

    @Override
    public void close() {
        cancelled.set(true);
        drainAndClose();
        if (lookahead != null) {
            closeQuietly(lookahead);
            lookahead = null;
        }
    }

    private Object take() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while awaiting a decoded batch", e);
        }
    }

    private void rethrowFailure() {
        Throwable cause = failure;
        if (cause == null) {
            return;
        }
        if (cause instanceof RuntimeException re) {
            throw re;
        }
        if (cause instanceof IOException io) {
            throw new UncheckedIOException("Row group decode failed", io);
        }
        if (cause instanceof Error error) {
            throw error;
        }
        throw new IllegalStateException("Row group decode failed", cause);
    }

    private void drainAndClose() {
        Object item = queue.poll();
        while (item != null) {
            if (item instanceof ParquetRecordBatch batch) {
                closeQuietly(batch);
            }
            item = queue.poll();
        }
    }

    private static void closeQuietly(ParquetRecordBatch batch) {
        try {
            batch.close();
        } catch (RuntimeException _) {
            // best-effort; a chronic leak shows up in pool accounting
        }
    }
}
