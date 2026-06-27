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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;

import lombok.NonNull;

/**
 * A bounded hand-off of decoded batches from one producing decode worker to the single consuming read thread. The
 * producer {@link #put(HandoffItem)}s items one at a time and parks when the hand-off is full (backpressure), which
 * bounds a single row group's live batches. Each item is a batch held in heap or a handle to one spilled to disk; the
 * consumer drains in order via {@link #hasNext()} / {@link #next()}, restoring a spilled batch when it reaches it.
 *
 * <p>The producer signals end-of-stream with {@link #complete()} or a decode failure with {@link #fail(Throwable)}; the
 * consumer's {@link #hasNext()} returns {@code false} after the last batch, or rethrows the failure. {@link #close()}
 * cancels the hand-off: it discards every item still queued or about to be queued (releasing each one's heap or disk
 * reservation), and a producer parked in {@code put} stops by discarding the item it holds.
 */
final class BatchHandoff implements AutoCloseable {

    private static final Object COMPLETED = new Object();
    private static final long POLL_MILLIS = 50L;

    private final BlockingQueue<Object> queue;
    private final Semaphore slots;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    // Written once by the producer before it enqueues the completion marker, read once by the consumer after it
    // dequeues that marker; the marker's enqueue/dequeue is the happens-before edge that publishes it safely.
    @SuppressWarnings("java:S3077")
    private volatile Throwable failure;

    private HandoffItem lookahead;
    private boolean ended;

    BatchHandoff(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0, got " + capacity);
        }
        this.slots = new Semaphore(capacity);
        // capacity + room for the completion marker, which keeps completion from blocking behind a full queue.
        this.queue = new LinkedBlockingQueue<>(capacity + 1);
    }

    /**
     * Enqueues one item, parking on a permit while the hand-off is full. A permit per queued item, released when the
     * consumer takes one, keeps a waiting producer parked instead of polling the queue's size, which would spin a full
     * decode worker at 100% whenever the consumer is the slower side. The poll interval only bounds how long a parked
     * producer takes to notice cancellation.
     */
    void put(@NonNull HandoffItem item) throws InterruptedException {
        while (!cancelled.get()) {
            if (!slots.tryAcquire(POLL_MILLIS, TimeUnit.MILLISECONDS)) {
                continue;
            }
            if (cancelled.get()) {
                slots.release();
                break;
            }
            // Permits bound queued items at capacity and the physical queue holds capacity + 1, leaving the
            // reserved marker slot; with a permit held this offer cannot fail.
            if (!queue.offer(item)) {
                slots.release();
                throw new IllegalStateException("Hand-off queue rejected an item within its permit bound");
            }
            if (cancelled.get()) {
                drainAndDiscard();
            }
            return;
        }
        discardQuietly(item);
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
        // The taken item no longer occupies the hand-off; freeing its permit unparks a waiting producer.
        slots.release();
        lookahead = (HandoffItem) taken;
        return true;
    }

    ParquetRecordBatch next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more batches in the hand-off");
        }
        HandoffItem item = lookahead;
        lookahead = null;
        return item.take();
    }

    @Override
    public void close() {
        cancelled.set(true);
        drainAndDiscard();
        if (lookahead != null) {
            discardQuietly(lookahead);
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

    private void drainAndDiscard() {
        Object item = queue.poll();
        while (item != null) {
            if (item instanceof HandoffItem handoffItem) {
                discardQuietly(handoffItem);
            }
            item = queue.poll();
        }
    }

    private static void discardQuietly(HandoffItem item) {
        try {
            item.discard();
        } catch (RuntimeException _) {
            // best-effort; a chronic leak shows up in pool / disk accounting
        }
    }
}
