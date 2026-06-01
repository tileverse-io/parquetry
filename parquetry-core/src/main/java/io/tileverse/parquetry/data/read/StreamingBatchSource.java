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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import io.tileverse.parquetry.batch.ParquetRecordBatch;

import lombok.NonNull;

/**
 * Drains a worker-fed {@link BatchHandoff} for one speculatively decoded row group. The producer reserves an estimate
 * of the next batch's heap bytes from the {@link DecodeBudget} before decoding it, parking when over budget, then
 * reconciles the reservation to the batch's actual size; it skips the reservation once the row group is exempt.
 * Reserving before the decode keeps a worker from allocating a batch while the budget has no headroom.
 * {@link #promote()} marks the row group exempt and wakes the budget; a parked producer then stops reserving (the
 * deadlock-avoidance rule for the in-order current row group).
 */
final class StreamingBatchSource implements BatchSource {

    private static final long INITIAL_BATCH_ESTIMATE_BYTES = 16L * 1024 * 1024;

    private final BatchHandoff handoff;
    private final RowGroupBatchDriver driver;
    private final DecodeBudget budget;
    private final AtomicBoolean exempt;

    // Created once and reused: the producer reserves on every batch, and a fresh capturing lambda per reservation was a
    // top allocation site under a full scan.
    private final BooleanSupplier giveUp = this::shouldGiveUp;

    // Producer-thread-confined: refined to the last decoded batch's actual heap bytes each iteration.
    private long estimatedBatchBytes = INITIAL_BATCH_ESTIMATE_BYTES;

    // Written once by the coordinator right after submission, read once by the consumer in close(); the volatile
    // reference publishes the immutable Future safely, which is all this field needs.
    @SuppressWarnings("java:S3077")
    private volatile Future<?> producerTask;

    StreamingBatchSource(
            @NonNull BatchHandoff handoff,
            @NonNull RowGroupBatchDriver driver,
            @NonNull DecodeBudget budget,
            boolean exempt) {
        this.handoff = handoff;
        this.driver = driver;
        this.budget = budget;
        this.exempt = new AtomicBoolean(exempt);
    }

    void attachProducerTask(Future<?> producerTask) {
        this.producerTask = producerTask;
    }

    /** Marks this row group's decode as no longer budget-controlled and releases a parked producer. */
    @Override
    public void promote() {
        exempt.set(true);
        budget.wakeWaiters();
    }

    /**
     * The producer loop: reserve an estimate before decoding (the current row group is exempt), decode the batch,
     * reconcile the reservation to the actual size, attach the release, and hand the batch over. Reserving before the
     * decode keeps a speculative worker from allocating a batch while the budget has no headroom, which bounds the
     * concurrently decoding speculative batches to roughly the budget. Runs on a decode worker until the row group is
     * drained, decode fails, or the consumer cancels.
     */
    @SuppressWarnings("java:S1181") // an Error on the worker must reach the consumer, otherwise the hand-off hangs
    void runProducer() {
        try (driver) {
            while (driver.hasMore() && !handoff.isCancelled()) {
                long reserved = reserveBeforeDecode();
                ParquetRecordBatch batch = driver.nextBatch();
                long net = reconcileToActual(reserved, batch.approximateHeapBytes());
                attachRelease(batch, net);
                handoff.put(batch);
            }
            handoff.complete();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            handoff.fail(e);
        } catch (RuntimeException | Error e) {
            handoff.fail(e);
        }
    }

    /** Reserves the running estimate before decode; returns the reserved bytes, or 0 when exempt or aborted. */
    private long reserveBeforeDecode() {
        if (exempt.get()) {
            return 0L;
        }
        long estimate = estimatedBatchBytes;
        boolean reserved = budget.reserve(estimate, giveUp);
        return reserved ? estimate : 0L;
    }

    /** Abort condition for a budget reservation: the row group became exempt, or the consumer cancelled the handoff. */
    private boolean shouldGiveUp() {
        return exempt.get() || handoff.isCancelled();
    }

    /**
     * Refines the estimate to {@code actual} and adjusts the reservation: reserves the deficit when the batch is bigger
     * than estimated, releases the surplus when smaller. Returns the net bytes held for this batch. A deficit reserve
     * is abortable on promotion or cancel, matching the pre-decode reservation; on abort the already-held bytes are
     * kept (and released when the batch closes).
     */
    private long reconcileToActual(long reserved, long actual) {
        estimatedBatchBytes = actual > 0L ? actual : INITIAL_BATCH_ESTIMATE_BYTES;
        if (reserved == 0L) {
            return 0L;
        }
        if (actual > reserved) {
            long deficit = actual - reserved;
            boolean more = budget.reserve(deficit, giveUp);
            return more ? actual : reserved;
        }
        if (actual < reserved) {
            budget.release(reserved - actual);
            return actual;
        }
        return reserved;
    }

    /** Attaches a release of {@code net} reserved bytes that runs once when the consumer closes the batch. */
    private void attachRelease(ParquetRecordBatch batch, long net) {
        if (net <= 0L) {
            return;
        }
        DecodeReservation reservation = new DecodeReservation(budget, net);
        batch.attachReleaseAction(reservation::release);
    }

    @Override
    public boolean hasNext() {
        return handoff.hasNext();
    }

    @Override
    public ParquetRecordBatch next() {
        return handoff.next();
    }

    @Override
    public void close() {
        handoff.close();
        budget.wakeWaiters();
        awaitProducer();
    }

    private void awaitProducer() {
        Future<?> task = producerTask;
        if (task == null) {
            return;
        }
        try {
            task.get();
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException _) {
            // The producer's failure was already delivered to the consumer via the hand-off.
        }
    }
}
