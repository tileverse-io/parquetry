/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.BooleanSupplier;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.runtime.DecodeBudget;
import io.tileverse.parquetry.runtime.DiskBudget;

import lombok.NonNull;

/**
 * Drains a worker-fed {@link BatchHandoff} for one speculatively decoded row group. The producer decodes a batch, then
 * tries to reserve its heap bytes from the {@link DecodeBudget}: a batch that fits is handed off in heap with a release
 * attached; a batch that does not fit is spilled to a {@link BatchSpillStore} (reserving the {@link DiskBudget}) and
 * handed off as a handle the consumer restores when it reaches it. The producer parks only as a last resort, when heap
 * and disk are both full. Spilling rather than parking keeps the in-order current row group live without holding more
 * heap than the budget allows; the consumer holds at most one restored batch at a time, the irreducible working set.
 */
final class StreamingBatchSource implements BatchSource {

    private final BatchHandoff handoff;
    private final RowGroupBatchDriver driver;
    private final DecodeBudget budget;
    private final BatchSpillStore spillStore;
    private final boolean spillEnabled;
    private final boolean wantsTimings;

    private final BooleanSupplier giveUp;

    // Written by the producer thread; the consumer reads it only after close() has joined the producer.
    private long decodeNanos;

    // Written once by the coordinator right after submission, read once by the consumer in close().
    @SuppressWarnings("java:S3077")
    private volatile Future<?> producerTask;

    StreamingBatchSource(
            @NonNull BatchHandoff handoff,
            @NonNull RowGroupBatchDriver driver,
            @NonNull DecodeBudget budget,
            @NonNull BatchSpillStore spillStore,
            boolean spillEnabled) {
        this(handoff, driver, budget, spillStore, spillEnabled, false);
    }

    /**
     * Same as the five-argument constructor, additionally timing the producer's driver calls when {@code wantsTimings}
     * is on: the elapsed decode time accumulates into {@link #decodeNanos()}. When off (the default), no clock is read.
     */
    StreamingBatchSource(
            @NonNull BatchHandoff handoff,
            @NonNull RowGroupBatchDriver driver,
            @NonNull DecodeBudget budget,
            @NonNull BatchSpillStore spillStore,
            boolean spillEnabled,
            boolean wantsTimings) {
        this.handoff = handoff;
        this.driver = driver;
        this.budget = budget;
        this.spillStore = spillStore;
        this.spillEnabled = spillEnabled;
        this.wantsTimings = wantsTimings;
        this.giveUp = handoff::isCancelled;
    }

    void attachProducerTask(Future<?> producerTask) {
        this.producerTask = producerTask;
    }

    /**
     * The producer loop: decode a batch, admit it to the hand-off (in heap if it fits the budget, spilled otherwise),
     * and repeat until the row group is drained, decode fails, or the consumer cancels.
     */
    @SuppressWarnings("java:S1181") // an Error on the worker must reach the consumer, otherwise the hand-off hangs
    void runProducer() {
        try (driver) {
            while (hasMoreTimed() && !handoff.isCancelled()) {
                ParquetRecordBatch batch = nextBatchTimed();
                handoff.put(admit(batch));
            }
            handoff.complete();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            handoff.fail(e);
        } catch (RuntimeException | Error e) {
            handoff.fail(e);
        }
    }

    // hasMore is timed too: the masked scan driver computes its first surviving window on the first hasMore call.
    // Only the driver calls are bracketed, never the hand-off, which may park waiting on the consumer.
    private boolean hasMoreTimed() {
        if (!wantsTimings) {
            return driver.hasMore();
        }
        long start = System.nanoTime();
        boolean more = driver.hasMore();
        decodeNanos += System.nanoTime() - start;
        return more;
    }

    private ParquetRecordBatch nextBatchTimed() {
        if (!wantsTimings) {
            return driver.nextBatch();
        }
        long start = System.nanoTime();
        ParquetRecordBatch batch = driver.nextBatch();
        decodeNanos += System.nanoTime() - start;
        return batch;
    }

    /**
     * Reserves the decoded batch's heap bytes; if it fits, returns it in heap. If it does not fit and spill is enabled,
     * spills it to disk and returns a handle. If spill is disabled or disk is full, parks until heap frees, then
     * returns the batch in heap (the last-resort backpressure path).
     */
    private HandoffItem admit(ParquetRecordBatch batch) {
        long bytes = batch.approximateHeapBytes();
        if (budget.tryReserve(bytes)) {
            return inHeap(batch, bytes);
        }
        if (spillEnabled) {
            Optional<SpillHandle> handle = spillStore.trySpill(batch);
            if (handle.isPresent()) {
                batch.close();
                return new HandoffItem.Spilled(handle.get(), spillStore);
            }
        }
        boolean reserved = budget.reserve(bytes, giveUp);
        return inHeap(batch, reserved ? bytes : 0L);
    }

    private HandoffItem inHeap(ParquetRecordBatch batch, long bytes) {
        if (bytes > 0L) {
            DecodeReservation reservation = new DecodeReservation(budget, bytes);
            batch.attachReleaseAction(reservation::release);
        }
        return new HandoffItem.InHeap(batch);
    }

    @Override
    public boolean hasNext() {
        return handoff.hasNext();
    }

    @Override
    public ParquetRecordBatch next() {
        return handoff.next();
    }

    /**
     * The driver's page counts. The consumer reads this only after {@link #close()} has joined the producer, hence the
     * producer-thread writes to the page cursors happen-before this read.
     */
    @Override
    public BatchRowGroupReader.PageCounts pageCounts() {
        return driver.pageCounts();
    }

    /**
     * The driver's decoded-row tally, under the same join as {@link #pageCounts()}: the consumer reads it only after
     * {@link #close()} has awaited the producer.
     */
    @Override
    public long rowsProduced() {
        return driver.rowsProduced();
    }

    /**
     * The producer's decode-time tally, under the same join as {@link #pageCounts()}: the consumer reads it only after
     * {@link #close()} has awaited the producer.
     */
    @Override
    public long decodeNanos() {
        return decodeNanos;
    }

    @Override
    public void close() {
        handoff.close();
        budget.wakeWaiters();
        awaitProducer();
        spillStore.close();
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
