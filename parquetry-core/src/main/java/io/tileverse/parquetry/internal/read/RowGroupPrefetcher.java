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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;

import io.tileverse.parquetry.runtime.FetchBudget;

import lombok.NonNull;

/**
 * Overlaps upcoming row-group fetches with the current row group's decode. The consumer pulls row groups in order via
 * {@link #take(int)}; before returning row group {@code n} the prefetcher tries to submit fetches for the next
 * {@code prefetchDepth} survivors on a per-read virtual-thread executor, under a shared {@link FetchBudget} and a
 * per-read concurrency permit. When the budget or a permit is unavailable a row group is fetched inline on the calling
 * (decode) thread - the never-break serial fallback.
 *
 * <p>Each row group is planned through the read's {@link RowGroupPlans} at the moment its fetch plan is built, on the
 * consumer thread. A row group dropped by its plan is never fetched: the window records it as submitted and moves on.
 * Every other row group fetches under the plan's fetch mask, which skips the pages outside the planned rows.
 *
 * <p>Speculation is in order and never waits: the window stops at the first row group whose span the budget cannot
 * reserve now, and {@link #tryTake} hands a row group out only when it was prefetched or can be reserved now. The
 * in-order {@link #take} is the one path that may wait. It reserves the span of the current row group when the budget
 * has room now, submits the window ahead, then reads. When the budget has no room it fetches through the mandatory
 * allocator and queues there for budget room; the window ahead is submitted once the row group has its buffer. A
 * waiting read therefore holds no reservation of its own. The per-read concurrency permit is released when a fetch task
 * completes (buffer filled); the budget bytes are released when the resulting {@link RowGroupFetch} is closed (after
 * decode), or when the coordinator {@link #discard discards} a prefetched row group skipped by the consumer.
 */
public final class RowGroupPrefetcher implements AutoCloseable {

    private final List<RowGroupSurvivor> survivors;
    private final RowGroupPlans plans;
    private final RowGroupFetcher fetcher;
    private final FetchBudget budget;
    private final ExecutorService executor;
    private final int prefetchDepth;
    private final Semaphore concurrencyPermits;
    private final boolean wantsTimings;

    private final Map<Integer, Future<RowGroupFetch>> window = new HashMap<>();
    private int highestSubmitted = -1;

    public RowGroupPrefetcher(
            @NonNull List<RowGroupSurvivor> survivors,
            @NonNull RowGroupPlans plans,
            @NonNull RowGroupFetcher fetcher,
            @NonNull FetchBudget budget,
            @NonNull ExecutorService executor,
            int prefetchDepth,
            int maxConcurrentFetchesPerRead) {
        this(survivors, plans, fetcher, budget, executor, prefetchDepth, maxConcurrentFetchesPerRead, false);
    }

    /**
     * Same as the seven-argument constructor, additionally timing each fetch when {@code wantsTimings} is on: every
     * {@link RowGroupFetch} then reports its {@code fetchNanos}. When off (the default), no fetch reads the clock.
     *
     * @param plans the read's per-row-group plans: which survivors are dropped before any fetch, and the fetch-side
     *     page-skip mask of each one, empty for a survivor whose chunks are fetched whole
     */
    @SuppressWarnings("java:S107") // internal prefetcher wiring: the parameters are cohesive collaborators of one read
    public RowGroupPrefetcher(
            @NonNull List<RowGroupSurvivor> survivors,
            @NonNull RowGroupPlans plans,
            @NonNull RowGroupFetcher fetcher,
            @NonNull FetchBudget budget,
            @NonNull ExecutorService executor,
            int prefetchDepth,
            int maxConcurrentFetchesPerRead,
            boolean wantsTimings) {
        if (plans.size() != survivors.size()) {
            throw new IllegalArgumentException("Expected one plan per survivor, got " + plans.size() + " plans for "
                    + survivors.size() + " survivors");
        }
        this.survivors = List.copyOf(survivors);
        this.plans = plans;
        this.fetcher = fetcher;
        this.budget = budget;
        this.executor = executor;
        this.prefetchDepth = prefetchDepth;
        this.concurrencyPermits = new Semaphore(maxConcurrentFetchesPerRead);
        this.wantsTimings = wantsTimings;
    }

    public int size() {
        return survivors.size();
    }

    /**
     * Returns row group {@code index}: a completed prefetch when there is one, otherwise a fetch of its own. That fetch
     * reserves its span when the budget has room now, and otherwise waits for room through the mandatory allocator
     * while the read holds no reservation. The window ahead is submitted while the row group is still in the window, or
     * once it has its reservation or its buffer; a failure while submitting ahead leaves nothing behind.
     */
    public RowGroupFetch take(int index) throws IOException {
        Future<RowGroupFetch> prefetched = window.get(index);
        if (prefetched != null) {
            return takePrefetched(index, prefetched);
        }
        RowGroupSurvivor survivor = survivors.get(index);
        FetchPlan plan = fetcher.planFor(survivor, plans.fetchMask(index));
        Optional<RowGroupFetch> reserved = fetchIfRoom(index, survivor, plan);
        if (reserved.isPresent()) {
            return reserved.orElseThrow();
        }
        return fetchAfterWait(index, survivor, plan);
    }

    /**
     * Returns row group {@code index} without waiting: a completed prefetch when there is one, a fetch reserved up
     * front when the budget has room for its span now, and nothing otherwise. Nothing is fetched or reserved on the
     * empty result; the row group is fetched through {@link #take} when the consumer reaches it.
     */
    public Optional<RowGroupFetch> tryTake(int index) throws IOException {
        Future<RowGroupFetch> prefetched = window.get(index);
        if (prefetched != null) {
            return Optional.of(takePrefetched(index, prefetched));
        }
        RowGroupSurvivor survivor = survivors.get(index);
        FetchPlan plan = fetcher.planFor(survivor, plans.fetchMask(index));
        return fetchIfRoom(index, survivor, plan);
    }

    /**
     * Drops the prefetch of row group {@code index}, if any: waits for its fetch to finish and closes the result,
     * returning its buffer and its budget reservation. Called when the consumer skips a row group without taking it.
     */
    public void discard(int index) {
        Future<RowGroupFetch> prefetched = window.remove(index);
        if (prefetched != null) {
            drainAndClose(prefetched);
        }
    }

    /**
     * Submits the row groups after {@code currentIndex}, in order, and stops at the first one refused by the budget.
     */
    private void submitWindowAhead(int currentIndex) {
        int target = Math.min(survivors.size() - 1, currentIndex + prefetchDepth);
        for (int next = Math.max(highestSubmitted + 1, currentIndex + 1); next <= target; next++) {
            if (plans.dropped(next)) {
                highestSubmitted = Math.max(highestSubmitted, next);
                continue;
            }
            if (!trySubmit(next)) {
                return;
            }
        }
    }

    /** Whether the row group was submitted; a refusal by the budget, the permits or the executor stops the window. */
    private boolean trySubmit(int index) {
        FetchPlan plan = fetcher.planFor(survivors.get(index), plans.fetchMask(index));
        long span = plan.spanBytes();
        if (!budget.tryReserve(span)) {
            return false;
        }
        if (!concurrencyPermits.tryAcquire()) {
            budget.release(span);
            return false;
        }
        BudgetReservation reservation = new BudgetReservation(budget, span);
        RowGroupSurvivor survivor = survivors.get(index);
        Future<RowGroupFetch> future;
        try {
            future = executor.submit(() -> {
                try {
                    return fetcher.fetch(survivor, plan, reservation, wantsTimings);
                } finally {
                    concurrencyPermits.release();
                }
            });
        } catch (RejectedExecutionException _) {
            concurrencyPermits.release();
            reservation.release();
            return false;
        }
        window.put(index, future);
        highestSubmitted = Math.max(highestSubmitted, index);
        return true;
    }

    /**
     * Hands out a prefetched row group. The window ahead is submitted while the row group is still in the window: a
     * planning failure then leaves the prefetch for {@link #close()} to drain.
     */
    private RowGroupFetch takePrefetched(int index, Future<RowGroupFetch> prefetched) throws IOException {
        submitWindowAhead(index);
        window.remove(index);
        return join(prefetched);
    }

    /**
     * Fetches the row group with its span reserved up front, like a prefetch, when the budget has room now; empty when
     * it does not. The window ahead is submitted between the reservation and the read: the speculative fetches then
     * overlap this one. A failure while submitting ahead releases the reservation; the fetch releases it itself if it
     * fails.
     */
    private Optional<RowGroupFetch> fetchIfRoom(int index, RowGroupSurvivor survivor, FetchPlan plan) {
        long span = plan.spanBytes();
        if (!budget.tryReserve(span)) {
            return Optional.empty();
        }
        BudgetReservation reservation = new BudgetReservation(budget, span);
        try {
            submitWindowAhead(index);
        } catch (RuntimeException e) {
            reservation.release();
            throw e;
        }
        return Optional.of(fetcher.fetch(survivor, plan, reservation, wantsTimings));
    }

    /**
     * Fetches the row group through the mandatory allocator and waits there for budget room while the read holds no
     * reservation of its own. The window ahead is submitted once the row group has its buffer; a failure while
     * submitting ahead closes the fetch.
     */
    private RowGroupFetch fetchAfterWait(int index, RowGroupSurvivor survivor, FetchPlan plan) {
        RowGroupFetch fetch = fetcher.fetch(survivor, plan, BudgetReservation.NONE, wantsTimings);
        try {
            submitWindowAhead(index);
        } catch (RuntimeException e) {
            fetch.close();
            throw e;
        }
        return fetch;
    }

    private RowGroupFetch join(Future<RowGroupFetch> future) throws IOException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            throw new IOException("Interrupted while awaiting a prefetched row group", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            if (cause instanceof UncheckedIOException uio) {
                throw uio;
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IOException("Row group prefetch failed", cause);
        }
    }

    /**
     * Drains every still-pending prefetch: waits for each in-flight fetch to finish and closes its result, returning
     * the pooled buffers and releasing the budget reservation. We deliberately do not cancel in-flight reads - a
     * {@code RangeReader} blocked in a non-interruptible read would complete anyway, and a cancelled {@code Future}
     * would orphan the produced {@code RowGroupFetch}, leaking its buffers and budget. Aborting a truly hung read would
     * require {@code RangeReader} interrupt support and is out of scope.
     */
    @Override
    public void close() {
        for (Future<RowGroupFetch> future : window.values()) {
            drainAndClose(future);
        }
        window.clear();
        executor.shutdown();
    }

    private static void drainAndClose(Future<RowGroupFetch> future) {
        try {
            RowGroupFetch fetch = future.get();
            fetch.close();
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException _) {
            // The fetch failed and already released its own buffers and reservation in RowGroupFetcher.fetch.
        }
    }
}
