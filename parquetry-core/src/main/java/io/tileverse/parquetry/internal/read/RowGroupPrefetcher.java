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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * {@code prefetchDepth} survivors on a per-read virtual-thread executor, gated by a shared {@link FetchBudget} and a
 * per-read concurrency permit. When the budget or a permit is unavailable a row group is fetched inline on the calling
 * (decode) thread - the never-break serial fallback.
 *
 * <p>The current row group's fetch is mandatory and is never gated by the budget; speculative prefetches are. The
 * per-read concurrency permit is released when a fetch task completes (buffer filled); the budget bytes are released
 * when the resulting {@link RowGroupFetch} is closed (after decode).
 */
public final class RowGroupPrefetcher implements AutoCloseable {

    private final List<RowGroupSurvivor> survivors;
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
            @NonNull RowGroupFetcher fetcher,
            @NonNull FetchBudget budget,
            @NonNull ExecutorService executor,
            int prefetchDepth,
            int maxConcurrentFetchesPerRead) {
        this(survivors, fetcher, budget, executor, prefetchDepth, maxConcurrentFetchesPerRead, false);
    }

    /**
     * Same as the six-argument constructor, additionally timing each fetch when {@code wantsTimings} is on: every
     * {@link RowGroupFetch} then reports its {@code fetchNanos}. When off (the default), no fetch reads the clock.
     */
    @SuppressWarnings("java:S107") // internal prefetcher wiring: the parameters are cohesive collaborators of one read
    public RowGroupPrefetcher(
            @NonNull List<RowGroupSurvivor> survivors,
            @NonNull RowGroupFetcher fetcher,
            @NonNull FetchBudget budget,
            @NonNull ExecutorService executor,
            int prefetchDepth,
            int maxConcurrentFetchesPerRead,
            boolean wantsTimings) {
        this.survivors = List.copyOf(survivors);
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

    /** Returns row group {@code index}, using a completed prefetch if available, otherwise fetching it inline. */
    public RowGroupFetch take(int index) throws IOException {
        submitWindowAhead(index);
        Future<RowGroupFetch> prefetched = window.remove(index);
        if (prefetched != null) {
            return join(prefetched);
        }
        return fetchInline(index);
    }

    private void submitWindowAhead(int currentIndex) {
        int target = Math.min(survivors.size() - 1, currentIndex + prefetchDepth);
        for (int next = Math.max(highestSubmitted + 1, currentIndex + 1); next <= target; next++) {
            trySubmit(next);
        }
    }

    private void trySubmit(int index) {
        FetchPlan plan = fetcher.planFor(survivors.get(index));
        long span = plan.totalBytes();
        if (!budget.tryReserve(span)) {
            return;
        }
        if (!concurrencyPermits.tryAcquire()) {
            budget.release(span);
            return;
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
            return;
        }
        window.put(index, future);
        highestSubmitted = Math.max(highestSubmitted, index);
    }

    private RowGroupFetch fetchInline(int index) throws IOException {
        RowGroupSurvivor survivor = survivors.get(index);
        FetchPlan plan = fetcher.planFor(survivor);
        return fetcher.fetch(survivor, plan, BudgetReservation.NONE, wantsTimings);
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
