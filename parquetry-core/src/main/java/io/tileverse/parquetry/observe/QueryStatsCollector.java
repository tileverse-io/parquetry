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
package io.tileverse.parquetry.observe;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.Tier;

/**
 * Thread-safe {@link QueryObserver} that accumulates one query's stats for the pull report. {@code onRowGroupRead} may
 * arrive concurrently; counters are lock-free adders and the per-tier elimination map is guarded by its own monitor.
 * {@link #snapshot()} is safe to call after {@code onQueryFinished}.
 */
public final class QueryStatsCollector implements QueryObserver {

    private final LongAdder rowsDecoded = new LongAdder();
    private final LongAdder rowsMatched = new LongAdder();
    private final LongAdder pagesDecoded = new LongAdder();
    private final LongAdder pagesPruned = new LongAdder();
    private final LongAdder rowGroupsRead = new LongAdder();

    private final LongAdder pageBytes = new LongAdder();
    private final LongAdder dictionaryBytes = new LongAdder();
    private final LongAdder columnIndexBytes = new LongAdder();
    private final LongAdder offsetIndexBytes = new LongAdder();
    private final LongAdder bloomFilterBytes = new LongAdder();
    private final LongAdder fetchCount = new LongAdder();

    private final Object tierLock = new Object();
    private final EnumMap<Tier, Integer> eliminatedByTier = new EnumMap<>(Tier.class);

    private final AtomicReference<Optional<PhaseTimings>> timings = new AtomicReference<>(Optional.empty());

    private volatile int rowGroupsTotal;
    private volatile long startNanos;
    private volatile long finishNanos;

    @Override
    public void onQueryStarted(QueryStarted event) {
        this.rowGroupsTotal = event.rowGroupCount();
        this.startNanos = System.nanoTime();
    }

    @Override
    public void onRowGroupPlanned(int rowGroup, PruningDecision decision) {
        if (decision instanceof PruningDecision.Eliminated eliminated) {
            synchronized (tierLock) {
                eliminatedByTier.merge(eliminated.tier(), 1, Integer::sum);
            }
        }
    }

    @Override
    public void onRowGroupRead(RowGroupRead event) {
        rowGroupsRead.increment();
        rowsDecoded.add(event.rowsDecoded());
        rowsMatched.add(event.rowsMatched());
        pagesDecoded.add(event.pagesDecoded());
        pagesPruned.add(event.pagesPruned());
        addFetch(event.fetch());
        event.timings().ifPresent(this::addTimings);
    }

    @Override
    public void onQueryFinished(QueryStats stats) {
        this.finishNanos = System.nanoTime();
    }

    public QueryStats snapshot() {
        // reading the volatile finishNanos publishes the startNanos write that preceded it
        long elapsed = Math.max(0L, finishNanos - startNanos);
        return new QueryStats(
                elapsed,
                rowsDecoded.sum(),
                rowsMatched.sum(),
                tierSnapshot(),
                (int) rowGroupsRead.sum(),
                rowGroupsTotal,
                pagesDecoded.sum(),
                pagesPruned.sum(),
                fetchSnapshot(),
                SpillStats.EMPTY,
                timings.get());
    }

    private void addFetch(FetchStats fetch) {
        pageBytes.add(fetch.pageBytes());
        dictionaryBytes.add(fetch.dictionaryBytes());
        columnIndexBytes.add(fetch.columnIndexBytes());
        offsetIndexBytes.add(fetch.offsetIndexBytes());
        bloomFilterBytes.add(fetch.bloomFilterBytes());
        fetchCount.add(fetch.fetchCount());
    }

    private void addTimings(PhaseTimings delta) {
        timings.updateAndGet(current ->
                Optional.of(current.map(existing -> existing.combine(delta)).orElse(delta)));
    }

    private Map<Tier, Integer> tierSnapshot() {
        synchronized (tierLock) {
            return new HashMap<>(eliminatedByTier);
        }
    }

    private FetchStats fetchSnapshot() {
        return new FetchStats(
                pageBytes.sum(),
                dictionaryBytes.sum(),
                columnIndexBytes.sum(),
                offsetIndexBytes.sum(),
                bloomFilterBytes.sum(),
                (int) fetchCount.sum());
    }
}
