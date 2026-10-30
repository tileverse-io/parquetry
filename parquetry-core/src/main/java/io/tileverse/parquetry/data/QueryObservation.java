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
package io.tileverse.parquetry.data;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import io.tileverse.parquetry.filter.explain.RowGroupPlan;
import io.tileverse.parquetry.internal.read.ParallelDecodeCoordinator.DecodeObservation;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.observe.FetchStats;
import io.tileverse.parquetry.observe.PhaseTimings;
import io.tileverse.parquetry.observe.QueryObserver;
import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.observe.QueryStatsCollector;
import io.tileverse.parquetry.observe.RowGroupRead;
import io.tileverse.parquetry.observe.SpillAccumulator;

/**
 * The observability of one query. When an observer is attached, an internal {@link QueryStatsCollector} runs ahead of
 * it: the query runs against both, and at its end the observer receives the aggregate exactly once. Without an observer
 * the query runs with its options unchanged and nothing fires, which keeps the decode path byte-identical.
 *
 * <p>The analyze form observes in any case. It also tallies the fetches, including the index sections read by planning,
 * and keeps the read of each row group for the plan returned by {@link ParquetFileReader#explainAnalyze}.
 */
final class QueryObservation {

    private final ReadOptions options;
    private final QueryStatsCollector internal;
    private final QueryObserver userObserver;
    private final FetchAccumulator fetchAccumulator;
    private final SpillAccumulator spillAccumulator;
    private final Map<Integer, RowGroupRead> rowGroupReads;

    // The query-level filter-pipeline time, recorded once on the reading thread before the stream is consumed and
    // folded into the final stats on the same thread at the finish. Stays zero when timings are off.
    private long pipelineNanos;

    private QueryObservation(
            ReadOptions options,
            QueryStatsCollector internal,
            QueryObserver userObserver,
            FetchAccumulator fetchAccumulator,
            SpillAccumulator spillAccumulator,
            Map<Integer, RowGroupRead> rowGroupReads) {
        this.options = options;
        this.internal = internal;
        this.userObserver = userObserver;
        this.fetchAccumulator = fetchAccumulator;
        this.spillAccumulator = spillAccumulator;
        this.rowGroupReads = rowGroupReads;
    }

    /** The observation of a query run with {@code options}, a pass-through when no observer is attached. */
    static QueryObservation observe(ReadOptions options) {
        QueryObserver userObserver = options.queryObserver();
        if (userObserver == QueryObserver.NONE) {
            return new QueryObservation(options, null, null, FetchAccumulator.NONE, SpillAccumulator.NONE, Map.of());
        }
        QueryStatsCollector internal = new QueryStatsCollector();
        ReadOptions effective = withObserver(options, QueryObserver.composite(internal, userObserver));
        return new QueryObservation(
                effective, internal, userObserver, FetchAccumulator.NONE, SpillAccumulator.active(), Map.of());
    }

    /**
     * The observation of the drain run by {@link ParquetFileReader#explainAnalyze}. It observes with or without an
     * observer in {@code options}, tallies the fetches and keeps the read of each row group.
     */
    static QueryObservation analyze(ReadOptions options) {
        QueryObserver userObserver = options.queryObserver();
        QueryStatsCollector internal = new QueryStatsCollector();
        Map<Integer, RowGroupRead> rowGroupReads = new LinkedHashMap<>();
        QueryObserver composite = QueryObserver.composite(internal, capturing(rowGroupReads), userObserver);
        ReadOptions effective = withObserver(options, composite);
        return new QueryObservation(
                effective, internal, userObserver, FetchAccumulator.active(), SpillAccumulator.active(), rowGroupReads);
    }

    private static ReadOptions withObserver(ReadOptions options, QueryObserver observer) {
        return options.toBuilder().queryObserver(observer).build();
    }

    /** An observer recording each row-group read into {@code rowGroupReads}, merging any duplicate by index. */
    private static QueryObserver capturing(Map<Integer, RowGroupRead> rowGroupReads) {
        return new QueryObserver() {
            @Override
            public void onRowGroupRead(RowGroupRead event) {
                rowGroupReads.merge(event.rowGroupIndex(), event, RowGroupRead::combine);
            }
        };
    }

    /** The options the query runs with: when observing, their observer feeds the internal collector too. */
    ReadOptions options() {
        return options;
    }

    /** Whether the query fires its events. */
    boolean observing() {
        return internal != null;
    }

    /** Whether the observers asked for timings; no clock is read for the query otherwise. */
    boolean wantsTimings() {
        return observing() && options.queryObserver().wantsTimings();
    }

    /** The tally of the fetches of the query, {@link FetchAccumulator#NONE} outside the analyze form. */
    FetchAccumulator fetchAccumulator() {
        return fetchAccumulator;
    }

    /** The spill tally for this query, threaded to the decode coordinator and folded into the stats at finish. */
    SpillAccumulator spillAccumulator() {
        return spillAccumulator;
    }

    /** Records the measured filter-pipeline nanoseconds, folded into the final stats at the finish. */
    void addPipelineNanos(long nanos) {
        pipelineNanos += nanos;
    }

    /**
     * Reports the read of a MATCHED row group. Statistics proved each of its rows matches, hence the row group is never
     * decoded, and the decode cannot report it.
     */
    void matchedRowGroup(RowGroupPlan rowGroup) {
        if (!observing()) {
            return;
        }
        RowGroupRead read = new RowGroupRead(
                rowGroup.index(), rowGroup.rowCount(), rowGroup.rowCount(), 0, 0, FetchStats.EMPTY, Optional.empty());
        options.queryObserver().onRowGroupRead(read);
    }

    /**
     * The observation threaded into the decode, or {@link DecodeObservation#NONE} when the query fires no event, which
     * keeps the decode path byte-identical. {@code matchedEqualsDecoded} is {@code true} when each decoded row reaches
     * the output, and {@code false} when the path tallies the rows it emits.
     */
    DecodeObservation decodeObservation(boolean matchedEqualsDecoded) {
        if (!observing()) {
            return DecodeObservation.NONE;
        }
        QueryObserver observer = options.queryObserver();
        return new DecodeObservation(observer, matchedEqualsDecoded, observer.wantsTimings(), spillAccumulator);
    }

    /** Chains the finish onto the stream's close for the lazy {@code read}/{@code readBatches} paths. */
    <T> Stream<T> onClose(Stream<T> stream) {
        if (!observing()) {
            return stream;
        }
        return stream.onClose(this::finish);
    }

    /** Ends the eager {@code count} and {@code bounds} queries; a no-op without an observer. */
    void finishIfObserving() {
        if (observing()) {
            finish();
        }
    }

    /**
     * Stamps the finish of the internal collector, delivers its stats to the observer of the caller exactly once, and
     * returns them.
     */
    QueryStats finish() {
        internal.onQueryFinished(null);
        QueryStats stats = internal.snapshot().withSpillStats(spillAccumulator.snapshot());
        if (fetchAccumulator != FetchAccumulator.NONE) {
            stats = stats.withTotalFetch(fetchAccumulator.snapshot());
        }
        if (pipelineNanos > 0L) {
            stats = withPipelineNanos(stats, pipelineNanos);
        }
        userObserver.onQueryFinished(stats);
        return stats;
    }

    /** The read of each row group, kept by the analyze form and empty otherwise. */
    Map<Integer, RowGroupRead> rowGroupReads() {
        return rowGroupReads;
    }

    /** Folds the query-level pipeline time into {@code stats}' cpu timings, creating them when absent. */
    private static QueryStats withPipelineNanos(QueryStats stats, long pipelineNanos) {
        PhaseTimings pipeline = new PhaseTimings(pipelineNanos, 0L, 0L, 0L);
        PhaseTimings folded = stats.cpuTimings().map(pipeline::combine).orElse(pipeline);
        return stats.withCpuTimings(Optional.of(folded));
    }
}
