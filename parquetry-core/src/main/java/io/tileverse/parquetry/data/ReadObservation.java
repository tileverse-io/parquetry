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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.RowGroupOutcome;
import io.tileverse.parquetry.filter.explain.RowGroupPlan;
import io.tileverse.parquetry.internal.read.ParallelDecodeCoordinator.DecodeObservation;
import io.tileverse.parquetry.observe.PhaseTimings;
import io.tileverse.parquetry.observe.QueryObserver;
import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.observe.QueryStatsCollector;
import io.tileverse.parquetry.observe.SpillAccumulator;

/**
 * The per-query observability binding for a decoding entry point. When an observer is attached it composes an internal
 * {@link QueryStatsCollector} ahead of the user observer (the read runs against the composite, feeding both) and, at
 * query end, stamps the collector's finish, snapshots it, and delivers the aggregate to the user observer exactly once.
 * When no observer is attached it is a pass-through: the original options run unchanged and nothing fires, which keeps
 * the decode path byte-identical.
 */
final class ReadObservation {

    private final ReadOptions effectiveOptions;
    private final QueryStatsCollector internal;
    private final QueryObserver userObserver;
    private final SpillAccumulator spillAccumulator;

    // The query-level filter-pipeline time, recorded once on the reading thread before the stream is consumed and
    // folded into the final stats on the same thread at the finish. Stays zero when timings are off.
    private long pipelineNanos;

    private ReadObservation(
            ReadOptions effectiveOptions,
            QueryStatsCollector internal,
            QueryObserver userObserver,
            SpillAccumulator spillAccumulator) {
        this.effectiveOptions = effectiveOptions;
        this.internal = internal;
        this.userObserver = userObserver;
        this.spillAccumulator = spillAccumulator;
    }

    /**
     * Builds the per-query observability binding for {@code options}. When an observer is attached it composes an
     * internal {@link QueryStatsCollector} ahead of the user observer (the read runs against the composite, feeding
     * both) and, at query end, stamps the collector's finish, snapshots it, and delivers the aggregate to the user
     * observer exactly once. When no observer is attached it is a pass-through: the original options run unchanged and
     * nothing fires, which keeps the decode path byte-identical.
     */
    static ReadObservation observe(ReadOptions options) {
        if (options.queryObserver() == QueryObserver.NONE) {
            return passThrough(options);
        }
        QueryObserver userObserver = options.queryObserver();
        QueryStatsCollector internal = new QueryStatsCollector();
        QueryObserver effectiveObserver = QueryObserver.composite(internal, userObserver);
        ReadOptions effectiveOptions =
                options.toBuilder().queryObserver(effectiveObserver).build();
        return new ReadObservation(effectiveOptions, internal, userObserver, SpillAccumulator.active());
    }

    static ReadObservation passThrough(ReadOptions options) {
        return new ReadObservation(options, null, null, SpillAccumulator.NONE);
    }

    ReadOptions effectiveOptions() {
        return effectiveOptions;
    }

    /** The spill tally for this query, threaded to the decode coordinator and folded into the stats at finish. */
    SpillAccumulator spillAccumulator() {
        return spillAccumulator;
    }

    /** Records the measured filter-pipeline nanoseconds, folded into the final stats at the finish. */
    void addPipelineNanos(long nanos) {
        pipelineNanos += nanos;
    }

    /** Chains the finish onto the stream's close for the lazy {@code read}/{@code readBatches} paths. */
    <T> Stream<T> onClose(Stream<T> stream) {
        if (userObserver == null) {
            return stream;
        }
        return stream.onClose(this::fireFinished);
    }

    /** Fires the finish for the eager {@code count} path; a no-op on the pass-through binding. */
    void fireFinishedIfObserving() {
        if (userObserver != null) {
            fireFinished();
        }
    }

    /** Stamps the internal collector's finish and delivers its snapshot to the user observer exactly once. */
    void fireFinished() {
        internal.onQueryFinished(null);
        QueryStats stats = internal.snapshot().withSpillStats(spillAccumulator.snapshot());
        if (pipelineNanos > 0L) {
            stats = withPipelineNanos(stats, pipelineNanos);
        }
        userObserver.onQueryFinished(stats);
    }

    /** Folds the query-level pipeline time into {@code stats}' cpu timings, creating them when absent. */
    static QueryStats withPipelineNanos(QueryStats stats, long pipelineNanos) {
        PhaseTimings pipeline = new PhaseTimings(pipelineNanos, 0L, 0L, 0L);
        PhaseTimings folded = stats.cpuTimings().map(pipeline::combine).orElse(pipeline);
        return stats.withCpuTimings(Optional.of(folded));
    }

    /**
     * Builds the decode-side observation for the {@code read} and {@code readBatches} paths: the true file ordinal for
     * each survivor, parallel to {@link ParquetFileReader#survivorsFor}'s output (every non-eliminated group). Returns
     * {@link DecodeObservation#NONE} when no observer is attached, which keeps the decode path byte-identical.
     */
    static DecodeObservation decodeObservationFor(
            ExplainPlan plan,
            boolean observe,
            QueryObserver observer,
            boolean matchedEqualsDecoded,
            SpillAccumulator spillAccumulator) {
        if (!observe) {
            return DecodeObservation.NONE;
        }
        List<Integer> indices = new ArrayList<>();
        for (RowGroupPlan rgPlan : plan.rowGroups()) {
            if (rgPlan.outcome() != RowGroupOutcome.ELIMINATED) {
                indices.add(rgPlan.index());
            }
        }
        return new DecodeObservation(
                observer, indices, matchedEqualsDecoded, observer.wantsTimings(), spillAccumulator);
    }

    /**
     * Builds the decode-side observation for the count residual path: the true file ordinal for each FULL and PARTIAL
     * group, parallel to {@link ParquetFileReader#residualSurvivors}'s output (MATCHED groups are excluded, having been
     * reported already by {@link ParquetFileReader#matchedRowCount}). The count path accumulates matches per group,
     * hence {@code matchedEqualsDecoded} is {@code false}.
     */
    static DecodeObservation residualObservationFor(
            ExplainPlan plan, boolean observe, QueryObserver observer, SpillAccumulator spillAccumulator) {
        if (!observe) {
            return DecodeObservation.NONE;
        }
        List<Integer> indices = new ArrayList<>();
        for (RowGroupPlan rgPlan : plan.rowGroups()) {
            switch (rgPlan.outcome()) {
                case ELIMINATED, MATCHED -> {
                    /* eliminated decodes nothing; matched is reported from metadata, never decoded */
                }
                case PARTIAL, FULL -> indices.add(rgPlan.index());
            }
        }
        return new DecodeObservation(observer, indices, false, observer.wantsTimings(), spillAccumulator);
    }
}
