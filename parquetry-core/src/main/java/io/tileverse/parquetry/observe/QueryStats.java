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
package io.tileverse.parquetry.observe;

import java.util.EnumMap;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.filter.explain.Tier;

/**
 * Mergeable summary of one query's execution. {@code wallClockNanos} is real elapsed time and is kept separate from any
 * {@link PhaseTimings} CPU sums, which overlap across row groups.
 */
public record QueryStats(
        long wallClockNanos,
        long rowsDecoded,
        long rowsMatched,
        Map<Tier, Integer> rowGroupsEliminatedByTier,
        int rowGroupsRead,
        int rowGroupsTotal,
        long pagesDecoded,
        long pagesPruned,
        FetchStats totalFetch,
        SpillStats spillStats,
        Optional<PhaseTimings> cpuTimings) {

    public QueryStats {
        rowGroupsEliminatedByTier = Map.copyOf(rowGroupsEliminatedByTier);
    }

    /**
     * A copy with {@code totalFetch} replaced and every other field preserved. The analyze path uses this to override
     * the aggregated per-row-group fetch (still zero, as per-group fetch attribution is not yet wired) with the
     * authoritative query-level total measured by the count-style drain.
     */
    public QueryStats withTotalFetch(FetchStats totalFetch) {
        return new QueryStats(
                wallClockNanos,
                rowsDecoded,
                rowsMatched,
                rowGroupsEliminatedByTier,
                rowGroupsRead,
                rowGroupsTotal,
                pagesDecoded,
                pagesPruned,
                totalFetch,
                spillStats,
                cpuTimings);
    }

    /**
     * A copy with {@code spillStats} replaced and every other field preserved. The observed read paths use this to fold
     * the decode-time spill tally - measured by the {@link SpillAccumulator}, not by an observer event - into the stats
     * right before delivering them.
     */
    public QueryStats withSpillStats(SpillStats spillStats) {
        return new QueryStats(
                wallClockNanos,
                rowsDecoded,
                rowsMatched,
                rowGroupsEliminatedByTier,
                rowGroupsRead,
                rowGroupsTotal,
                pagesDecoded,
                pagesPruned,
                totalFetch,
                spillStats,
                cpuTimings);
    }

    /**
     * A copy with {@code cpuTimings} replaced and every other field preserved. The read entry points use this to fold
     * the query-level pipeline timing into the per-row-group sums right before delivering the final stats.
     */
    public QueryStats withCpuTimings(Optional<PhaseTimings> cpuTimings) {
        return new QueryStats(
                wallClockNanos,
                rowsDecoded,
                rowsMatched,
                rowGroupsEliminatedByTier,
                rowGroupsRead,
                rowGroupsTotal,
                pagesDecoded,
                pagesPruned,
                totalFetch,
                spillStats,
                cpuTimings);
    }

    public QueryStats combine(QueryStats other) {
        return new QueryStats(
                wallClockNanos + other.wallClockNanos,
                rowsDecoded + other.rowsDecoded,
                rowsMatched + other.rowsMatched,
                mergeTierCounts(other.rowGroupsEliminatedByTier),
                rowGroupsRead + other.rowGroupsRead,
                rowGroupsTotal + other.rowGroupsTotal,
                pagesDecoded + other.pagesDecoded,
                pagesPruned + other.pagesPruned,
                totalFetch.combine(other.totalFetch),
                spillStats.combine(other.spillStats),
                combineTimings(other.cpuTimings));
    }

    private Map<Tier, Integer> mergeTierCounts(Map<Tier, Integer> other) {
        EnumMap<Tier, Integer> merged = new EnumMap<>(Tier.class);
        merged.putAll(rowGroupsEliminatedByTier);
        other.forEach((tier, count) -> merged.merge(tier, count, Integer::sum));
        return merged;
    }

    private Optional<PhaseTimings> combineTimings(Optional<PhaseTimings> other) {
        if (cpuTimings.isPresent() && other.isPresent()) {
            return Optional.of(cpuTimings.get().combine(other.get()));
        }
        if (cpuTimings.isPresent()) {
            return cpuTimings;
        }
        return other;
    }
}
