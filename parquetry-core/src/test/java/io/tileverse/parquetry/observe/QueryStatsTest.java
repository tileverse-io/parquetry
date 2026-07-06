/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.explain.Tier;

class QueryStatsTest {

    @Test
    void combineMergesTierMapsAndSumsCounts() {
        QueryStats a = new QueryStats(
                100,
                1000,
                400,
                Map.of(Tier.STATS, 2),
                3,
                5,
                40,
                4,
                new FetchStats(10, 0, 1, 1, 0, 3),
                SpillStats.EMPTY,
                Optional.empty());
        QueryStats b = new QueryStats(
                50,
                500,
                100,
                Map.of(Tier.STATS, 1, Tier.BLOOM_FILTER, 1),
                2,
                5,
                20,
                2,
                new FetchStats(5, 0, 1, 1, 0, 2),
                SpillStats.EMPTY,
                Optional.empty());

        QueryStats sum = a.combine(b);

        assertThat(sum.wallClockNanos()).isEqualTo(150);
        assertThat(sum.rowsDecoded()).isEqualTo(1500);
        assertThat(sum.rowsMatched()).isEqualTo(500);
        assertThat(sum.rowGroupsEliminatedByTier()).containsEntry(Tier.STATS, 3).containsEntry(Tier.BLOOM_FILTER, 1);
        assertThat(sum.rowGroupsRead()).isEqualTo(5);
        assertThat(sum.rowGroupsTotal()).isEqualTo(10);
        assertThat(sum.pagesDecoded()).isEqualTo(60);
        assertThat(sum.pagesPruned()).isEqualTo(6);
        assertThat(sum.totalFetch()).isEqualTo(new FetchStats(15, 0, 2, 2, 0, 5));
    }

    @Test
    void withTotalFetchReplacesFetchAndPreservesEveryOtherField() {
        PhaseTimings timings = new PhaseTimings(1, 2, 3, 4);
        QueryStats original = new QueryStats(
                100,
                1000,
                400,
                Map.of(Tier.STATS, 2),
                3,
                5,
                40,
                4,
                FetchStats.EMPTY,
                SpillStats.EMPTY,
                Optional.of(timings));

        FetchStats measured = new FetchStats(99, 1, 2, 3, 4, 7);
        QueryStats overridden = original.withTotalFetch(measured);

        assertThat(overridden.totalFetch()).isEqualTo(measured);
        assertThat(overridden.wallClockNanos()).isEqualTo(100);
        assertThat(overridden.rowsDecoded()).isEqualTo(1000);
        assertThat(overridden.rowsMatched()).isEqualTo(400);
        assertThat(overridden.rowGroupsEliminatedByTier()).containsEntry(Tier.STATS, 2);
        assertThat(overridden.rowGroupsRead()).isEqualTo(3);
        assertThat(overridden.rowGroupsTotal()).isEqualTo(5);
        assertThat(overridden.pagesDecoded()).isEqualTo(40);
        assertThat(overridden.pagesPruned()).isEqualTo(4);
        assertThat(overridden.cpuTimings()).contains(timings);
    }

    @Test
    void combineMergesCpuTimings() {
        PhaseTimings t = new PhaseTimings(1, 2, 3, 4);
        QueryStats withTimings =
                new QueryStats(0, 0, 0, Map.of(), 0, 0, 0, 0, FetchStats.EMPTY, SpillStats.EMPTY, Optional.of(t));
        QueryStats withoutTimings =
                new QueryStats(0, 0, 0, Map.of(), 0, 0, 0, 0, FetchStats.EMPTY, SpillStats.EMPTY, Optional.empty());

        assertThat(withTimings.combine(withTimings).cpuTimings()).contains(new PhaseTimings(2, 4, 6, 8));
        assertThat(withTimings.combine(withoutTimings).cpuTimings()).contains(t);
        assertThat(withoutTimings.combine(withTimings).cpuTimings()).contains(t);
    }
}
