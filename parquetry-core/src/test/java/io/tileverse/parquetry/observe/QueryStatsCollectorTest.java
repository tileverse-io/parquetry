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

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.Tier;

class QueryStatsCollectorTest {

    @Test
    void aggregatesPlannedEliminationsByTier() {
        QueryStatsCollector collector = new QueryStatsCollector();
        collector.onQueryStarted(new QueryStarted(Predicate.ALWAYS_TRUE, null, 4));

        collector.onRowGroupPlanned(0, new PruningDecision.Eliminated(Tier.STATS, "min>max"));
        collector.onRowGroupPlanned(1, new PruningDecision.Eliminated(Tier.STATS, "min>max"));
        collector.onRowGroupPlanned(2, new PruningDecision.Eliminated(Tier.BLOOM_FILTER, "absent"));
        collector.onRowGroupPlanned(3, new PruningDecision.PassedAll(Tier.STATS, "ok"));
        collector.onQueryFinished(null);

        QueryStats stats = collector.snapshot();

        assertThat(stats.rowGroupsTotal()).isEqualTo(4);
        assertThat(stats.rowGroupsEliminatedByTier())
                .containsEntry(Tier.STATS, 2)
                .containsEntry(Tier.BLOOM_FILTER, 1);
    }

    @Test
    void aggregatesConcurrentRowGroupReads() throws InterruptedException {
        QueryStatsCollector collector = new QueryStatsCollector();
        collector.onQueryStarted(new QueryStarted(Predicate.ALWAYS_TRUE, null, 100));

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int i = 0; i < threads; i++) {
            final int index = i;
            pool.submit(() -> {
                await(start);
                collector.onRowGroupRead(
                        new RowGroupRead(index, 10, 4, 2, 1, new FetchStats(100, 0, 0, 0, 0, 1), Optional.empty()));
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        QueryStats stats = collector.snapshot();

        assertThat(stats.rowGroupsRead()).isEqualTo(threads);
        assertThat(stats.rowsDecoded()).isEqualTo(10L * threads);
        assertThat(stats.rowsMatched()).isEqualTo(4L * threads);
        assertThat(stats.pagesDecoded()).isEqualTo(2L * threads);
        assertThat(stats.pagesPruned()).isEqualTo(1L * threads);
        assertThat(stats.totalFetch().pageBytes()).isEqualTo(100L * threads);
        assertThat(stats.totalFetch().fetchCount()).isEqualTo(threads);
    }

    @Test
    void aggregatesConcurrentTimings() throws InterruptedException {
        QueryStatsCollector collector = new QueryStatsCollector();
        collector.onQueryStarted(new QueryStarted(Predicate.ALWAYS_TRUE, null, 8));

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        for (int i = 0; i < threads; i++) {
            final int index = i;
            pool.submit(() -> {
                await(start);
                collector.onRowGroupRead(new RowGroupRead(
                        index, 0, 0, 0, 0, FetchStats.EMPTY, Optional.of(new PhaseTimings(1, 2, 3, 4))));
            });
        }
        start.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        assertThat(collector.snapshot().cpuTimings())
                .contains(new PhaseTimings(threads, 2L * threads, 3L * threads, 4L * threads));
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
