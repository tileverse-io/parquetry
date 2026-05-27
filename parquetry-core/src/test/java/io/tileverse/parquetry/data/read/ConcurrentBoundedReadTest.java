/*
 * Copyright (c) 2026 Tileverse.io
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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.testsupport.CountingSegmentPool;

/**
 * Proves that many concurrent reads sharing one small {@link FetchBudget} never exceed it, never deadlock, and leave
 * the budget fully restored with no pooled-buffer leak.
 *
 * <p>The budget is deliberately smaller than the working set so that budget reservations frequently fail and the serial
 * fallback path is exercised alongside the prefetch path.
 */
class ConcurrentBoundedReadTest {

    @Test
    void concurrentReadsShareABudgetWithoutLeakingOrBreaking(@TempDir Path tmp) throws Exception {
        int rows = 4_000;
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, rows);

        // One small budget shared across all reads, deliberately smaller than the working set so reservations
        // frequently fail and the serial fallback path runs.
        FetchBudget budget = FetchBudget.ofBytes(64 * 1024);
        long capacityBefore = budget.available();
        CountingSegmentPool pool = new CountingSegmentPool();

        int readers = 16;
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        AtomicInteger completed = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();
        for (int r = 0; r < readers; r++) {
            threads.add(Thread.ofVirtual().start(() -> {
                try (ByteRangeSource source = TestParquetFiles.openRangeReader(file)) {
                    ParquetDataset dataset = ParquetDataset.open(source);
                    ReadOptions options = ReadOptions.builder()
                            .segmentPool(pool)
                            .fetchBudget(budget)
                            .prefetchDepth(3)
                            .maxConcurrentFetchesPerRead(4)
                            .build();
                    long count;
                    try (Stream<ParquetRecord> stream = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
                        count = stream.count();
                    }
                    if (count != rows) {
                        failures.add(new AssertionError("expected " + rows + " rows, got " + count));
                    }
                    completed.incrementAndGet();
                } catch (Throwable t) {
                    failures.add(t);
                }
            }));
        }
        for (Thread t : threads) {
            t.join();
        }

        assertThat(failures).as("no reader failed or deadlocked").isEmpty();
        assertThat(completed.get()).isEqualTo(readers);
        assertThat(budget.available())
                .as("every reserved byte is released once all reads finish")
                .isEqualTo(capacityBefore);
        assertThat(pool.outstanding()).as("no pooled buffer leaks").isZero();
    }
}
