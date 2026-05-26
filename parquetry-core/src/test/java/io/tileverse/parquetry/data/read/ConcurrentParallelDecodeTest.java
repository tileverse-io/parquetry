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
 * Proves that many concurrent reads sharing one small {@link DecodeExecutor} never deadlock, never leak decode slots,
 * and produce the correct row count.
 *
 * <p>The executor parallelism is deliberately smaller than the number of concurrent readers, so slot contention is high
 * and the inline-fallback path runs frequently alongside the parallel-decode path.
 */
class ConcurrentParallelDecodeTest {

    @Test
    void concurrentReadsShareADecodePoolWithoutLeakingOrBreaking(@TempDir Path tmp) throws Exception {
        int rows = 4_000;
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, rows);

        // Smaller than reader count to ensure contention and inline-fallback coverage.
        DecodeExecutor decodePool = DecodeExecutor.ofParallelism(2);
        CountingSegmentPool pool = new CountingSegmentPool();
        int readers = 12;
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        AtomicInteger completed = new AtomicInteger();
        List<Thread> threads = new ArrayList<>();
        try {
            for (int r = 0; r < readers; r++) {
                threads.add(Thread.ofVirtual().start(() -> {
                    try (ByteRangeSource source = TestParquetFiles.openRangeReader(file)) {
                        ParquetDataset dataset = ParquetDataset.open(source);
                        ReadOptions options = ReadOptions.builder()
                                .segmentPool(pool)
                                .decodeExecutor(decodePool)
                                .maxDecodeAheadPerRead(3)
                                .build();
                        long count;
                        try (Stream<ParquetRecord> stream =
                                dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
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
            assertThat(decodePool.availableSlots())
                    .as("every decode slot is released after all reads finish")
                    .isEqualTo(2);
        } finally {
            decodePool.shutdownNow();
        }

        assertThat(failures).as("no reader failed or deadlocked").isEmpty();
        assertThat(completed.get()).isEqualTo(readers);
        assertThat(pool.outstanding()).as("no pooled buffer leaks").isZero();
    }
}
