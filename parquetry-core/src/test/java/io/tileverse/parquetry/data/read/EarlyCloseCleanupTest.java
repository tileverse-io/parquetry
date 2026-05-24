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
import java.util.Iterator;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;

import io.tileverse.io.ByteBufferPool;

/**
 * Proves that closing a read stream after consuming only one record releases all prefetched-but-unconsumed buffers and
 * restores the fetch budget - i.e. early termination (the common {@code limit}/{@code findFirst}/break pattern) does
 * not leak.
 *
 * <p>This directly exercises the prefetcher's close/drain path.
 */
class EarlyCloseCleanupTest {

    @Test
    void closingEarlyReleasesPrefetchedBuffersAndBudget(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, 4_000);
        ByteBufferPool pool = new ByteBufferPool();
        FetchBudget budget = FetchBudget.ofMaxMemoryFraction(0.1);
        long capacityBefore = budget.available();

        try (RangeReader reader = TestParquetFiles.openRangeReader(file)) {
            ParquetDataset dataset = ParquetDataset.open(reader);
            ReadOptions options = ReadOptions.builder()
                    .byteBufferPool(pool)
                    .fetchBudget(budget)
                    .prefetchDepth(4)
                    .build();
            try (Stream<ParquetRecord> stream = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
                Iterator<ParquetRecord> it = stream.iterator();
                // Consume just one record, then close the stream via try-with-resources without draining it.
                assertThat(it.hasNext()).isTrue();
                it.next();
            }
        }

        assertThat(budget.available())
                .as("prefetched-but-unconsumed row groups release their reserved bytes on early close")
                .isEqualTo(capacityBefore);
        assertThat(outstandingBorrows(pool))
                .as("prefetched-but-unconsumed buffers are returned on early close")
                .isZero();
    }

    private static long outstandingBorrows(ByteBufferPool pool) {
        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        return (stats.created() + stats.reused()) - (stats.returned() + stats.discarded());
    }
}
