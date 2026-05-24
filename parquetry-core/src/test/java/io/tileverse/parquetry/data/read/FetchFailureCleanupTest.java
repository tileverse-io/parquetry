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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
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
 * Proves that a mid-stream fetch failure releases the fetch budget and returns all borrowed buffers to the pool - i.e.
 * a failure in prefetch does not leak resources.
 */
class FetchFailureCleanupTest {

    /**
     * Delegates all reads to the wrapped reader until armed, then throws on the second {@link #readRange} call after
     * arming. Letting the first armed read succeed allows the prefetcher to start a row-group fetch before failing on
     * the next one, exercising the mid-stream cleanup path.
     */
    private static final class FailAfterArmRangeReader implements RangeReader {

        private final RangeReader delegate;
        private volatile boolean armed = false;
        private final AtomicInteger armedReads = new AtomicInteger();

        FailAfterArmRangeReader(RangeReader delegate) {
            this.delegate = delegate;
        }

        void arm() {
            armed = true;
        }

        @Override
        public int readRange(long offset, int length, ByteBuffer target) {
            if (armed && armedReads.incrementAndGet() == 2) {
                throw new UncheckedIOException(new IOException("injected fetch failure"));
            }
            return delegate.readRange(offset, length, target);
        }

        @Override
        public OptionalLong size() {
            return delegate.size();
        }

        @Override
        public String getSourceIdentifier() {
            return delegate.getSourceIdentifier();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    @Test
    void fetchFailureMidStreamReleasesBudgetAndBuffers(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, 4_000);
        ByteBufferPool pool = new ByteBufferPool();
        FetchBudget budget = FetchBudget.ofMaxMemoryFraction(0.1);
        long capacityBefore = budget.available();

        try (RangeReader base = TestParquetFiles.openRangeReader(file)) {
            FailAfterArmRangeReader failing = new FailAfterArmRangeReader(base);
            ParquetDataset dataset = ParquetDataset.open(failing);
            ReadOptions options = ReadOptions.builder()
                    .byteBufferPool(pool)
                    .fetchBudget(budget)
                    .prefetchDepth(2)
                    .build();
            try (Stream<ParquetRecord> stream = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
                failing.arm();
                assertThatThrownBy(() -> stream.forEach(r -> {}))
                        .isInstanceOf(UncheckedIOException.class)
                        .hasRootCauseInstanceOf(IOException.class);
            }
        }

        assertThat(budget.available())
                .as("budget fully restored after a mid-stream fetch failure")
                .isEqualTo(capacityBefore);
        assertThat(outstandingBorrows(pool))
                .as("all pooled buffers returned after a mid-stream fetch failure")
                .isZero();
    }

    private static long outstandingBorrows(ByteBufferPool pool) {
        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        return (stats.created() + stats.reused()) - (stats.returned() + stats.discarded());
    }
}
