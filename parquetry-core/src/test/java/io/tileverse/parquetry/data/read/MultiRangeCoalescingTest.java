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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
 * Proves that the multi-range-per-row-group slicing path (rangeIndex > 0) decodes records correctly when the coalesced
 * span is smaller than a row group's column data, forcing the planner to split each row group into multiple ranges.
 */
class MultiRangeCoalescingTest {

    /**
     * Delegating {@link RangeReader} that counts how many times {@link #readRange(long, int, ByteBuffer)} is called.
     */
    private static final class CountingRangeReader implements RangeReader {

        private final RangeReader delegate;
        final AtomicInteger calls = new AtomicInteger();

        CountingRangeReader(RangeReader delegate) {
            this.delegate = delegate;
        }

        @Override
        public int readRange(long offset, int length, ByteBuffer target) {
            calls.incrementAndGet();
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
    void tinySpanSplitsRowGroupsIntoMultipleRangesAndStillReadsCorrectly(@TempDir Path tmp) throws Exception {
        int rows = 4_000;
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, rows);
        int rowGroups = TestParquetFiles.rowGroupCount(file);
        int columns = 3;

        ByteBufferPool pool = new ByteBufferPool();
        List<ParquetRecord> records = new ArrayList<>();
        int dataReads;
        try (RangeReader base = TestParquetFiles.openRangeReader(file)) {
            CountingRangeReader counting = new CountingRangeReader(base);
            ParquetDataset dataset = ParquetDataset.open(counting);
            // A span of 1 byte forces every column chunk to be its own range, exercising the multi-range slicing path
            // (ColumnSlice.rangeIndex > 0). Column chunks that fit within the span are still single-range.
            ReadOptions options = ReadOptions.builder()
                    .byteBufferPool(pool)
                    .maxCoalesceGap(0)
                    .maxCoalescedSpan(1)
                    .build();
            try (Stream<ParquetRecord> stream = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
                int before = counting.calls.get();
                stream.forEach(records::add);
                dataReads = counting.calls.get() - before;
            }
        }

        assertThat(records).hasSize(rows);
        assertThat(dataReads)
                .as(
                        "a tiny coalesced span splits each row group into more than one range"
                                + " (actual dataReads=%d, rowGroups=%d, columns=%d)",
                        dataReads, rowGroups, columns)
                .isGreaterThan(rowGroups)
                .isLessThanOrEqualTo(columns * rowGroups);
        assertThat(outstandingBorrows(pool)).isZero();
    }

    private static long outstandingBorrows(ByteBufferPool pool) {
        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        return (stats.created() + stats.reused()) - (stats.returned() + stats.discarded());
    }
}
