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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;

/**
 * Proves that the multi-range-per-row-group slicing path (rangeIndex > 0) decodes records correctly when the coalesced
 * span is smaller than a row group's column data, forcing the planner to split each row group into multiple ranges.
 */
class MultiRangeCoalescingTest {

    /** Delegating {@link ByteRangeSource} that counts how many times {@link #read(long, MemorySegment)} is called. */
    private static final class CountingByteRangeSource implements ByteRangeSource {

        private final ByteRangeSource delegate;
        final AtomicInteger calls = new AtomicInteger();

        CountingByteRangeSource(ByteRangeSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public int read(long offset, MemorySegment dst) {
            calls.incrementAndGet();
            return delegate.read(offset, dst);
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    @Test
    void tinySpanSplitsRowGroupsIntoMultipleRangesAndStillReadsCorrectly(@TempDir Path tmp) throws Exception {
        int rows = 4_000;
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, rows);
        int rowGroups = TestParquetFiles.rowGroupCount(file);
        int columns = 3;

        SegmentPool pool = SegmentPool.create();
        List<ParquetRecord> records = new ArrayList<>();
        int dataReads;
        try (ByteRangeSource base = TestParquetFiles.openRangeReader(file)) {
            CountingByteRangeSource counting = new CountingByteRangeSource(base);
            // A span of 1 byte forces every column chunk to be its own range, exercising the multi-range slicing path
            // (ColumnSlice.rangeIndex > 0). Column chunks that fit within the span are still single-range.
            ParquetRuntime runtime = ParquetRuntime.builder()
                    .segmentPool(pool)
                    .maxCoalesceGap(0)
                    .maxCoalescedSpan(1)
                    .build();
            ParquetFileReader dataset = ParquetFileReader.open(counting, runtime, Optional.empty());
            try (Stream<ParquetRecord> stream =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
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
        assertThat(pool.stats().outstandingBorrows()).isZero();
    }
}
