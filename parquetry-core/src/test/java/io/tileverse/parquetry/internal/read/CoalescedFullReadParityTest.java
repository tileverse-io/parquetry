/*
 * Copyright (c) 2026 Multivers.io
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
import io.tileverse.parquetry.data.ParquetRuntime;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.record.ParquetRecord;

/**
 * Proves two properties of the coalesced+prefetched read path:
 *
 * <ol>
 *   <li>A full read returns the correct number of records (parity with the written data).
 *   <li>The number of {@link ByteRangeSource#read} calls during data-page fetching is significantly fewer than one call
 *       per column per row group, confirming that column chunks within a row group are coalesced into a single range
 *       read.
 * </ol>
 */
class CoalescedFullReadParityTest {

    /**
     * Delegating {@link ByteRangeSource} that counts how many times {@link #read(long, MemorySegment)} is called. Uses
     * no mocking framework - just a plain wrapper.
     */
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
    void fullReadIsCorrectAndCoalescesRangeReads(@TempDir Path tmp) throws Exception {
        int rows = 4_000;
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, rows);
        int rowGroups = TestParquetFiles.rowGroupCount(file);
        int columns = 3;

        SegmentPool pool = SegmentPool.create();
        List<ParquetRecord> records = new ArrayList<>();
        int dataReads;
        try (ByteRangeSource base = TestParquetFiles.openRangeReader(file)) {
            CountingByteRangeSource counting = new CountingByteRangeSource(base);
            ParquetRuntime runtime = ParquetRuntime.builder().segmentPool(pool).build();
            ParquetFileReader dataset = ParquetFileReader.open(counting, runtime, Optional.empty());
            try (Stream<ParquetRecord> stream =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                // Footer and filter-plan reads have already happened inside read().
                // Counting from here isolates the coalesced data-page fetches only.
                int before = counting.calls.get();
                stream.forEach(records::add);
                dataReads = counting.calls.get() - before;
            }
        }

        assertThat(records).hasSize(rows);
        assertThat(rowGroups).as("fixture must span multiple row groups").isGreaterThan(1);
        assertThat(dataReads)
                .as(
                        "each row group's columns coalesce into about one range read, far below %d cols x %d row groups",
                        columns, rowGroups)
                .isLessThanOrEqualTo(rowGroups)
                .isLessThan(columns * rowGroups);
        assertThat(pool.stats().outstandingBorrows()).isZero();
    }
}
