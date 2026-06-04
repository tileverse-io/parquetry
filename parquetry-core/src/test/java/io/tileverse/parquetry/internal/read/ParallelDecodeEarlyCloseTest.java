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

import java.nio.file.Path;
import java.util.Iterator;
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
 * Proves that closing a parallel-decode stream early - after consuming just one record - drains any in-flight decode
 * futures, releases every decode slot, and returns every prefetched buffer to the pool.
 */
class ParallelDecodeEarlyCloseTest {

    @Test
    void closingEarlyDrainsInFlightDecodesWithoutLeaks(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, 4_000);
        DecodeExecutor decodePool = DecodeExecutor.ofParallelism(4);
        CountingSegmentPool pool = new CountingSegmentPool();
        try (ByteRangeSource source = TestParquetFiles.openRangeReader(file)) {
            ParquetDataset dataset = ParquetDataset.open(source);
            ReadOptions options = ReadOptions.builder()
                    .segmentPool(pool)
                    .decodeExecutor(decodePool)
                    .maxDecodeAheadPerRead(4)
                    .build();
            try (Stream<ParquetRecord> stream = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
                Iterator<ParquetRecord> it = stream.iterator();
                assertThat(it.hasNext()).isTrue();
                it.next(); // consume one record, then close without draining
            }
        }
        assertThat(decodePool.availableSlots())
                .as("all decode slots released after early close")
                .isEqualTo(4);
        assertThat(pool.outstanding())
                .as("prefetched/decoded-but-unconsumed buffers returned on early close")
                .isZero();
        decodePool.shutdownNow();
    }
}
