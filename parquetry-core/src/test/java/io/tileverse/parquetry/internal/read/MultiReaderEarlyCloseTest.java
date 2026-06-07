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
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.FilesetReader;
import io.tileverse.parquetry.data.OpenOptions;
import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ParquetRuntime;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.testsupport.CountingSegmentPool;

/**
 * Proves that early-terminating a read over a multi-file dataset drains the segment pool across every reader. A
 * multi-file {@link ParquetDataset} stitches the per-file reads with {@code flatMap}; consuming rows from a later file
 * and then closing the outer stream must close both the exhausted earlier inner streams and the in-flight current one,
 * leaving no borrowed buffers outstanding and the fetch budget fully restored.
 */
class MultiReaderEarlyCloseTest {

    @Test
    void earlyCloseAcrossReadersDrainsPoolAndBudget(@TempDir Path tmp) throws Exception {
        int rowsPerFile = 4_000;
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, rowsPerFile);
        CountingSegmentPool pool = new CountingSegmentPool();
        FetchBudget budget = FetchBudget.ofMaxMemoryFraction(0.1);
        long capacityBefore = budget.available();

        try (ByteRangeSource first = TestParquetFiles.openRangeReader(file);
                ByteRangeSource second = TestParquetFiles.openRangeReader(file)) {

            OpenOptions openOptions = OpenOptions.builder()
                    .runtime(ParquetRuntime.builder()
                            .segmentPool(pool)
                            .fetchBudget(budget)
                            .prefetchDepth(4)
                            .build())
                    .build();
            ParquetDataset dataset = ParquetDataset.open(twoFileFileset(first, second), openOptions);

            try (Stream<ParquetRecord> stream =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                Iterator<ParquetRecord> it = stream.iterator();
                // Exhaust the first reader and step one row into the second, then close without draining the rest.
                for (int consumed = 0; consumed <= rowsPerFile; consumed++) {
                    assertThat(it.hasNext()).isTrue();
                    it.next();
                }
            }
        }

        assertThat(pool.outstanding())
                .as("buffers borrowed across both readers are returned on early close")
                .isZero();
        assertThat(budget.available())
                .as("reserved fetch budget is restored across both readers on early close")
                .isEqualTo(capacityBefore);
    }

    private static FilesetReader twoFileFileset(ByteRangeSource first, ByteRangeSource second) {
        List<ByteRangeSource> sources = List.of(first, second);
        return new FilesetReader() {
            @Override
            public ByteRangeSource openFile(int index) {
                return sources.get(index);
            }

            @Override
            public int fileCount() {
                return sources.size();
            }
        };
    }
}
