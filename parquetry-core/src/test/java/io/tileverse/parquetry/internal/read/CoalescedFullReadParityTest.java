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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.RecordingByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;

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
            RecordingByteRangeSource recording = new RecordingByteRangeSource(base);
            ParquetRuntime runtime = ParquetRuntime.builder().segmentPool(pool).build();
            ParquetFileReader dataset = ParquetFileReader.open(recording, runtime, Optional.empty());
            try (Stream<ParquetRecord> stream =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                // Footer and filter-plan reads have already happened inside read().
                // Counting from here isolates the coalesced data-page fetches only.
                int before = recording.requestCount();
                stream.forEach(records::add);
                dataReads = recording.requestCount() - before;
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
