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
 * Proves that the multi-range-per-row-group slicing path (rangeIndex > 0) decodes records correctly when the coalesced
 * span is smaller than a row group's column data, forcing the planner to split each row group into multiple ranges.
 */
class MultiRangeCoalescingTest {

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
            RecordingByteRangeSource recording = new RecordingByteRangeSource(base);
            // A span of 1 byte forces every column chunk to be its own range, exercising the multi-range slicing path
            // (ColumnSlice.rangeIndex > 0). Column chunks that fit within the span are still single-range.
            ParquetRuntime runtime = ParquetRuntime.builder()
                    .segmentPool(pool)
                    .maxCoalesceGap(0)
                    .maxCoalescedSpan(1)
                    .build();
            ParquetFileReader dataset = ParquetFileReader.open(recording, runtime, Optional.empty());
            try (Stream<ParquetRecord> stream =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                int before = recording.requestCount();
                stream.forEach(records::add);
                dataReads = recording.requestCount() - before;
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
