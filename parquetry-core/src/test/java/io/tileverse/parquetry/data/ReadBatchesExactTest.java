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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.internal.read.TestParquetFiles;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Coverage for {@link ParquetFileReader#readBatches} applying the record filter exactly: each emitted batch holds only
 * matching rows, narrowed to the caller's projection, and no empty batch is emitted.
 */
class ReadBatchesExactTest {

    private static final int ROW_COUNT = 4_000;

    @Test
    void readBatchesAppliesTheRecordFilter(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROW_COUNT);
        Predicate predicate = Pred.col("year").eq(2022);

        try (ByteRangeSource source = TestParquetFiles.openRangeReader(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            long expected = reader.count(predicate, ReadOptions.DEFAULTS);

            long emitted = sumBatchRowCounts(reader, predicate, Projection.ALL);

            assertThat(emitted).as("rows emitted match the exact count").isEqualTo(expected);
            assertThat(emitted)
                    .as("the filter drops some of the %d rows", ROW_COUNT)
                    .isLessThan(ROW_COUNT);
        }
    }

    @Test
    void readBatchesEmitsEveryRowWhenAllRowsMatch(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROW_COUNT);
        Predicate predicate = Pred.col("year").gtEq(2020);

        try (ByteRangeSource source = TestParquetFiles.openRangeReader(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            long emitted = sumBatchRowCounts(reader, predicate, Projection.ALL);

            assertThat(emitted)
                    .as("a predicate every row group fully matches emits all rows via the narrow-only path")
                    .isEqualTo(ROW_COUNT);
        }
    }

    @Test
    void readBatchesEmitsEveryRowWithoutAFilter(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROW_COUNT);

        try (ByteRangeSource source = TestParquetFiles.openRangeReader(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            long emitted = sumBatchRowCounts(reader, Predicate.ALWAYS_TRUE, Projection.ALL);

            assertThat(emitted)
                    .as("an always-true predicate takes the unfiltered arm and emits all rows")
                    .isEqualTo(ROW_COUNT);
        }
    }

    @Test
    void readBatchesNeverEmitsAnEmptyBatch(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROW_COUNT);
        Predicate predicate = Pred.col("year").eq(2024);

        try (ByteRangeSource source = TestParquetFiles.openRangeReader(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            try (Stream<ParquetRecordBatch> batches =
                    reader.readBatches(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                for (ParquetRecordBatch batch : batches.toList()) {
                    assertThat(batch.rowCount())
                            .as("every emitted batch holds at least one matching row")
                            .isPositive();
                    batch.close();
                }
            }
        }
    }

    @Test
    void readBatchesNarrowsToTheProjectionEvenWhenFilteringOnANonProjectedColumn(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROW_COUNT);
        Predicate predicate = Pred.col("year").eq(2022);
        ColumnPath value = ColumnPath.of("value");
        Projection projection = Projection.ofPhysical(Set.of(value));

        try (ByteRangeSource source = TestParquetFiles.openRangeReader(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            try (Stream<ParquetRecordBatch> batches = reader.readBatches(predicate, projection, ReadOptions.DEFAULTS)) {
                for (ParquetRecordBatch batch : batches.toList()) {
                    assertThat(batch.columns().keySet())
                            .as("the predicate column 'year' must not leak into the output")
                            .containsExactly(value);
                    batch.close();
                }
            }
        }
    }

    @Test
    void readBatchesIsExactUnderLateMaterialization(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROW_COUNT);
        Predicate predicate = Pred.col("year").eq(2022);
        ReadOptions lateMatOn =
                ReadOptions.builder().useLateMaterialization(true).build();

        try (ByteRangeSource source = TestParquetFiles.openRangeReader(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            long viaCount = reader.count(predicate, lateMatOn);

            long viaBatches = 0L;
            try (Stream<ParquetRecordBatch> batches = reader.readBatches(predicate, Projection.ALL, lateMatOn)) {
                for (ParquetRecordBatch batch : batches.toList()) {
                    viaBatches += batch.rowCount();
                    batch.close();
                }
            }

            assertThat(viaBatches)
                    .as("late-materialized readBatches emits exactly the matching rows")
                    .isEqualTo(viaCount);
        }
    }

    private static long sumBatchRowCounts(ParquetFileReader reader, Predicate predicate, Projection projection) {
        long total = 0L;
        try (Stream<ParquetRecordBatch> batches = reader.readBatches(predicate, projection, ReadOptions.DEFAULTS)) {
            List<ParquetRecordBatch> seen = batches.toList();
            for (ParquetRecordBatch batch : seen) {
                total += batch.rowCount();
                batch.close();
            }
        }
        return total;
    }
}
