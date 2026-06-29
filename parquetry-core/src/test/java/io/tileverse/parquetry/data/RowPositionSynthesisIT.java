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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.SortedLongPositionSet;
import io.tileverse.parquetry.internal.read.TestParquetFiles;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchemaException;

/**
 * Guards the row-position synthesis machinery: the row-position column is produced only when a row-position delete
 * predicate asks for it, and a read whose caller-named column collides with a physical column of the same name is
 * rejected rather than silently shadowing it. The correctness of the synthesized positions themselves is checked
 * through the delete filter effect in {@link io.tileverse.parquetry.data.RowIndexExcludedReadIT}.
 */
class RowPositionSynthesisIT {

    // The caller names the synthesized row-position column; the engine mandates no fixed name (Iceberg uses _pos).
    private static final ColumnPath POS = ColumnPath.of("_pos");

    @Test
    void noRowPositionColumnWithoutADeletePredicate(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, 100);

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecordBatch> batches =
                    reader.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                batches.forEach(batch -> {
                    assertThat(batch.columns().keySet())
                            .as("a read without a row-position delete predicate synthesizes no extra column")
                            .containsExactlyInAnyOrder(
                                    ColumnPath.of("year"), ColumnPath.of("country"), ColumnPath.of("value"));
                    batch.close();
                });
            }
        }
    }

    @Test
    void rejectsACallerNameThatCollidesWithAPhysicalColumn(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeSingleLongColumnFile(tmp, 8, POS.name());
        Predicate delete = new Predicate.RowIndexExcluded(POS, SortedLongPositionSet.of(new long[] {0L}));

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            // readBatches validates the name collision eagerly, before returning the stream.
            assertThatThrownBy(() -> reader.readBatches(delete, Projection.ALL, ReadOptions.DEFAULTS))
                    .isInstanceOf(ParquetSchemaException.class)
                    .hasMessageContaining(POS.dot());
        }
    }

    @Test
    void countRejectsACallerNameThatCollidesWithAPhysicalColumn(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeSingleLongColumnFile(tmp, 8, POS.name());
        Predicate delete = new Predicate.RowIndexExcluded(POS, SortedLongPositionSet.of(new long[] {0L}));

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            assertThatThrownBy(() -> reader.count(delete, ReadOptions.DEFAULTS))
                    .isInstanceOf(ParquetSchemaException.class)
                    .hasMessageContaining(POS.dot());
        }
    }

    @Test
    void rejectsANestedRowPositionColumnName(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, 8);
        ColumnPath nested = ColumnPath.of("a", "b");
        Predicate delete = new Predicate.RowIndexExcluded(nested, SortedLongPositionSet.of(new long[] {0L}));

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            assertThatThrownBy(() -> reader.readBatches(delete, Projection.ALL, ReadOptions.DEFAULTS))
                    .isInstanceOf(ParquetSchemaException.class)
                    .hasMessageContaining("top-level");
        }
    }

    @Test
    void readBatchesAppliesDeletesAndHidesPositionWithRecordLevelFilteringOff(@TempDir Path tmp) throws Exception {
        int rows = 10_000;
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, rows);
        long[] deletedPositions = {0L, 5L, 8_191L, 8_192L, 8_193L, 9_999L};
        Predicate delete = new Predicate.RowIndexExcluded(POS, SortedLongPositionSet.of(deletedPositions));
        ReadOptions pushdownOnly =
                ReadOptions.builder().useRecordLevelFilter(false).build();
        long[] surviving = {0L};

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecordBatch> batches = reader.readBatches(delete, Projection.ALL, pushdownOnly)) {
                batches.forEach(batch -> {
                    assertThat(batch.columns().keySet())
                            .as("the synthesized row-position column must not leak into readBatches output")
                            .doesNotContain(POS);
                    surviving[0] += batch.rowCount();
                    batch.close();
                });
            }
        }

        assertThat(surviving[0])
                .as("readBatches applies positional deletes even with record-level filtering off")
                .isEqualTo(rows - deletedPositions.length);
    }
}
