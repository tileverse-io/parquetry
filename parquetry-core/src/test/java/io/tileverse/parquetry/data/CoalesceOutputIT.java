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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.SequencedSet;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.internal.read.TestParquetFiles;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchemaException;

/**
 * End-to-end check of a {@link Projection.Column.Coalesce} output column: the decoded {@code stored} cell wins where
 * present and a null cell takes the fallback (a row position or a constant), across row-group boundaries and under a
 * record-level predicate. The fixture stores {@code stored = 1000 + id} on every third row and null elsewhere, making
 * each cell's expected value recoverable from its row's {@code id}.
 *
 * <p>The coalesce may present under its source name (filling the source column in place) or under a different name (a
 * renamed output, the shape an Iceberg materialized lineage column takes when its physical leaf is not named after the
 * reserved column). Both presentations must agree.
 */
class CoalesceOutputIT {

    private static final int ROWS = 10_000;
    private static final long FIRST_ROW_ID = 1_000_000L;
    private static final long SEQUENCE_NUMBER = 42L;
    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath STORED = ColumnPath.of("stored");
    private static final ColumnPath ROW_ID = ColumnPath.of("_row_id");

    @Test
    void coalescesUnderTheSourceName(@TempDir Path tmp) throws Exception {
        Path file = fixture(tmp);
        Projection projection = projectionOf(
                new Projection.Column.Physical(ID, ID),
                new Projection.Column.Coalesce(STORED, STORED, positionFallback()));

        long rows = forEachRow(file, Predicate.ALWAYS_TRUE, projection, rec -> {
            assertThat(rec.getLong(STORED)).isEqualTo(expectedRowId(rec.getLong(ID)));
        });

        assertThat(rows).isEqualTo(ROWS);
    }

    @Test
    void presentsARenamedCoalesce(@TempDir Path tmp) throws Exception {
        Path file = fixture(tmp);
        Projection projection = projectionOf(
                new Projection.Column.Physical(ID, ID),
                new Projection.Column.Coalesce(ROW_ID, STORED, positionFallback()));

        long rows = forEachRow(file, Predicate.ALWAYS_TRUE, projection, rec -> {
            assertThat(rec.schema().leafColumns()).containsExactly(ID, ROW_ID);
            assertThat(rec.getLong(ROW_ID)).isEqualTo(expectedRowId(rec.getLong(ID)));
        });

        assertThat(rows).isEqualTo(ROWS);
    }

    @Test
    void keepsTrueFallbackPositionsForARenamedCoalesceUnderAPredicate(@TempDir Path tmp) throws Exception {
        Path file = fixture(tmp);
        // Dropping one row must not shift the fallback positions of its successors: a surviving null cell still
        // resolves to FIRST_ROW_ID + its TRUE file position, not a compacted index.
        Predicate dropsOneRow = new Predicate.NotEq(ID, new Value.LongVal(7L));
        Projection projection = projectionOf(
                new Projection.Column.Physical(ID, ID),
                new Projection.Column.Coalesce(ROW_ID, STORED, positionFallback()));

        long rows = forEachRow(file, dropsOneRow, projection, rec -> {
            assertThat(rec.getLong(ID)).isNotEqualTo(7L);
            assertThat(rec.getLong(ROW_ID)).isEqualTo(expectedRowId(rec.getLong(ID)));
        });

        assertThat(rows).isEqualTo(ROWS - 1L);
    }

    @Test
    void coalescesWithAConstantFallback(@TempDir Path tmp) throws Exception {
        Path file = fixture(tmp);
        Projection projection = projectionOf(
                new Projection.Column.Physical(ID, ID),
                new Projection.Column.Coalesce(ROW_ID, STORED, constantFallback()));

        long rows = forEachRow(file, Predicate.ALWAYS_TRUE, projection, rec -> {
            long id = rec.getLong(ID);
            long expected = (id % 3 == 0) ? TestParquetFiles.STORED_BASE + id : SEQUENCE_NUMBER;
            assertThat(rec.getLong(ROW_ID)).isEqualTo(expected);
        });

        assertThat(rows).isEqualTo(ROWS);
    }

    @Test
    void batchesAgreeWithRowsForARenamedCoalesce(@TempDir Path tmp) throws Exception {
        Path file = fixture(tmp);
        Projection projection = projectionOf(
                new Projection.Column.Physical(ID, ID),
                new Projection.Column.Coalesce(ROW_ID, STORED, positionFallback()));

        long rows = 0;
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecordBatch> batches =
                    reader.readBatches(Predicate.ALWAYS_TRUE, projection, ReadOptions.DEFAULTS)) {
                rows = batches.mapToLong(batch -> {
                            LongVector ids = (LongVector) batch.columns().get(ID);
                            LongVector rowIds = (LongVector) batch.columns().get(ROW_ID);
                            for (int row = 0; row < batch.rowCount(); row++) {
                                assertThat(rowIds.getLong(row)).isEqualTo(expectedRowId(ids.getLong(row)));
                            }
                            return batch.rowCount();
                        })
                        .sum();
            }
        }
        assertThat(rows).isEqualTo(ROWS);
    }

    @Test
    void rejectsARenamedCoalesceWhoseNameAPhysicalColumnAlreadyHas(@TempDir Path tmp) throws Exception {
        Path file = fixture(tmp);
        Projection projection = projectionOf(new Projection.Column.Coalesce(ID, STORED, positionFallback()));

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            assertThatThrownBy(() -> reader.read(Predicate.ALWAYS_TRUE, projection, ReadOptions.DEFAULTS)
                            .close())
                    .isInstanceOf(ParquetSchemaException.class)
                    .hasMessageContaining("id");
        }
    }

    /** A stored cell (every third row) keeps its value; a null cell falls back to {@code FIRST_ROW_ID + position}. */
    private static long expectedRowId(long id) {
        return (id % 3 == 0) ? TestParquetFiles.STORED_BASE + id : FIRST_ROW_ID + id;
    }

    private static Projection.Column.Coalesce.Fallback positionFallback() {
        return new Projection.Column.Coalesce.Fallback.Position(FIRST_ROW_ID);
    }

    private static Projection.Column.Coalesce.Fallback constantFallback() {
        return new Projection.Column.Coalesce.Fallback.Constant(new Value.LongVal(SEQUENCE_NUMBER));
    }

    private static Path fixture(Path tmp) throws Exception {
        Path file = TestParquetFiles.writeNullableLongColumnFileMultiRowGroup(tmp, ROWS);
        assertThat(TestParquetFiles.rowGroupCount(file))
                .as("fixture must span multiple row groups")
                .isGreaterThan(1);
        return file;
    }

    /** Runs {@code check} on every emitted row and returns the row count, anchoring against a vacuously empty read. */
    private static long forEachRow(Path file, Predicate predicate, Projection projection, Consumer<ParquetRecord> check)
            throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows = reader.read(predicate, projection, ReadOptions.DEFAULTS)) {
                return rows.mapToLong(rec -> {
                            check.accept(rec);
                            return 1L;
                        })
                        .sum();
            }
        }
    }

    private static Projection projectionOf(Projection.Column... columns) {
        SequencedSet<Projection.Column> set = new LinkedHashSet<>(List.of(columns));
        return Projection.of(set);
    }
}
