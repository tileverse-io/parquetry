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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.SequencedSet;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.SortedLongPositionSet;
import io.tileverse.parquetry.internal.read.TestParquetFiles;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * End-to-end check that a {@link Projection.Column.RowPosition} output column presents each row's absolute file
 * position ({@code firstRowId} plus the row's 0-based position), kept true across row-group boundaries and under
 * positional deletes. The fixture stores {@code value = position * 1.5}, making each row's true position recoverable
 * from its data and verifiable against the emitted column.
 */
class RowPositionOutputIT {

    private static final int ROWS = 10_000;
    private static final ColumnPath VALUE = ColumnPath.of("value");
    private static final ColumnPath POS = ColumnPath.of("_pos");
    private static final ColumnPath ROW_ID = ColumnPath.of("_row_id");

    @Test
    void posMatchesTheTrueFilePositionAcrossRowGroups(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROWS);
        assertThat(TestParquetFiles.rowGroupCount(file))
                .as("fixture must span multiple row groups")
                .isGreaterThan(1);
        Projection valueAndPosition =
                projectionOf(new Projection.Column.Physical(VALUE, VALUE), new Projection.Column.RowPosition(POS, 0L));

        List<Long> positions = new ArrayList<>(ROWS);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows =
                    reader.read(Predicate.ALWAYS_TRUE, valueAndPosition, ReadOptions.DEFAULTS)) {
                rows.forEach(rec -> {
                    assertThat(rec.schema().leafColumns()).containsExactly(VALUE, POS);
                    assertThat(rec.getLong(POS))
                            .as("the emitted position equals the position recovered from value = position * 1.5")
                            .isEqualTo(positionFromValue(rec));
                    positions.add(rec.getLong(POS));
                });
            }
        }

        assertThat(positions).containsExactlyElementsOf(ascending(ROWS));
    }

    @Test
    void rowIdAddsTheFileFirstRowId(@TempDir Path tmp) throws Exception {
        long firstRowId = 1_000_000L;
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROWS);
        Projection valueAndRowId = projectionOf(
                new Projection.Column.Physical(VALUE, VALUE), new Projection.Column.RowPosition(ROW_ID, firstRowId));

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows = reader.read(Predicate.ALWAYS_TRUE, valueAndRowId, ReadOptions.DEFAULTS)) {
                rows.forEach(rec -> assertThat(rec.getLong(ROW_ID))
                        .as("a row id is the file's first_row_id plus the within-file position")
                        .isEqualTo(firstRowId + positionFromValue(rec)));
            }
        }
    }

    @Test
    void emitsPosAndRowIdTogether(@TempDir Path tmp) throws Exception {
        long firstRowId = 500L;
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROWS);
        Projection valuePosAndRowId = projectionOf(
                new Projection.Column.Physical(VALUE, VALUE),
                new Projection.Column.RowPosition(POS, 0L),
                new Projection.Column.RowPosition(ROW_ID, firstRowId));

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows =
                    reader.read(Predicate.ALWAYS_TRUE, valuePosAndRowId, ReadOptions.DEFAULTS)) {
                rows.forEach(rec -> {
                    long position = positionFromValue(rec);
                    assertThat(rec.getLong(POS)).isEqualTo(position);
                    assertThat(rec.getLong(ROW_ID)).isEqualTo(firstRowId + position);
                });
            }
        }
    }

    @Test
    void emitsRowPositionWhileApplyingPositionalDeletes(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROWS);
        long[] deletedPositions = {0L, 5L, 8_191L, 8_192L, 8_193L, 9_999L};
        Predicate deletes = new Predicate.RowIndexExcluded(POS, SortedLongPositionSet.of(deletedPositions));
        Projection valueAndPosition =
                projectionOf(new Projection.Column.Physical(VALUE, VALUE), new Projection.Column.RowPosition(POS, 0L));

        List<Long> emitted = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows = reader.read(deletes, valueAndPosition, ReadOptions.DEFAULTS)) {
                rows.forEach(rec -> {
                    assertThat(rec.getLong(POS))
                            .as("a surviving row keeps its true position even with deletes applied")
                            .isEqualTo(positionFromValue(rec));
                    emitted.add(rec.getLong(POS));
                });
            }
        }

        assertThat(emitted)
                .as("the emitted positions are the live rows, with deleted positions left as gaps")
                .containsExactlyElementsOf(survivors(deletedPositions, ROWS));
    }

    private static Projection projectionOf(Projection.Column... columns) {
        SequencedSet<Projection.Column> set = new LinkedHashSet<>(List.of(columns));
        return Projection.of(set);
    }

    private static List<Long> ascending(int count) {
        List<Long> values = new ArrayList<>(count);
        for (long i = 0; i < count; i++) {
            values.add(i);
        }
        return values;
    }

    private static List<Long> survivors(long[] deletedPositions, int rows) {
        SortedLongPositionSet deleted = SortedLongPositionSet.of(deletedPositions);
        List<Long> live = new ArrayList<>(rows);
        for (long position = 0; position < rows; position++) {
            if (!deleted.contains(position)) {
                live.add(position);
            }
        }
        return live;
    }

    /** The three-column fixture stores {@code value = position * 1.5}; recover the row's absolute file position. */
    private static long positionFromValue(ParquetRecord rec) {
        return Math.round(rec.getDouble(VALUE) / 1.5);
    }
}
