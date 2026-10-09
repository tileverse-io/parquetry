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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * A comparison with a date literal beyond the days of INT32 is exact: each cell is earlier than a literal after the
 * range and later than a literal before it. A DATE column reaches about 5.8 million years to each side of the epoch.
 *
 * <p>The file has 100 rows in two row groups: a DATE column holding one day per row from the epoch, with a null cell in
 * one row out of four.
 */
class DateLiteralOutOfRangeTest {

    private static final ColumnPath DAY = ColumnPath.of("day");
    private static final int ROWS = 100;
    private static final int ROWS_PER_GROUP = 50;
    private static final int ROWS_PER_NULL = 4;
    private static final long CELLS = ROWS - ROWS / ROWS_PER_NULL;
    private static final long DAYS_OF_INT32 = 1L << 32;
    private static final Value YEAR_6_MILLION = date(LocalDate.of(6_000_000, 1, 1));
    private static final Value YEAR_6_MILLION_BC = date(LocalDate.of(-6_000_000, 1, 1));
    /** Narrowed to an int, the days of this date are those of the cell of row 5. */
    private static final Value DAY_5_ONE_RANGE_LATER = date(LocalDate.ofEpochDay(DAYS_OF_INT32 + 5));

    private static final Value DAY_5 = date(LocalDate.ofEpochDay(5));
    private static final ReadOptions METADATA_PRUNING_OFF = ReadOptions.builder()
            .useStatsFilter(false)
            .useDictionaryFilter(false)
            .useColumnIndexFilter(false)
            .useBloomFilter(false)
            .build();

    @TempDir
    static Path tempDir;

    private static Path file;

    @BeforeAll
    static void writeFile() throws IOException {
        SchemaNode.Primitive day = new SchemaNode.Primitive(
                "day",
                Repetition.OPTIONAL,
                PrimitiveKind.INT32,
                OptionalInt.empty(),
                Optional.of(new LogicalType.DateType()),
                -1);
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(day), Optional.empty(), -1);
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .build();
        file = tempDir.resolve("dates.parquet");
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, new ParquetSchema(root), options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            for (int row = 0; row < ROWS; row++) {
                appendDay(appender, row);
            }
        }
    }

    // Count, read and batch read give the same exact answer, with and without pruning.
    @ParameterizedTest(name = "{0}")
    @MethodSource("comparisonsWithALiteralOutOfRange")
    void comparesLiteralOutOfRange(String name, Predicate predicate, long expected) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(reader.count(predicate, ReadOptions.DEFAULTS))
                    .as("count")
                    .isEqualTo(expected);
            assertThat(reader.count(predicate, METADATA_PRUNING_OFF))
                    .as("count without pruning")
                    .isEqualTo(expected);
            assertThat(rowsRead(reader, predicate)).as("rows read").isEqualTo(expected);
            assertThat(rowsReadInBatches(reader, predicate))
                    .as("rows read in batches")
                    .isEqualTo(expected);
        }
    }

    static Stream<Arguments> comparisonsWithALiteralOutOfRange() {
        return Stream.of(
                Arguments.of("day < year 6 million", new Predicate.Lt(DAY, YEAR_6_MILLION), CELLS),
                Arguments.of("day <= year 6 million", new Predicate.LtEq(DAY, YEAR_6_MILLION), CELLS),
                Arguments.of("day <> year 6 million", new Predicate.NotEq(DAY, YEAR_6_MILLION), CELLS),
                Arguments.of("day > year 6 million", new Predicate.Gt(DAY, YEAR_6_MILLION), 0L),
                Arguments.of("day >= year 6 million", new Predicate.GtEq(DAY, YEAR_6_MILLION), 0L),
                Arguments.of("day = year 6 million", new Predicate.Eq(DAY, YEAR_6_MILLION), 0L),
                Arguments.of("day > year 6 million BC", new Predicate.Gt(DAY, YEAR_6_MILLION_BC), CELLS),
                Arguments.of("day >= year 6 million BC", new Predicate.GtEq(DAY, YEAR_6_MILLION_BC), CELLS),
                Arguments.of("day <> year 6 million BC", new Predicate.NotEq(DAY, YEAR_6_MILLION_BC), CELLS),
                Arguments.of("day < year 6 million BC", new Predicate.Lt(DAY, YEAR_6_MILLION_BC), 0L),
                Arguments.of("day <= year 6 million BC", new Predicate.LtEq(DAY, YEAR_6_MILLION_BC), 0L),
                Arguments.of("day = year 6 million BC", new Predicate.Eq(DAY, YEAR_6_MILLION_BC), 0L),
                Arguments.of("day = day 5 one range later", new Predicate.Eq(DAY, DAY_5_ONE_RANGE_LATER), 0L),
                Arguments.of("day <> day 5 one range later", new Predicate.NotEq(DAY, DAY_5_ONE_RANGE_LATER), CELLS),
                Arguments.of("day <= day 5 one range later", new Predicate.LtEq(DAY, DAY_5_ONE_RANGE_LATER), CELLS),
                Arguments.of(
                        "day IN (day 5, day 5 one range later, year 6 million)",
                        new Predicate.In(DAY, List.of(DAY_5, DAY_5_ONE_RANGE_LATER, YEAR_6_MILLION)),
                        1L),
                Arguments.of("day <= the latest date", new Predicate.LtEq(DAY, date(LocalDate.MAX)), CELLS),
                Arguments.of("day >= the earliest date", new Predicate.GtEq(DAY, date(LocalDate.MIN)), CELLS));
    }

    private static Value date(LocalDate day) {
        return new Value.DateVal(day);
    }

    private static long rowsRead(ParquetFileReader reader, Predicate predicate) {
        try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.count();
        }
    }

    private static long rowsReadInBatches(ParquetFileReader reader, Predicate predicate) {
        try (Stream<ParquetRecordBatch> batches = reader.readBatches(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return batches.mapToLong(ParquetRecordBatch::rowCount).sum();
        }
    }

    /** Appends one row: the day {@code row} days after the epoch, or a null cell in one row out of four. */
    private static void appendDay(ParquetRecordBatchBuilder appender, int row) {
        if (row % ROWS_PER_NULL == 0) {
            appender.setNull(DAY);
        } else {
            appender.setInt(DAY, row);
        }
        appender.endRow();
    }
}
