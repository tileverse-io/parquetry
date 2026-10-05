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
import java.time.LocalDateTime;
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
 * A comparison with a timestamp literal beyond the instants of INT64 in the unit of the column is exact: each cell is
 * earlier than a literal after the range and later than a literal before it. A nanosecond column reaches from the year
 * 1677 to the year 2262.
 *
 * <p>The file has 100 rows in two row groups: a nanosecond timestamp column holding one second per row from the epoch,
 * with a null cell in one row out of four.
 */
class TimestampLiteralOutOfRangeTest {

    private static final ColumnPath TS = ColumnPath.of("ts");
    private static final int ROWS = 100;
    private static final int ROWS_PER_GROUP = 50;
    private static final int ROWS_PER_NULL = 4;
    private static final long CELLS = ROWS - ROWS / ROWS_PER_NULL;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;
    private static final LocalDateTime EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0);
    private static final LocalDateTime LAST_INSTANT = EPOCH.plusNanos(Long.MAX_VALUE);
    private static final LocalDateTime FIRST_INSTANT = EPOCH.plusNanos(Long.MIN_VALUE);
    private static final Value YEAR_9999 = timestamp(LocalDateTime.of(9999, 12, 31, 0, 0));
    private static final Value YEAR_1600 = timestamp(LocalDateTime.of(1600, 1, 1, 0, 0));
    private static final Value SECOND_5 = timestamp(EPOCH.plusSeconds(5));
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
        LogicalType nanos = new LogicalType.Timestamp(true, LogicalType.TimeUnit.NANOS);
        SchemaNode.Primitive ts = new SchemaNode.Primitive(
                "ts", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.of(nanos), -1);
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(ts), Optional.empty(), -1);
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .build();
        file = tempDir.resolve("timestamps.parquet");
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, new ParquetSchema(root), options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            for (long row = 0; row < ROWS; row++) {
                appendSecond(appender, row);
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("comparisonsWithALiteralOutOfRange")
    void comparisonWithALiteralOutOfRangeIsExact(String name, Predicate predicate, long expected) {
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
        Value oneNanosecondLater = timestamp(LAST_INSTANT.plusNanos(1));
        Value oneNanosecondEarlier = timestamp(FIRST_INSTANT.minusNanos(1));
        return Stream.of(
                Arguments.of("ts < year 9999", new Predicate.Lt(TS, YEAR_9999), CELLS),
                Arguments.of("ts <= year 9999", new Predicate.LtEq(TS, YEAR_9999), CELLS),
                Arguments.of("ts <> year 9999", new Predicate.NotEq(TS, YEAR_9999), CELLS),
                Arguments.of("ts > year 9999", new Predicate.Gt(TS, YEAR_9999), 0L),
                Arguments.of("ts >= year 9999", new Predicate.GtEq(TS, YEAR_9999), 0L),
                Arguments.of("ts = year 9999", new Predicate.Eq(TS, YEAR_9999), 0L),
                Arguments.of("ts IN (year 9999)", new Predicate.In(TS, List.of(YEAR_9999)), 0L),
                Arguments.of("ts > year 1600", new Predicate.Gt(TS, YEAR_1600), CELLS),
                Arguments.of("ts >= year 1600", new Predicate.GtEq(TS, YEAR_1600), CELLS),
                Arguments.of("ts <> year 1600", new Predicate.NotEq(TS, YEAR_1600), CELLS),
                Arguments.of("ts < year 1600", new Predicate.Lt(TS, YEAR_1600), 0L),
                Arguments.of("ts <= year 1600", new Predicate.LtEq(TS, YEAR_1600), 0L),
                Arguments.of("ts = year 1600", new Predicate.Eq(TS, YEAR_1600), 0L),
                Arguments.of(
                        "ts IN (second 5, year 9999, year 1600)",
                        new Predicate.In(TS, List.of(SECOND_5, YEAR_9999, YEAR_1600)),
                        1L),
                Arguments.of("ts <= the last instant", new Predicate.LtEq(TS, timestamp(LAST_INSTANT)), CELLS),
                Arguments.of("ts < one nanosecond later", new Predicate.Lt(TS, oneNanosecondLater), CELLS),
                Arguments.of("ts >= one nanosecond later", new Predicate.GtEq(TS, oneNanosecondLater), 0L),
                Arguments.of("ts >= the first instant", new Predicate.GtEq(TS, timestamp(FIRST_INSTANT)), CELLS),
                Arguments.of("ts > one nanosecond earlier", new Predicate.Gt(TS, oneNanosecondEarlier), CELLS),
                Arguments.of("ts <= one nanosecond earlier", new Predicate.LtEq(TS, oneNanosecondEarlier), 0L),
                Arguments.of("ts <= the latest timestamp", new Predicate.LtEq(TS, timestamp(LocalDateTime.MAX)), CELLS),
                Arguments.of(
                        "ts >= the earliest timestamp", new Predicate.GtEq(TS, timestamp(LocalDateTime.MIN)), CELLS));
    }

    private static Value timestamp(LocalDateTime instant) {
        return new Value.TimestampVal(instant, true);
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

    /** Appends one row: the instant {@code row} seconds after the epoch, or a null cell in one row out of four. */
    private static void appendSecond(ParquetRecordBatchBuilder appender, long row) {
        if (row % ROWS_PER_NULL == 0) {
            appender.setNull(TS);
        } else {
            appender.setLong(TS, row * NANOS_PER_SECOND);
        }
        appender.endRow();
    }
}
