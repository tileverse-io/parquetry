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
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.MatchAction;
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
 * A quantified comparison with a timestamp or a time literal compares the instant held by each element of a list: the
 * literal matches the elements holding that instant, and no other. A timestamp beyond the instants of INT64 in the unit
 * of the elements is later or earlier than each of them; microseconds reach about 292,000 years from the epoch.
 *
 * <p>The file has 100 rows in two row groups. Row {@code i} holds the list {@code [i, i + 100]} of microseconds, as
 * timestamps in one column and as times of day in another. A third column of timestamps holds, in turn, a null list, an
 * empty list, the list {@code [i, null]} and the list {@code [i, i + 100]}.
 */
class QuantifiedTemporalLiteralTest {

    private static final ColumnPath INSTANTS = ColumnPath.of("instants");
    private static final ColumnPath TIMES = ColumnPath.of("times");
    private static final ColumnPath INSTANT = ColumnPath.of("instants", "list", "element");
    private static final ColumnPath TIME = ColumnPath.of("times", "list", "element");
    private static final ColumnPath SPARSE = ColumnPath.of("sparse");
    private static final ColumnPath SPARSE_INSTANT = ColumnPath.of("sparse", "list", "element");
    private static final int ROWS = 100;
    private static final int ROWS_PER_GROUP = 50;
    private static final long SECOND_ELEMENT_OFFSET = 100L;
    private static final LocalDateTime EPOCH = LocalDateTime.of(1970, 1, 1, 0, 0);
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
        LogicalType.TimeUnit micros = LogicalType.TimeUnit.MICROS;
        SchemaNode instants = listOf("instants", new LogicalType.Timestamp(true, micros));
        SchemaNode times = listOf("times", new LogicalType.Time(true, micros));
        SchemaNode sparse = listOf("sparse", new LogicalType.Timestamp(true, micros));
        List<SchemaNode> columns = List.of(instants, times, sparse);
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, columns, Optional.empty(), -1);
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .build();
        file = tempDir.resolve("lists.parquet");
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, new ParquetSchema(root), options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            for (long row = 0; row < ROWS; row++) {
                appendMicros(appender, INSTANTS, row);
                appendMicros(appender, TIMES, row);
                appendSparse(appender, row);
                appender.endRow();
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("comparisonsWithATemporalLiteral")
    void temporalLiteralMatchesTheElementsHoldingItsInstant(String name, Predicate predicate, long expected) {
        assertEachReadSelects(predicate, expected);
    }

    static Stream<Arguments> comparisonsWithATemporalLiteral() {
        Value instant7 = instant(7L);
        Value instant150 = instant(150L);
        Value time7 = time(7L);
        return Stream.of(
                Arguments.of("ANY instant = 7us", any(new Predicate.Eq(INSTANT, instant7)), 1L),
                Arguments.of("ANY instant < 7us", any(new Predicate.Lt(INSTANT, instant7)), 7L),
                Arguments.of("ANY instant <> 7us", any(new Predicate.NotEq(INSTANT, instant7)), 100L),
                Arguments.of("ALL instant = 7us", all(new Predicate.Eq(INSTANT, instant7)), 0L),
                Arguments.of("ALL instant >= 7us", all(new Predicate.GtEq(INSTANT, instant7)), 93L),
                Arguments.of(
                        "ANY instant IN (7us, 150us)",
                        any(new Predicate.In(INSTANT, List.of(instant7, instant150))),
                        2L),
                Arguments.of("ONE instant > 150us", one(new Predicate.Gt(INSTANT, instant150)), 49L),
                Arguments.of("ANY time = 7us", any(new Predicate.Eq(TIME, time7)), 1L),
                Arguments.of("ANY time < 7us", any(new Predicate.Lt(TIME, time7)), 7L),
                Arguments.of("ALL time > 7us", all(new Predicate.Gt(TIME, time7)), 92L));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rangesOfTemporalLiterals")
    void rangeOfTemporalLiteralsMatchesTheElementsWithinIt(String name, Predicate predicate, long expected) {
        assertEachReadSelects(predicate, expected);
    }

    static Stream<Arguments> rangesOfTemporalLiterals() {
        Predicate instantFrom7To9 = between(INSTANT, instant(7L), instant(9L));
        Predicate instantFrom7To150 = between(INSTANT, instant(7L), instant(150L));
        Predicate instantFrom50To120 = between(INSTANT, instant(50L), instant(120L));
        Predicate instantOutside7To190 = new Predicate.Or(
                List.of(new Predicate.Lt(INSTANT, instant(7L)), new Predicate.Gt(INSTANT, instant(190L))));
        Predicate timeFrom7To9 = between(TIME, time(7L), time(9L));
        Predicate instantNot7Nor107 = new Predicate.Not(new Predicate.In(INSTANT, List.of(instant(7L), instant(107L))));
        return Stream.of(
                Arguments.of("ANY instant BETWEEN 7us AND 9us", any(instantFrom7To9), 3L),
                Arguments.of("ALL instant BETWEEN 7us AND 150us", all(instantFrom7To150), 44L),
                Arguments.of("ONE instant BETWEEN 50us AND 120us", one(instantFrom50To120), 71L),
                Arguments.of("NOT ANY instant BETWEEN 7us AND 9us", new Predicate.Not(any(instantFrom7To9)), 97L),
                Arguments.of("ANY instant < 7us OR instant > 190us", any(instantOutside7To190), 16L),
                Arguments.of("ANY time BETWEEN 7us AND 9us", any(timeFrom7To9), 3L),
                Arguments.of("ALL instant NOT IN (7us, 107us)", all(instantNot7Nor107), 99L));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("comparisonsWithATimestampOutOfRange")
    void timestampOutOfRangeIsLaterOrEarlierThanEachElement(String name, Predicate predicate, long expected) {
        assertEachReadSelects(predicate, expected);
    }

    static Stream<Arguments> comparisonsWithATimestampOutOfRange() {
        Value later = timestamp(LocalDateTime.of(300_000, 1, 1, 0, 0));
        Value earlier = timestamp(LocalDateTime.of(-300_000, 1, 1, 0, 0));
        Value instant7 = instant(7L);
        Predicate notInLater = new Predicate.Not(new Predicate.In(INSTANT, List.of(later)));
        return Stream.of(
                Arguments.of("ANY instant < year 300000", any(new Predicate.Lt(INSTANT, later)), 100L),
                Arguments.of("ALL instant <= year 300000", all(new Predicate.LtEq(INSTANT, later)), 100L),
                Arguments.of("ONE instant < year 300000", one(new Predicate.Lt(INSTANT, later)), 0L),
                Arguments.of("ALL instant <> year 300000", all(new Predicate.NotEq(INSTANT, later)), 100L),
                Arguments.of("ANY instant > year 300000", any(new Predicate.Gt(INSTANT, later)), 0L),
                Arguments.of("ANY instant >= year 300000", any(new Predicate.GtEq(INSTANT, later)), 0L),
                Arguments.of("ANY instant = year 300000", any(new Predicate.Eq(INSTANT, later)), 0L),
                Arguments.of("ANY instant > year -300000", any(new Predicate.Gt(INSTANT, earlier)), 100L),
                Arguments.of("ALL instant >= year -300000", all(new Predicate.GtEq(INSTANT, earlier)), 100L),
                Arguments.of("ANY instant < year -300000", any(new Predicate.Lt(INSTANT, earlier)), 0L),
                Arguments.of("ANY instant <= year -300000", any(new Predicate.LtEq(INSTANT, earlier)), 0L),
                Arguments.of(
                        "ANY instant IN (7us, year 300000)",
                        any(new Predicate.In(INSTANT, List.of(instant7, later))),
                        1L),
                Arguments.of("ANY instant IN (year 300000)", any(new Predicate.In(INSTANT, List.of(later))), 0L),
                Arguments.of("ALL instant NOT IN (year 300000)", all(notInLater), 100L),
                Arguments.of("ALL instant BETWEEN 7us AND year 300000", all(between(INSTANT, instant7, later)), 93L));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("comparisonsBeyondTheElements")
    void timestampOutOfRangeSelectsTheRowsOfATimestampBeyondTheElements(
            String name, Predicate outOfRange, Predicate withinTheRange) {
        long expected = rowsCountedWithoutPruning(withinTheRange);

        assertEachReadSelects(outOfRange, expected);
    }

    /**
     * Each quantifier and comparison over the third column, with a timestamp out of range and with a timestamp of the
     * same side within the range: the year 9999 is later than the elements, and the year 1 earlier.
     */
    static Stream<Arguments> comparisonsBeyondTheElements() {
        Value year300000 = timestamp(LocalDateTime.of(300_000, 1, 1, 0, 0));
        Value year9999 = timestamp(LocalDateTime.of(9999, 1, 1, 0, 0));
        Value yearMinus300000 = timestamp(LocalDateTime.of(-300_000, 1, 1, 0, 0));
        Value year1 = timestamp(LocalDateTime.of(1, 1, 1, 0, 0));
        List<Arguments> cases = new ArrayList<>();
        for (MatchAction match : MatchAction.values()) {
            for (Comparison comparison : COMPARISONS) {
                cases.add(beyondTheElements(match, comparison, "year 300000", year300000, year9999));
                cases.add(beyondTheElements(match, comparison, "year -300000", yearMinus300000, year1));
            }
        }
        return cases.stream();
    }

    private static Arguments beyondTheElements(
            MatchAction match, Comparison comparison, String label, Value outOfRange, Value withinTheRange) {
        String name = match + " sparse " + comparison.symbol() + " " + label;
        Predicate overTheLiteralOutOfRange = new Predicate.Quantified(match, comparison.of(SPARSE_INSTANT, outOfRange));
        Predicate overTheLiteralWithinTheRange =
                new Predicate.Quantified(match, comparison.of(SPARSE_INSTANT, withinTheRange));
        return Arguments.of(name, overTheLiteralOutOfRange, overTheLiteralWithinTheRange);
    }

    private record Comparison(String symbol, BiFunction<ColumnPath, Value, Predicate> constructor) {

        Predicate of(ColumnPath column, Value literal) {
            return constructor.apply(column, literal);
        }
    }

    private static final List<Comparison> COMPARISONS = List.of(
            new Comparison("=", Predicate.Eq::new),
            new Comparison("<>", Predicate.NotEq::new),
            new Comparison("<", Predicate.Lt::new),
            new Comparison("<=", Predicate.LtEq::new),
            new Comparison(">", Predicate.Gt::new),
            new Comparison(">=", Predicate.GtEq::new));

    private static long rowsCountedWithoutPruning(Predicate predicate) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            return reader.count(predicate, METADATA_PRUNING_OFF);
        }
    }

    private static void assertEachReadSelects(Predicate predicate, long expected) {
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

    private static Predicate between(ColumnPath element, Value from, Value to) {
        return new Predicate.And(List.of(new Predicate.GtEq(element, from), new Predicate.LtEq(element, to)));
    }

    private static Value instant(long micros) {
        return timestamp(EPOCH.plus(micros, ChronoUnit.MICROS));
    }

    private static Value timestamp(LocalDateTime instant) {
        return new Value.TimestampVal(instant, true);
    }

    private static Value time(long micros) {
        return new Value.TimeVal(LocalTime.ofNanoOfDay(micros * 1_000L));
    }

    private static Predicate any(Predicate leaf) {
        return new Predicate.Quantified(MatchAction.ANY, leaf);
    }

    private static Predicate all(Predicate leaf) {
        return new Predicate.Quantified(MatchAction.ALL, leaf);
    }

    private static Predicate one(Predicate leaf) {
        return new Predicate.Quantified(MatchAction.ONE, leaf);
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

    /** Appends the list {@code [row, row + 100]} of microseconds to the list column {@code list}. */
    private static void appendMicros(ParquetRecordBatchBuilder appender, ColumnPath list, long row) {
        appender.beginList(list);
        appender.addLong(row);
        appender.addLong(row + SECOND_ELEMENT_OFFSET);
        appender.endList();
    }

    /** Appends to the third column, by the row: a null list, an empty list, {@code [row, null]} or two instants. */
    private static void appendSparse(ParquetRecordBatchBuilder appender, long row) {
        int shape = (int) (row % 4);
        switch (shape) {
            case 0 -> appender.setNull(SPARSE);
            case 1 -> appender.beginList(SPARSE).endList();
            case 2 -> appender.beginList(SPARSE).addLong(row).addNull().endList();
            default -> appendMicros(appender, SPARSE, row);
        }
    }

    private static SchemaNode.Group listOf(String name, LogicalType elementType) {
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.of(elementType), -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        Optional<LogicalType> listType = Optional.of(new LogicalType.ListType());
        return new SchemaNode.Group(name, Repetition.OPTIONAL, List.of(repeated), listType, -1);
    }
}
