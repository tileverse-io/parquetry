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
 * A quantified comparison over a list nested in lists tests the values of the innermost lists of a row, at any depth.
 * An empty or null inner list holds no value, and neither does a null struct holding the inner list. A null value, a
 * null struct and a struct without the compared field are elements of an innermost list matching no comparison.
 *
 * <p>The file has 8 rows in two row groups and four columns: a list of lists of integers, a list of lists of structs
 * with an integer field, a list of lists of lists of integers, and a list of structs holding an integer and a list of
 * integers.
 */
class QuantifiedNestedListTest {

    private static final ColumnPath MATRIX = ColumnPath.of("matrix");
    private static final ColumnPath GROUPS = ColumnPath.of("groups");
    private static final ColumnPath CUBE = ColumnPath.of("cube");
    private static final ColumnPath MATRIX_VALUE = ColumnPath.of("matrix", "list", "element", "list", "element");
    private static final ColumnPath GROUP_CODE = ColumnPath.of("groups", "list", "element", "list", "element", "code");
    private static final ColumnPath CUBE_VALUE =
            ColumnPath.of("cube", "list", "element", "list", "element", "list", "element");
    private static final ColumnPath CODE_OF_ELEMENT = ColumnPath.of("element", "code");
    private static final ColumnPath EVENTS = ColumnPath.of("events");
    private static final ColumnPath EVENT_TAG = ColumnPath.of("events", "list", "element", "tags", "list", "element");
    private static final ColumnPath TAGS_OF_ELEMENT = ColumnPath.of("element", "tags");
    private static final ColumnPath ID_OF_ELEMENT = ColumnPath.of("element", "id");
    private static final int ROWS_PER_GROUP = 4;

    private static final Integer[][][] MATRIX_ROWS = {
        {{1, 2}, {3}}, {{4}, {4}, {5}}, {{}, {5}}, {null, {6}}, {{null, 7}}, {}, null, {{8, 8}},
    };

    /** A struct of the second column; a null code is a struct without its field, and a null cell a null struct. */
    private record Cell(Integer code) {}

    private static final Cell[][][] GROUP_ROWS = {
        {{code(1), code(2)}, {code(3)}},
        {{code(4)}, {code(4)}, {code(5)}},
        {{}, {code(5)}},
        {null, {code(6)}},
        {{code(null), code(7)}},
        {{null, code(7)}},
        {},
        null,
    };

    private static final Integer[][][][] CUBE_ROWS = {
        {{{1}, {2}}, {{3}}}, {{{4, 5}}, {}, null, {{}, null, {6}}}, null, null, null, null, null, null,
    };

    /** A struct of the fourth column; null tags are a struct without its list, and a null cell a null struct. */
    private record Event(Integer[] tags) {}

    private static final Event[][] EVENT_ROWS = {
        {tags(1, 2), tags(3)},
        {null, tags(4)},
        {new Event(null), tags(5)},
        {tags(), tags(6)},
        {tags(null, 7)},
        {},
        null,
        {null},
    };

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
        SchemaNode matrix = listOf("matrix", listOf("element", optionalInt("element")));
        SchemaNode groups = listOf("groups", listOf("element", structOf("element", optionalInt("code"))));
        SchemaNode cube = listOf("cube", listOf("element", listOf("element", optionalInt("element"))));
        SchemaNode tags = listOf("tags", optionalInt("element"));
        SchemaNode event = new SchemaNode.Group(
                "element", Repetition.OPTIONAL, List.of(optionalInt("id"), tags), Optional.empty(), -1);
        SchemaNode events = listOf("events", event);
        List<SchemaNode> columns = List.of(matrix, groups, cube, events);
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, columns, Optional.empty(), -1);
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .build();
        file = tempDir.resolve("nested-lists.parquet");
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, new ParquetSchema(root), options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            for (int row = 0; row < MATRIX_ROWS.length; row++) {
                appendMatrix(appender, MATRIX_ROWS[row]);
                appendGroups(appender, GROUP_ROWS[row]);
                appendCube(appender, CUBE_ROWS[row]);
                appendEvents(appender, EVENT_ROWS[row]);
                appender.endRow();
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("comparisonsOverNestedLists")
    void quantifiedComparisonTestsTheValuesOfTheInnermostLists(String name, Predicate predicate, long expected) {
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

    static Stream<Arguments> comparisonsOverNestedLists() {
        Predicate matrixIs5 = new Predicate.Eq(MATRIX_VALUE, intValue(5));
        Predicate matrixIs3Or6 = new Predicate.In(MATRIX_VALUE, List.of(intValue(3), intValue(6)));
        return Stream.of(
                Arguments.of("ANY matrix = 5", any(matrixIs5), 2L),
                Arguments.of("ANY matrix = 4", any(new Predicate.Eq(MATRIX_VALUE, intValue(4))), 1L),
                Arguments.of("ANY matrix > 6", any(new Predicate.Gt(MATRIX_VALUE, intValue(6))), 2L),
                Arguments.of("ANY matrix IN (3, 6)", any(matrixIs3Or6), 2L),
                Arguments.of("ALL matrix > 0", all(new Predicate.Gt(MATRIX_VALUE, intValue(0))), 7L),
                Arguments.of("ALL matrix = 8", all(new Predicate.Eq(MATRIX_VALUE, intValue(8))), 3L),
                Arguments.of("ONE matrix = 4", one(new Predicate.Eq(MATRIX_VALUE, intValue(4))), 0L),
                Arguments.of("ONE matrix = 5", one(matrixIs5), 2L),
                Arguments.of("NOT ANY matrix = 5", new Predicate.Not(any(matrixIs5)), 5L),
                Arguments.of("ANY code = 5", any(new Predicate.Eq(GROUP_CODE, intValue(5))), 2L),
                Arguments.of("ANY code = 7", any(new Predicate.Eq(GROUP_CODE, intValue(7))), 2L),
                Arguments.of("ALL code > 0", all(new Predicate.Gt(GROUP_CODE, intValue(0))), 6L),
                Arguments.of("ONE code = 7", one(new Predicate.Eq(GROUP_CODE, intValue(7))), 2L),
                Arguments.of("ONE code = 4", one(new Predicate.Eq(GROUP_CODE, intValue(4))), 0L),
                Arguments.of("ANY cube = 3", any(new Predicate.Eq(CUBE_VALUE, intValue(3))), 1L),
                Arguments.of("ANY cube = 6", any(new Predicate.Eq(CUBE_VALUE, intValue(6))), 1L),
                Arguments.of("ALL cube > 4", all(new Predicate.Gt(CUBE_VALUE, intValue(4))), 6L),
                Arguments.of("ONE cube = 5", one(new Predicate.Eq(CUBE_VALUE, intValue(5))), 1L),
                Arguments.of("ANY tag = 4", any(new Predicate.Eq(EVENT_TAG, intValue(4))), 1L),
                Arguments.of("ALL tag > 0", all(new Predicate.Gt(EVENT_TAG, intValue(0))), 7L),
                Arguments.of("ONE tag = 5", one(new Predicate.Eq(EVENT_TAG, intValue(5))), 1L),
                Arguments.of("NOT ANY tag = 4", new Predicate.Not(any(new Predicate.Eq(EVENT_TAG, intValue(4)))), 6L));
    }

    private static Value intValue(int value) {
        return new Value.IntVal(value);
    }

    private static Cell code(Integer value) {
        return new Cell(value);
    }

    private static Event tags(Integer... values) {
        return new Event(values);
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

    private static void appendMatrix(ParquetRecordBatchBuilder appender, Integer[][] lists) {
        if (lists == null) {
            appender.setNull(MATRIX);
            return;
        }
        appender.beginList(MATRIX);
        for (Integer[] values : lists) {
            addIntegers(appender, values);
        }
        appender.endList();
    }

    private static void appendCube(ParquetRecordBatchBuilder appender, Integer[][][] matrices) {
        if (matrices == null) {
            appender.setNull(CUBE);
            return;
        }
        appender.beginList(CUBE);
        for (Integer[][] lists : matrices) {
            addMatrix(appender, lists);
        }
        appender.endList();
    }

    private static void addMatrix(ParquetRecordBatchBuilder appender, Integer[][] lists) {
        if (lists == null) {
            appender.addNull();
            return;
        }
        appender.addList();
        for (Integer[] values : lists) {
            addIntegers(appender, values);
        }
        appender.endList();
    }

    private static void addIntegers(ParquetRecordBatchBuilder appender, Integer[] values) {
        if (values == null) {
            appender.addNull();
            return;
        }
        appender.addList();
        for (Integer value : values) {
            addInteger(appender, value);
        }
        appender.endList();
    }

    private static void addInteger(ParquetRecordBatchBuilder appender, Integer value) {
        if (value == null) {
            appender.addNull();
        } else {
            appender.addInt(value);
        }
    }

    private static void appendGroups(ParquetRecordBatchBuilder appender, Cell[][] lists) {
        if (lists == null) {
            appender.setNull(GROUPS);
            return;
        }
        appender.beginList(GROUPS);
        for (Cell[] cells : lists) {
            addCells(appender, cells);
        }
        appender.endList();
    }

    private static void addCells(ParquetRecordBatchBuilder appender, Cell[] cells) {
        if (cells == null) {
            appender.addNull();
            return;
        }
        appender.addList();
        for (Cell cell : cells) {
            addCell(appender, cell);
        }
        appender.endList();
    }

    private static void addCell(ParquetRecordBatchBuilder appender, Cell cell) {
        if (cell == null) {
            appender.addNull();
            return;
        }
        appender.addElement();
        if (cell.code() == null) {
            appender.setNull(CODE_OF_ELEMENT);
        } else {
            appender.setInt(CODE_OF_ELEMENT, cell.code());
        }
        appender.endElement();
    }

    private static void appendEvents(ParquetRecordBatchBuilder appender, Event[] events) {
        if (events == null) {
            appender.setNull(EVENTS);
            return;
        }
        appender.beginList(EVENTS);
        for (Event event : events) {
            addEvent(appender, event);
        }
        appender.endList();
    }

    private static void addEvent(ParquetRecordBatchBuilder appender, Event event) {
        if (event == null) {
            appender.addNull();
            return;
        }
        appender.addElement();
        appender.setInt(ID_OF_ELEMENT, 1);
        if (event.tags() != null) {
            appender.beginList(TAGS_OF_ELEMENT);
            for (Integer tag : event.tags()) {
                addInteger(appender, tag);
            }
            appender.endList();
        }
        appender.endElement();
    }

    private static SchemaNode.Group listOf(String name, SchemaNode element) {
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        Optional<LogicalType> listType = Optional.of(new LogicalType.ListType());
        return new SchemaNode.Group(name, Repetition.OPTIONAL, List.of(repeated), listType, -1);
    }

    private static SchemaNode.Group structOf(String name, SchemaNode field) {
        return new SchemaNode.Group(name, Repetition.OPTIONAL, List.of(field), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive optionalInt(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }
}
