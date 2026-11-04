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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * Reads lists and maps written in the shapes of older writers, by the backward-compatibility rules of the format. Three
 * files come from parquet-testing:
 *
 * <ul>
 *   <li>{@code repeated_primitive_no_list.parquet} holds repeated primitives outside a list: {@code repeated int32
 *       Int32_list}, 4 rows {@code [0, 1, 2, 3], [], [4], [5, 6, 7, 8]}.
 *   <li>{@code repeated_no_annotation.parquet} holds a repeated group outside a list: {@code optional group
 *       phoneNumbers { repeated group phone { required int64 number; optional binary kind } }}, 6 rows.
 *   <li>{@code old_list_structure.parquet} holds a list of two-level lists: {@code required group a (LIST) { repeated
 *       group array (LIST) { repeated int32 array } }}, one row {@code [[1, 2], [3, 4]]}.
 * </ul>
 *
 * <p>parquet-java writes the shapes of parquet-thrift and of a map annotated {@code MAP_KEY_VALUE} in the test.
 */
class LegacyShapeReadTest {

    private static final String REPEATED_PRIMITIVES = "repeated_primitive_no_list.parquet";
    private static final String REPEATED_GROUP = "repeated_no_annotation.parquet";
    private static final String LIST_OF_LISTS = "old_list_structure.parquet";

    private static final ColumnPath INT32_ELEMENT = ColumnPath.of("Int32_list", "list", "element");
    private static final ColumnPath STRING_ELEMENT = ColumnPath.of("String_list", "list", "element");
    private static final ColumnPath PHONE_NUMBER = ColumnPath.of("phoneNumbers", "phone", "list", "element", "number");
    private static final ColumnPath PHONE_KIND = ColumnPath.of("phoneNumbers", "phone", "list", "element", "kind");
    private static final ColumnPath LISTS = ColumnPath.of("a");
    private static final ColumnPath LISTS_VALUE = ColumnPath.of("a", "list", "element", "list", "element");
    private static final ColumnPath TUPLE_FIELD = ColumnPath.of("a", "list", "element", "list", "element", "x");
    private static final ColumnPath MAP = ColumnPath.of("mk");
    private static final ColumnPath MAP_VALUE = ColumnPath.of("mk", "map", "value");

    private static final ReadOptions METADATA_PRUNING_OFF = ReadOptions.builder()
            .useStatsFilter(false)
            .useColumnIndexFilter(false)
            .useBloomFilter(false)
            .build();

    @TempDir
    Path tempDir;

    @Test
    void repeatedValuesReadAsLists() {
        List<List<Object>> lists = readRows(corpusFile(REPEATED_PRIMITIVES), row -> row.multiValue(INT32_ELEMENT));

        assertThat(lists).containsExactly(List.of(0, 1, 2, 3), List.of(), List.of(4), List.of(5, 6, 7, 8));
    }

    @Test
    void repeatedGroupKeepsItsFields() {
        List<List<Object>> numbers = readRows(corpusFile(REPEATED_GROUP), row -> row.multiValue(PHONE_NUMBER));
        List<Integer> kinds = readRows(corpusFile(REPEATED_GROUP), row -> row.multiValueSize(PHONE_KIND));

        assertThat(numbers)
                .containsExactly(
                        List.of(),
                        List.of(),
                        List.of(),
                        List.of(5555555555L),
                        List.of(1111111111L),
                        List.of(1111111111L, 2222222222L, 3333333333L));
        assertThat(kinds).containsExactly(0, 0, 0, 1, 1, 3);
    }

    @Test
    void listOfListsKeepsItsLists() {
        List<Object> lists = readRows(corpusFile(LIST_OF_LISTS), row -> row.get(LISTS));

        assertThat(lists).containsExactly(List.of(List.of(1, 2), List.of(3, 4)));
    }

    // parquet-thrift writes list<list<struct{x}>> with an inner one-field struct named after the stored list name.
    @Test
    void nestedOneTupleKeepsStruct() throws IOException {
        String schema = "message m { required group a (LIST) { repeated group a_tuple (LIST) {"
                + " repeated group a_tuple_tuple { required int32 x; } } } }";
        Path file = writeWithParquetJava("thrift.parquet", schema, row -> {
            Group lists = row.addGroup("a");
            lists.addGroup("a_tuple").addGroup("a_tuple_tuple").append("x", 1);
            Group second = lists.addGroup("a_tuple");
            second.addGroup("a_tuple_tuple").append("x", 2);
            second.addGroup("a_tuple_tuple").append("x", 3);
        });

        assertThat(readRows(file, row -> row.multiValue(TUPLE_FIELD))).containsExactly(List.of(1, 2, 3));
        assertThat(readRows(copyOf(file), row -> row.multiValue(TUPLE_FIELD))).containsExactly(List.of(1, 2, 3));
    }

    // Some writers annotated a map with MAP_KEY_VALUE in place of MAP.
    @Test
    void mapKeyValueReadsAsMap() throws IOException {
        String schema = "message m { optional group mk (MAP_KEY_VALUE) {"
                + " repeated group map { required binary key (UTF8); optional int32 value; } } }";
        Path file = writeWithParquetJava("map.parquet", schema, row -> {
            Group map = row.addGroup("mk");
            map.addGroup("map").append("key", "a").append("value", 1);
            map.addGroup("map").append("key", "b").append("value", 2);
        });

        assertThat(mapAnnotation(file)).contains(new LogicalType.MapType());
        assertThat(readRows(file, row -> row.multiValue(MAP_VALUE))).containsExactly(List.of(1, 2));
        Path copy = copyOf(file);
        assertThat(mapAnnotation(copy)).contains(new LogicalType.MapType());
        assertThat(readRows(copy, row -> row.multiValue(MAP_VALUE))).containsExactly(List.of(1, 2));
    }

    @Test
    void schemaHasStandardLists() {
        try (ByteRangeSource source = ByteRangeSource.ofFile(corpusFile(REPEATED_PRIMITIVES))) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(reader.schema().leafColumns())
                    .contains(INT32_ELEMENT, STRING_ELEMENT)
                    .doesNotContain(ColumnPath.of("Int32_list"));
        }
    }

    @ParameterizedTest
    @MethodSource("filtersOnElements")
    void filtersReachEachElement(String file, Predicate filter, long expected) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(corpusFile(file))) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(reader.count(filter, ReadOptions.DEFAULTS)).as("count").isEqualTo(expected);
            assertThat(reader.count(filter, METADATA_PRUNING_OFF))
                    .as("count without pruning")
                    .isEqualTo(expected);
            assertThat(rowsRead(reader, filter)).as("rows read").isEqualTo(expected);
            assertThat(rowsReadInBatches(reader, filter))
                    .as("rows read in batches")
                    .isEqualTo(expected);
        }
    }

    static Stream<Arguments> filtersOnElements() {
        return Stream.of(
                Arguments.of(REPEATED_PRIMITIVES, any(new Predicate.Eq(INT32_ELEMENT, new Value.IntVal(5))), 1L),
                Arguments.of(REPEATED_PRIMITIVES, any(new Predicate.IsNotNull(INT32_ELEMENT)), 3L),
                Arguments.of(
                        REPEATED_PRIMITIVES, any(new Predicate.Eq(STRING_ELEMENT, new Value.StringVal("three"))), 1L),
                Arguments.of(REPEATED_GROUP, any(new Predicate.Eq(PHONE_NUMBER, new Value.LongVal(5555555555L))), 1L),
                Arguments.of(REPEATED_GROUP, any(new Predicate.IsNull(PHONE_KIND)), 2L),
                Arguments.of(LIST_OF_LISTS, any(new Predicate.Eq(LISTS_VALUE, new Value.IntVal(4))), 1L));
    }

    private static Predicate any(Predicate leaf) {
        return new Predicate.Quantified(MatchAction.ANY, leaf);
    }

    private static <T> List<T> readRows(Path file, Function<ParquetRecord, T> extract) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows =
                    reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                return rows.map(extract).toList();
            }
        }
    }

    private static long rowsRead(ParquetFileReader reader, Predicate filter) {
        try (Stream<ParquetRecord> rows = reader.read(filter, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.count();
        }
    }

    private static long rowsReadInBatches(ParquetFileReader reader, Predicate filter) {
        try (Stream<ParquetRecordBatch> batches = reader.readBatches(filter, Projection.ALL, ReadOptions.DEFAULTS)) {
            return batches.mapToLong(ParquetRecordBatch::rowCount).sum();
        }
    }

    private static Optional<LogicalType> mapAnnotation(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            return reader.schema().find(MAP).orElseThrow().logicalType();
        }
    }

    /** Writes one row with parquet-java, in the schema given in its text form. */
    private Path writeWithParquetJava(String name, String schemaText, Consumer<Group> fillRow) throws IOException {
        Path file = tempDir.resolve(name);
        MessageType schema = MessageTypeParser.parseMessageType(schemaText);
        Group row = new SimpleGroupFactory(schema).newGroup();
        fillRow.accept(row);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(file))
                .withType(schema)
                .withConf(new PlainParquetConfiguration())
                .build()) {
            writer.write(row);
        }
        return file;
    }

    /** Copies {@code file} batch by batch, as {@code par cp} does. */
    private Path copyOf(Path file) throws IOException {
        Path copy = tempDir.resolve("copy-" + file.getFileName());
        WriteOptions options = WriteOptions.builder().tempDir(tempDir).build();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file);
                OutputStream out = Files.newOutputStream(copy)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (ParquetFileWriter writer = ParquetFileWriter.create(out, reader.schema(), options);
                    Stream<ParquetRecordBatch> batches =
                            reader.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                batches.forEach(batch -> {
                    try (batch) {
                        writer.writeBatch(batch);
                    }
                });
            }
        }
        return copy;
    }

    private static Path corpusFile(String name) {
        return CorpusFixtures.parquetTestingData().resolve(name);
    }
}
