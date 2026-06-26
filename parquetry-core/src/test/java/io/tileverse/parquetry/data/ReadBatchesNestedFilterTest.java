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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Regression net for exact {@code readBatches} over a file that mixes a flat filterable column with nested STRUCT,
 * LIST, and MAP columns. An assembled batch keys a LIST or MAP column at its group path and drops the element-granular
 * leaf entries; narrowing the survivor batch by leaf paths therefore loses those columns and breaks the batch. The
 * oracle is the differential between {@code read}, {@code readBatches}, and {@code count}, which reach the rows through
 * paths that do not run the survivor-batch narrowing and are independent of it.
 */
class ReadBatchesNestedFilterTest {

    private static final int ROW_COUNT = 12;

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath PERSON = ColumnPath.of("person");
    private static final ColumnPath PERSON_NAME = ColumnPath.of("person", "name");
    private static final ColumnPath PERSON_AGE = ColumnPath.of("person", "age");
    private static final ColumnPath PHONES = ColumnPath.of("phones");
    private static final ColumnPath ATTRS = ColumnPath.of("attrs");
    private static final ColumnPath ATTR_KEY = ColumnPath.of("key_value", "key");
    private static final ColumnPath ATTR_VALUE = ColumnPath.of("key_value", "value");

    @Test
    void readBatchesOverNestedColumnsMatchesReadAndCount(@TempDir Path tmp) throws Exception {
        Path file = writeNestedFixture(tmp);
        Predicate predicate = Pred.col("id").lt(ROW_COUNT / 2);

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            Set<Integer> viaRead = idsViaRead(reader, predicate, Projection.ALL);
            Set<Integer> viaBatches = idsViaBatches(reader, predicate, Projection.ALL);
            long viaCount = reader.count(predicate, ReadOptions.DEFAULTS);

            assertThat(viaBatches)
                    .as("readBatches matches read over a file with STRUCT, LIST, and MAP columns")
                    .isEqualTo(viaRead);
            assertThat(viaBatches.size())
                    .as("readBatches row count matches count()")
                    .isEqualTo((int) viaCount);
            assertThat(viaBatches.size())
                    .as("the predicate selects a strict subset of the %d rows", ROW_COUNT)
                    .isLessThan(ROW_COUNT);
        }
    }

    @Test
    void readBatchesProjectingANestedColumnWhileFilteringMatchesRead(@TempDir Path tmp) throws Exception {
        Path file = writeNestedFixture(tmp);
        Predicate predicate = Pred.col("id").lt(ROW_COUNT / 2);
        Projection projection = Projection.of(Set.of(PHONES, ATTRS));

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            List<String> viaRead = renderRowsViaRead(reader, predicate, projection);
            List<String> viaBatches = renderRowsViaBatches(reader, predicate, projection);

            assertThat(viaBatches)
                    .as("a projection that includes nested LIST and MAP columns reads without breaking the batch")
                    .containsExactlyInAnyOrderElementsOf(viaRead);
        }
    }

    // --- oracles ---

    private static Set<Integer> idsViaRead(ParquetFileReader reader, Predicate predicate, Projection projection) {
        Set<Integer> ids = new HashSet<>();
        try (Stream<ParquetRecord> records = reader.read(predicate, projection, ReadOptions.DEFAULTS)) {
            records.forEach(row -> ids.add(row.getInt(ID)));
        }
        return ids;
    }

    private static Set<Integer> idsViaBatches(ParquetFileReader reader, Predicate predicate, Projection projection) {
        Set<Integer> ids = new HashSet<>();
        try (Stream<ParquetRecordBatch> batches = reader.readBatches(predicate, projection, ReadOptions.DEFAULTS)) {
            batches.forEach(batch -> {
                try (ParquetRecordBatch owned = batch) {
                    for (int rowIndex = 0; rowIndex < owned.rowCount(); rowIndex++) {
                        ParquetRecord row = owned.materialize(rowIndex);
                        ids.add(row.getInt(ID));
                        forceNestedAccess(row);
                    }
                }
            });
        }
        return ids;
    }

    private static List<String> renderRowsViaRead(
            ParquetFileReader reader, Predicate predicate, Projection projection) {
        List<String> rendered = new ArrayList<>();
        try (Stream<ParquetRecord> records = reader.read(predicate, projection, ReadOptions.DEFAULTS)) {
            records.forEach(row -> rendered.add(renderNested(row)));
        }
        return rendered;
    }

    private static List<String> renderRowsViaBatches(
            ParquetFileReader reader, Predicate predicate, Projection projection) {
        List<String> rendered = new ArrayList<>();
        try (Stream<ParquetRecordBatch> batches = reader.readBatches(predicate, projection, ReadOptions.DEFAULTS)) {
            batches.forEach(batch -> {
                try (ParquetRecordBatch owned = batch) {
                    for (int rowIndex = 0; rowIndex < owned.rowCount(); rowIndex++) {
                        rendered.add(renderNested(owned.materialize(rowIndex)));
                    }
                }
            });
        }
        return rendered;
    }

    private static String renderNested(ParquetRecord row) {
        StringBuilder key = new StringBuilder();
        for (ColumnPath column : List.of(PHONES, ATTRS)) {
            key.append(column).append('=').append(renderValue(row.get(column))).append(';');
        }
        return key.toString();
    }

    private static void forceNestedAccess(ParquetRecord row) {
        String rendered = renderValue(row.get(PERSON)) + renderValue(row.get(PHONES)) + renderValue(row.get(ATTRS));
        assertThat(rendered).isNotNull();
    }

    /**
     * A content-stable rendering of a possibly-nested value. The default {@link MemorySegment#toString()} embeds the
     * off-heap address, which differs between the row path and the batch path even when the bytes match; this resolves
     * binary cells to their UTF-8 content so the two read paths render identically.
     */
    private static String renderValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof MemorySegment segment) {
            return new String(segment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
        }
        if (value instanceof List<?> list) {
            StringBuilder out = new StringBuilder("[");
            for (Object element : list) {
                out.append(renderValue(element)).append(',');
            }
            return out.append(']').toString();
        }
        if (value instanceof Map<?, ?> map) {
            StringBuilder out = new StringBuilder("{");
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                out.append(renderValue(entry.getKey()))
                        .append("->")
                        .append(renderValue(entry.getValue()))
                        .append(',');
            }
            return out.append('}').toString();
        }
        return value.getClass().getSimpleName();
    }

    // --- fixture ---

    private static Path writeNestedFixture(Path tmp) throws Exception {
        ParquetSchema schema = nestedSchema();
        Path file = tmp.resolve("nested.parquet");
        WriteOptions options = WriteOptions.builder().tempDir(tmp).build();

        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            for (int id = 0; id < ROW_COUNT; id++) {
                appendRow(appender, id);
            }
        }
        return file;
    }

    private static void appendRow(ParquetRecordBatchBuilder appender, int id) {
        appender.setInt(ID, id);

        appender.beginStruct(PERSON)
                .setString(PERSON_NAME, "name-" + id)
                .setInt(PERSON_AGE, 20 + id)
                .endStruct();

        appender.beginList(PHONES)
                .addString("phone-" + id + "-a")
                .addString("phone-" + id + "-b")
                .endList();

        appender.beginMap(ATTRS)
                .putEntry()
                .setString(ATTR_KEY, "city")
                .setString(ATTR_VALUE, "city-" + id)
                .endEntry()
                .endMap();

        appender.endRow();
    }

    /**
     * {@code required group schema { required int32 id; optional group person { optional binary name (STRING); optional
     * int32 age; } optional group phones (LIST) { repeated group list { optional binary element (STRING) } } optional
     * group attrs (MAP) { repeated group key_value { required binary key (STRING); optional binary value (STRING); } }
     * }}
     */
    private static ParquetSchema nestedSchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);

        SchemaNode.Primitive name = stringLeaf("name", Repetition.OPTIONAL);
        SchemaNode.Primitive age = new SchemaNode.Primitive(
                "age", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group person =
                new SchemaNode.Group("person", Repetition.OPTIONAL, List.of(name, age), Optional.empty(), -1);

        SchemaNode.Primitive element = stringLeaf("element", Repetition.OPTIONAL);
        SchemaNode.Group repeatedList =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group phones = new SchemaNode.Group(
                "phones", Repetition.OPTIONAL, List.of(repeatedList), Optional.of(new LogicalType.ListType()), -1);

        SchemaNode.Primitive key = stringLeaf("key", Repetition.REQUIRED);
        SchemaNode.Primitive value = stringLeaf("value", Repetition.OPTIONAL);
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        SchemaNode.Group attrs = new SchemaNode.Group(
                "attrs", Repetition.OPTIONAL, List.of(keyValue), Optional.of(new LogicalType.MapType()), -1);

        SchemaNode.Group root = new SchemaNode.Group(
                "schema", Repetition.REQUIRED, List.of(id, person, phones, attrs), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive stringLeaf(String fieldName, Repetition repetition) {
        return new SchemaNode.Primitive(
                fieldName,
                repetition,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
    }
}
