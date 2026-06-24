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
package io.tileverse.parquetry.internal.write.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Round-trip tests for LIST and MAP column write support: author rows via {@link ParquetRecordBatchBuilder#beginList} /
 * {@link ParquetRecordBatchBuilder#beginMap}, flush through {@link ParquetWriter}, read back via {@link ParquetReader},
 * and assert the reconstructed list sizes, element values, map entries, and nested struct fields match what was
 * authored.
 *
 * <p>These tests close the striper to engine to reader loop: the Dremel striper synthesizes the repetition and
 * definition levels for each repeated leaf, the column chunk writer bakes them into data pages, and the read-side
 * assembler reconstructs the same nesting and null pattern. The record API returns an outermost LIST column as a
 * {@link List} (null for an absent list, empty for a present empty list) and a MAP column as a {@link Map}.
 */
class ListMapWriteRoundTripIT {

    @TempDir
    Path tempDir;

    private static final ColumnPath NUMS = ColumnPath.of("nums");
    private static final ColumnPath TAGS = ColumnPath.of("tags");
    private static final ColumnPath ADDRESSES = ColumnPath.of("addresses");

    /**
     * Schema: {@code optional group nums (LIST) { repeated group list { optional int32 element; } }}.
     *
     * <p>Row 0: [10, 20, 30]. Row 1: empty list. Row 2: null list. Row 3: [40, null, 60].
     */
    @Test
    void listOfInt32RoundTrip() throws IOException {
        ParquetSchema schema = listOfInt32Schema();
        Path out = tempDir.resolve("list_int32.parquet");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(out), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.beginList("nums")
                    .addInt(10)
                    .addInt(20)
                    .addInt(30)
                    .endList()
                    .endRow();
            appender.beginList("nums").endList().endRow();
            appender.setNull(NUMS).endRow();
            appender.beginList("nums").addInt(40).addNull().addInt(60).endList().endRow();
        }

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        assertThat(rows).hasSize(4);

        assertThat(asList(rows.get(0).get(NUMS)))
                .containsExactly(Integer.valueOf(10), Integer.valueOf(20), Integer.valueOf(30));
        assertThat(asList(rows.get(1).get(NUMS)))
                .as("empty list reads as a present, empty List")
                .isEmpty();
        assertThat(rows.get(2).get(NUMS)).as("null list reads as null").isNull();
        assertThat(asList(rows.get(3).get(NUMS)))
                .as("a null element survives as a null slot")
                .containsExactly(Integer.valueOf(40), null, Integer.valueOf(60));
    }

    /**
     * Schema: {@code optional group tags (MAP) { repeated group key_value { required binary key (STRING); optional
     * binary value (STRING); } }}.
     *
     * <p>Row 0: {"a" -> "1", "b" -> null}. Row 1: empty map. Row 2: null map.
     */
    @Test
    void mapStringStringRoundTrip() throws IOException {
        ParquetSchema schema = mapStringStringSchema();
        Path out = tempDir.resolve("map_string_string.parquet");

        ColumnPath keyPath = ColumnPath.of("key_value", "key");
        ColumnPath valuePath = ColumnPath.of("key_value", "value");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(out), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.beginMap("tags")
                    .putEntry()
                    .setString(keyPath, "a")
                    .setString(valuePath, "1")
                    .endEntry()
                    .putEntry()
                    .setString(keyPath, "b")
                    .setNull(valuePath)
                    .endEntry()
                    .endMap()
                    .endRow();
            appender.beginMap("tags").endMap().endRow();
            appender.setNull(TAGS).endRow();
        }

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        assertThat(rows).hasSize(3);

        Map<String, String> map0 = stringMap(rows.get(0).get(TAGS));
        assertThat(map0).hasSize(2).containsEntry("a", "1");
        assertThat(map0).as("a null map value reads back as null").containsEntry("b", null);

        assertThat(asMap(rows.get(1).get(TAGS)))
                .as("empty map reads as a present, empty Map")
                .isEmpty();
        assertThat(rows.get(2).get(TAGS)).as("null map reads as null").isNull();
    }

    /**
     * Legacy two-level list whose repeated child is itself the primitive element: {@code optional group nums (LIST) {
     * repeated int32 element; }}. The leaf primitive's own repetition is REPEATED, the shape the row-group writer used
     * to reject outright.
     *
     * <p>Row 0: [1, 2]. Row 1: empty list. Row 2: null list.
     */
    @Test
    void twoLevelRepeatedPrimitiveListRoundTrip() throws IOException {
        ParquetSchema schema = twoLevelListOfInt32Schema();
        Path out = tempDir.resolve("two_level_list_int32.parquet");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(out), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.beginList("nums").addInt(1).addInt(2).endList().endRow();
            appender.beginList("nums").endList().endRow();
            appender.setNull(NUMS).endRow();
        }

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        assertThat(rows).hasSize(3);

        assertThat(asList(rows.get(0).get(NUMS))).containsExactly(Integer.valueOf(1), Integer.valueOf(2));
        assertThat(asList(rows.get(1).get(NUMS)))
                .as("empty two-level list reads as present, empty")
                .isEmpty();
        assertThat(rows.get(2).get(NUMS))
                .as("null two-level list reads as null")
                .isNull();
    }

    /**
     * Schema: {@code optional group addresses (LIST) { repeated group list { optional group element { optional binary
     * locality (STRING); optional int32 postcode; } } }}.
     *
     * <p>Row 0: two struct elements, the second with a null locality. Row 1: empty list. Row 2: null list.
     */
    @Test
    void listOfStructRoundTrip() throws IOException {
        ParquetSchema schema = listOfStructSchema();
        Path out = tempDir.resolve("list_struct.parquet");

        ColumnPath locality = ColumnPath.of("element", "locality");
        ColumnPath postcode = ColumnPath.of("element", "postcode");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(out), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.beginList("addresses")
                    .addElement()
                    .setString(locality, "Rosario")
                    .setInt(postcode, 2000)
                    .endElement()
                    .addElement()
                    .setNull(locality)
                    .setInt(postcode, 3000)
                    .endElement()
                    .endList()
                    .endRow();
            appender.beginList("addresses").endList().endRow();
            appender.setNull(ADDRESSES).endRow();
        }

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        assertThat(rows).hasSize(3);

        List<?> elements = asList(rows.get(0).get(ADDRESSES));
        assertThat(elements).hasSize(2);

        ParquetRecord first = (ParquetRecord) elements.get(0);
        assertThat(asString(first.get(ColumnPath.of("locality")))).isEqualTo("Rosario");
        assertThat(first.get(ColumnPath.of("postcode"))).isEqualTo(2000);

        ParquetRecord second = (ParquetRecord) elements.get(1);
        assertThat(second.get(ColumnPath.of("locality")))
                .as("the second element's locality is null")
                .isNull();
        assertThat(second.get(ColumnPath.of("postcode"))).isEqualTo(3000);

        assertThat(asList(rows.get(1).get(ADDRESSES)))
                .as("empty list of structs reads as no elements")
                .isEmpty();
        assertThat(rows.get(2).get(ADDRESSES)).as("null list reads as null").isNull();
    }

    // --- value extraction ---

    @SuppressWarnings("unchecked")
    private static List<Object> asList(Object value) {
        return (List<Object>) value;
    }

    private static Map<?, ?> asMap(Object value) {
        return (Map<?, ?>) value;
    }

    private static Map<String, String> stringMap(Object value) {
        Map<String, String> out = new java.util.LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            out.put(asString(entry.getKey()), asString(entry.getValue()));
        }
        return out;
    }

    private static String asString(Object value) {
        if (value == null) {
            return null;
        }
        MemorySegment segment = (MemorySegment) value;
        return new String(segment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    // --- schemas ---

    private static ParquetSchema listOfInt32Schema() {
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group listGroup = new SchemaNode.Group(
                "nums", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        return WriteConformanceSupport.rootOf(listGroup);
    }

    private static ParquetSchema twoLevelListOfInt32Schema() {
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.REPEATED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group listGroup = new SchemaNode.Group(
                "nums", Repetition.OPTIONAL, List.of(element), Optional.of(new LogicalType.ListType()), -1);
        return WriteConformanceSupport.rootOf(listGroup);
    }

    private static ParquetSchema mapStringStringSchema() {
        SchemaNode.Primitive key = new SchemaNode.Primitive(
                "key",
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                "value",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        SchemaNode.Group mapGroup = new SchemaNode.Group(
                "tags", Repetition.OPTIONAL, List.of(keyValue), Optional.of(new LogicalType.MapType()), -1);
        return WriteConformanceSupport.rootOf(mapGroup);
    }

    private static ParquetSchema listOfStructSchema() {
        SchemaNode.Primitive locality = new SchemaNode.Primitive(
                "locality",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Primitive postcode = new SchemaNode.Primitive(
                "postcode", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group element =
                new SchemaNode.Group("element", Repetition.OPTIONAL, List.of(locality, postcode), Optional.empty(), -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group listGroup = new SchemaNode.Group(
                "addresses", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        return WriteConformanceSupport.rootOf(listGroup);
    }
}
