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
package io.tileverse.parquetry.internal.write.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Conformance gate for LIST and MAP write output: parquetry writes nested repeated columns and parquet-java reads them
 * back, verifying that the Dremel repetition / definition levels, the three-level LIST and {@code key_value} MAP group
 * shapes, and the footer representation comply with the format spec. The null-versus-empty distinction for both lists
 * and maps must survive a round trip through parquet-java's Avro reader.
 */
@Tag("conformance")
class ListMapWriteConformanceIT {

    @TempDir
    Path tempDir;

    /**
     * Schema: {@code optional group nums (LIST) { repeated group list { optional int32 element; } }}.
     *
     * <p>Row 0: [10, 20, 30]. Row 1: empty list. Row 2: null list.
     */
    @Test
    void listColumnReadsCleanlyThroughParquetJava() throws Exception {
        ParquetSchema schema = listOfInt32Schema();
        Path file = tempDir.resolve("list-int32.parquet");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.beginList("nums")
                    .addInt(10)
                    .addInt(20)
                    .addInt(30)
                    .endList()
                    .endRow();
            appender.beginList("nums").endList().endRow();
            appender.setNull(ColumnPath.of("nums")).endRow();
        }

        MessageType parquetSchema = WriteConformanceSupport.readFooterViaParquetJava(file)
                .getFileMetaData()
                .getSchema();
        assertThreeLevelList(parquetSchema, "nums");

        List<GenericRecord> rows = WriteConformanceSupport.readWithAvro(file);
        assertThat(rows).hasSize(3);

        assertThat(intElements(rows.get(0).get("nums"))).containsExactly(10, 20, 30);
        assertThat(intElements(rows.get(1).get("nums")))
                .as("empty list reads back as an empty list")
                .isEmpty();
        assertThat(rows.get(2).get("nums")).as("null list reads back as null").isNull();
    }

    /**
     * Schema: {@code optional group tags (MAP) { repeated group key_value { required binary key (STRING); optional
     * binary value (STRING); } }}.
     *
     * <p>Row 0: {"a" -> "1", "b" -> "2"}. Row 1: empty map. Row 2: null map.
     */
    @Test
    void mapColumnReadsCleanlyThroughParquetJava() throws Exception {
        ParquetSchema schema = mapStringStringSchema();
        Path file = tempDir.resolve("map-string-string.parquet");

        ColumnPath keyPath = ColumnPath.of("key_value", "key");
        ColumnPath valuePath = ColumnPath.of("key_value", "value");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.beginMap("tags")
                    .putEntry()
                    .setString(keyPath, "a")
                    .setString(valuePath, "1")
                    .endEntry()
                    .putEntry()
                    .setString(keyPath, "b")
                    .setString(valuePath, "2")
                    .endEntry()
                    .endMap()
                    .endRow();
            appender.beginMap("tags").endMap().endRow();
            appender.setNull(ColumnPath.of("tags")).endRow();
        }

        MessageType parquetSchema = WriteConformanceSupport.readFooterViaParquetJava(file)
                .getFileMetaData()
                .getSchema();
        assertKeyValueMap(parquetSchema, "tags");

        List<GenericRecord> rows = WriteConformanceSupport.readWithAvro(file);
        assertThat(rows).hasSize(3);

        Map<String, String> map0 = avroMap(rows.get(0).get("tags"));
        assertThat(map0).hasSize(2).containsEntry("a", "1").containsEntry("b", "2");

        assertThat(avroMap(rows.get(1).get("tags")))
                .as("empty map reads back as an empty map")
                .isEmpty();
        assertThat(rows.get(2).get("tags")).as("null map reads back as null").isNull();
    }

    /**
     * Schema: {@code optional group addresses (LIST) { repeated group list { optional group element { optional binary
     * locality (STRING); optional int32 postcode; } } }}.
     *
     * <p>Row 0: two struct elements, the second with a null locality. Row 1: empty list. Row 2: null list.
     */
    @Test
    void listOfStructReadsCleanlyThroughParquetJava() throws Exception {
        ParquetSchema schema = listOfStructSchema();
        Path file = tempDir.resolve("list-struct.parquet");

        ColumnPath locality = ColumnPath.of("element", "locality");
        ColumnPath postcode = ColumnPath.of("element", "postcode");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, opts)) {
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
            appender.setNull(ColumnPath.of("addresses")).endRow();
        }

        MessageType parquetSchema = WriteConformanceSupport.readFooterViaParquetJava(file)
                .getFileMetaData()
                .getSchema();
        assertThreeLevelList(parquetSchema, "addresses");

        List<GenericRecord> rows = WriteConformanceSupport.readWithAvro(file);
        assertThat(rows).hasSize(3);

        List<?> items = (List<?>) rows.get(0).get("addresses");
        assertThat(items).hasSize(2);

        GenericRecord first = elementStruct(items.get(0));
        assertThat(toUtf8String(first.get("locality"))).isEqualTo("Rosario");
        assertThat(((Number) first.get("postcode")).intValue()).isEqualTo(2000);

        GenericRecord second = elementStruct(items.get(1));
        assertThat(second.get("locality"))
                .as("second element's locality is null")
                .isNull();
        assertThat(((Number) second.get("postcode")).intValue()).isEqualTo(3000);

        assertThat((List<?>) rows.get(1).get("addresses"))
                .as("empty list of structs reads back empty")
                .isEmpty();
        assertThat(rows.get(2).get("addresses"))
                .as("null list reads back as null")
                .isNull();
    }

    // --- footer schema assertions ---

    /**
     * Asserts the column is the standard three-level LIST: an {@code optional} group annotated LIST, holding one
     * {@code repeated} group named {@code list}, holding the element field.
     */
    private static void assertThreeLevelList(MessageType schema, String columnName) {
        Type column = schema.getType(columnName);
        assertThat(column.isPrimitive()).as("LIST column is a group").isFalse();
        assertThat(column.getLogicalTypeAnnotation())
                .as("LIST column has a list annotation")
                .hasToString("LIST");
        GroupType listGroup = column.asGroupType();
        assertThat(listGroup.getFieldCount()).isEqualTo(1);
        Type repeated = listGroup.getType(0);
        assertThat(repeated.getName()).isEqualTo("list");
        assertThat(repeated.getRepetition()).isEqualTo(Type.Repetition.REPEATED);
    }

    /**
     * Asserts the column is the standard MAP: a group annotated MAP holding one {@code repeated} group named
     * {@code key_value} whose first field is a {@code required} key.
     */
    private static void assertKeyValueMap(MessageType schema, String columnName) {
        Type column = schema.getType(columnName);
        assertThat(column.isPrimitive()).as("MAP column is a group").isFalse();
        assertThat(column.getLogicalTypeAnnotation())
                .as("MAP column has a map annotation")
                .hasToString("MAP");
        GroupType mapGroup = column.asGroupType();
        Type keyValue = mapGroup.getType(0);
        assertThat(keyValue.getName()).isEqualTo("key_value");
        assertThat(keyValue.getRepetition()).isEqualTo(Type.Repetition.REPEATED);
        GroupType keyValueGroup = keyValue.asGroupType();
        assertThat(keyValueGroup.getType("key").getRepetition()).isEqualTo(Type.Repetition.REQUIRED);
    }

    // --- avro value helpers ---

    /**
     * Extracts the int values of a three-level LIST as parquet-java's Avro reader presents it: an array whose items are
     * records with a single {@code element} field. The {@code element} field is itself nullable; a null slot reads back
     * as a record whose {@code element} is null.
     */
    private static List<Integer> intElements(Object listValue) {
        List<Integer> out = new ArrayList<>();
        for (Object item : (List<?>) listValue) {
            Object element = ((GenericRecord) item).get("element");
            out.add(element == null ? null : ((Number) element).intValue());
        }
        return out;
    }

    /** Unwraps a three-level list item record to the struct stored in its {@code element} field. */
    private static GenericRecord elementStruct(Object item) {
        return (GenericRecord) ((GenericRecord) item).get("element");
    }

    private static Map<String, String> avroMap(Object mapValue) {
        Map<String, String> out = new java.util.LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) mapValue).entrySet()) {
            out.put(toUtf8String(entry.getKey()), toUtf8String(entry.getValue()));
        }
        return out;
    }

    private static String toUtf8String(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Utf8 u) {
            return u.toString();
        }
        if (value instanceof CharSequence cs) {
            return cs.toString();
        }
        if (value instanceof byte[] b) {
            return new String(b, StandardCharsets.UTF_8);
        }
        throw new IllegalStateException("Unexpected string value type: " + value.getClass());
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
