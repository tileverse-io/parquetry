/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
 * Conformance gate for a STRUCT column that contains a LIST or MAP field: parquetry authors the struct-nested container
 * through the scope verbs, and parquet-java reads the file back. The struct's footer shape (a plain group enclosing a
 * three-level LIST or a {@code key_value} MAP) and the per-row null-versus-empty distinction must survive a round trip
 * through parquet-java's Avro reader.
 */
@Tag("conformance")
class StructNestedListWriteConformanceIT {

    @TempDir
    Path tempDir;

    private static final ColumnPath PERSON = ColumnPath.of("person");
    private static final ColumnPath PERSON_NAME = ColumnPath.of("person", "name");
    private static final ColumnPath PERSON_PHONES = ColumnPath.of("person", "phones");
    private static final ColumnPath PERSON_ATTRS = ColumnPath.of("person", "attrs");

    /**
     * Schema: {@code optional group person { optional binary name (STRING); optional group phones (LIST) { repeated
     * group list { optional binary element (STRING) } } } }.
     *
     * <p>Row 0: name "Alice", phones ["111", "222"]. Row 1: name "Bob", empty phones. Row 2: null person.
     */
    @Test
    void structContainingListReadsCleanlyThroughParquetJava() throws Exception {
        ParquetSchema schema = personWithPhonesSchema();
        Path file = tempDir.resolve("person-phones.parquet");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();

            appender.beginStruct(PERSON)
                    .setString(PERSON_NAME, "Alice")
                    .beginList(PERSON_PHONES)
                    .addString("111")
                    .addString("222")
                    .endList()
                    .endStruct()
                    .endRow();

            appender.beginStruct(PERSON)
                    .setString(PERSON_NAME, "Bob")
                    .beginList(PERSON_PHONES)
                    .endList()
                    .endStruct()
                    .endRow();

            appender.setNull(PERSON).endRow();
        }

        MessageType parquetSchema = WriteConformanceSupport.readFooterViaParquetJava(file)
                .getFileMetaData()
                .getSchema();
        assertStructWithThreeLevelList(parquetSchema, "person", "phones");

        List<GenericRecord> rows = WriteConformanceSupport.readWithAvro(file);
        assertThat(rows).hasSize(3);

        GenericRecord alice = (GenericRecord) rows.get(0).get("person");
        assertThat(toUtf8String(alice.get("name"))).isEqualTo("Alice");
        assertThat(stringElements(alice.get("phones"))).containsExactly("111", "222");

        GenericRecord bob = (GenericRecord) rows.get(1).get("person");
        assertThat(toUtf8String(bob.get("name"))).isEqualTo("Bob");
        assertThat((List<?>) bob.get("phones"))
                .as("empty nested list reads back empty")
                .isEmpty();

        assertThat(rows.get(2).get("person"))
                .as("null struct reads back as null")
                .isNull();
    }

    /**
     * Schema: {@code optional group person { optional binary name (STRING); optional group attrs (MAP) { repeated group
     * key_value { required binary key (STRING); optional binary value (STRING); } } } }.
     *
     * <p>Row 0: name "Alice", attrs {"city" -> "Rosario"}. Row 1: name "Bob", empty attrs. Row 2: null person.
     */
    @Test
    void structContainingMapReadsCleanlyThroughParquetJava() throws Exception {
        ParquetSchema schema = personWithAttrsSchema();
        Path file = tempDir.resolve("person-attrs.parquet");

        ColumnPath keyPath = ColumnPath.of("key_value", "key");
        ColumnPath valuePath = ColumnPath.of("key_value", "value");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();

            appender.beginStruct(PERSON)
                    .setString(PERSON_NAME, "Alice")
                    .beginMap(PERSON_ATTRS)
                    .putEntry()
                    .setString(keyPath, "city")
                    .setString(valuePath, "Rosario")
                    .endEntry()
                    .endMap()
                    .endStruct()
                    .endRow();

            appender.beginStruct(PERSON)
                    .setString(PERSON_NAME, "Bob")
                    .beginMap(PERSON_ATTRS)
                    .endMap()
                    .endStruct()
                    .endRow();

            appender.setNull(PERSON).endRow();
        }

        MessageType parquetSchema = WriteConformanceSupport.readFooterViaParquetJava(file)
                .getFileMetaData()
                .getSchema();
        assertStructWithKeyValueMap(parquetSchema, "person", "attrs");

        List<GenericRecord> rows = WriteConformanceSupport.readWithAvro(file);
        assertThat(rows).hasSize(3);

        GenericRecord alice = (GenericRecord) rows.get(0).get("person");
        assertThat(toUtf8String(alice.get("name"))).isEqualTo("Alice");
        Map<String, String> attrs = avroMap(alice.get("attrs"));
        assertThat(attrs).hasSize(1).containsEntry("city", "Rosario");

        GenericRecord bob = (GenericRecord) rows.get(1).get("person");
        assertThat(avroMap(bob.get("attrs")))
                .as("empty nested map reads back empty")
                .isEmpty();

        assertThat(rows.get(2).get("person"))
                .as("null struct reads back as null")
                .isNull();
    }

    // --- footer schema assertions ---

    private static void assertStructWithThreeLevelList(MessageType schema, String structName, String listFieldName) {
        Type structType = schema.getType(structName);
        assertThat(structType.isPrimitive()).as("struct column is a group").isFalse();
        GroupType structGroup = structType.asGroupType();
        Type listType = structGroup.getType(listFieldName);
        assertThat(listType.getLogicalTypeAnnotation())
                .as("nested LIST field has a list annotation")
                .hasToString("LIST");
        GroupType listGroup = listType.asGroupType();
        assertThat(listGroup.getFieldCount()).isEqualTo(1);
        Type repeated = listGroup.getType(0);
        assertThat(repeated.getName()).isEqualTo("list");
        assertThat(repeated.getRepetition()).isEqualTo(Type.Repetition.REPEATED);
    }

    private static void assertStructWithKeyValueMap(MessageType schema, String structName, String mapFieldName) {
        Type structType = schema.getType(structName);
        assertThat(structType.isPrimitive()).as("struct column is a group").isFalse();
        GroupType structGroup = structType.asGroupType();
        Type mapType = structGroup.getType(mapFieldName);
        assertThat(mapType.getLogicalTypeAnnotation())
                .as("nested MAP field has a map annotation")
                .hasToString("MAP");
        GroupType mapGroup = mapType.asGroupType();
        Type keyValue = mapGroup.getType(0);
        assertThat(keyValue.getName()).isEqualTo("key_value");
        assertThat(keyValue.getRepetition()).isEqualTo(Type.Repetition.REPEATED);
        GroupType keyValueGroup = keyValue.asGroupType();
        assertThat(keyValueGroup.getType("key").getRepetition()).isEqualTo(Type.Repetition.REQUIRED);
    }

    // --- avro value helpers ---

    /**
     * Extracts the string values of a three-level LIST as parquet-java's Avro reader presents it: an array whose items
     * are records with a single {@code element} field.
     */
    private static List<String> stringElements(Object listValue) {
        List<String> out = new ArrayList<>();
        for (Object item : (List<?>) listValue) {
            Object element = ((GenericRecord) item).get("element");
            out.add(toUtf8String(element));
        }
        return out;
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

    private static ParquetSchema personWithPhonesSchema() {
        SchemaNode.Primitive name = stringLeaf("name", Repetition.OPTIONAL);
        SchemaNode.Primitive element = stringLeaf("element", Repetition.OPTIONAL);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group phones = new SchemaNode.Group(
                "phones", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        SchemaNode.Group person =
                new SchemaNode.Group("person", Repetition.OPTIONAL, List.of(name, phones), Optional.empty(), -1);
        return WriteConformanceSupport.rootOf(person);
    }

    private static ParquetSchema personWithAttrsSchema() {
        SchemaNode.Primitive name = stringLeaf("name", Repetition.OPTIONAL);
        SchemaNode.Primitive key = stringLeaf("key", Repetition.REQUIRED);
        SchemaNode.Primitive value = stringLeaf("value", Repetition.OPTIONAL);
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        SchemaNode.Group attrs = new SchemaNode.Group(
                "attrs", Repetition.OPTIONAL, List.of(keyValue), Optional.of(new LogicalType.MapType()), -1);
        SchemaNode.Group person =
                new SchemaNode.Group("person", Repetition.OPTIONAL, List.of(name, attrs), Optional.empty(), -1);
        return WriteConformanceSupport.rootOf(person);
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
