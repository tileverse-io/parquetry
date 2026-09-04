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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Round-trip tests for a STRUCT column that contains a LIST or MAP field. The reader already assembles this shape;
 * these tests close the authoring side, exercising the nested container verbs that open a list or map relative to an
 * enclosing struct scope, then read the rows back and assert struct presence, list sizes, element values, and map
 * entries.
 */
class StructNestedListWriteRoundTripIT {

    @TempDir
    Path tempDir;

    private static final ColumnPath PERSON = ColumnPath.of("person");
    private static final ColumnPath PERSON_NAME = ColumnPath.of("person", "name");
    private static final ColumnPath PERSON_PHONES = ColumnPath.of("person", "phones");
    private static final ColumnPath PERSON_ATTRS = ColumnPath.of("person", "attrs");

    /**
     * Building a schema whose struct contains a list field must not throw, even before any row is authored. The reader
     * accepts this shape; the builder must accept its construction too.
     */
    @Test
    void buildingStructContainingListSchemaDoesNotThrow() {
        ParquetSchema schema = personWithPhonesSchema();
        assertThatCode(() -> ParquetRecordBatchBuilder.forSchema(schema)).doesNotThrowAnyException();
    }

    /**
     * Schema: {@code optional group person { optional binary name (STRING); optional group phones (LIST) { repeated
     * group list { optional binary element (STRING) } } } }.
     *
     * <p>Row 0: name "Alice", phones ["111", "222"]. Row 1: name "Bob", empty phones list. Row 2: null person. Row 3:
     * name "Carol", null phones.
     */
    @Test
    void structContainingListRoundTrip() throws IOException {
        ParquetSchema schema = personWithPhonesSchema();
        Path out = tempDir.resolve("person_phones.parquet");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(out), schema, opts)) {
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

            appender.beginStruct(PERSON)
                    .setString(PERSON_NAME, "Carol")
                    .setNull(PERSON_PHONES)
                    .endStruct()
                    .endRow();
        }

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        assertThat(rows).hasSize(4);

        ParquetRecord alice = (ParquetRecord) rows.get(0).get(PERSON);
        assertThat(asString(alice.get(ColumnPath.of("name")))).isEqualTo("Alice");
        assertThat(stringList(alice.get(ColumnPath.of("phones")))).containsExactly("111", "222");

        ParquetRecord bob = (ParquetRecord) rows.get(1).get(PERSON);
        assertThat(asString(bob.get(ColumnPath.of("name")))).isEqualTo("Bob");
        assertThat(asList(bob.get(ColumnPath.of("phones"))))
                .as("empty phones list reads as present, empty")
                .isEmpty();

        assertThat(rows.get(2).get(PERSON)).as("null person reads back as null").isNull();

        ParquetRecord carol = (ParquetRecord) rows.get(3).get(PERSON);
        assertThat(asString(carol.get(ColumnPath.of("name")))).isEqualTo("Carol");
        assertThat(carol.get(ColumnPath.of("phones")))
                .as("null phones list reads back as null")
                .isNull();
    }

    /**
     * Schema: {@code optional group person { optional binary name (STRING); optional group attrs (MAP) { repeated group
     * key_value { required binary key (STRING); optional binary value (STRING); } } } }.
     *
     * <p>Row 0: name "Alice", attrs {"city" -> "Rosario"}. Row 1: name "Bob", empty attrs. Row 2: null person.
     */
    @Test
    void structContainingMapRoundTrip() throws IOException {
        ParquetSchema schema = personWithAttrsSchema();
        Path out = tempDir.resolve("person_attrs.parquet");

        ColumnPath keyPath = ColumnPath.of("key_value", "key");
        ColumnPath valuePath = ColumnPath.of("key_value", "value");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(out), schema, opts)) {
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

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        assertThat(rows).hasSize(3);

        ParquetRecord alice = (ParquetRecord) rows.get(0).get(PERSON);
        assertThat(asString(alice.get(ColumnPath.of("name")))).isEqualTo("Alice");
        Map<String, String> attrs = stringMap(alice.get(ColumnPath.of("attrs")));
        assertThat(attrs).hasSize(1).containsEntry("city", "Rosario");

        ParquetRecord bob = (ParquetRecord) rows.get(1).get(PERSON);
        assertThat(asMap(bob.get(ColumnPath.of("attrs"))))
                .as("empty attrs map reads as present, empty")
                .isEmpty();

        assertThat(rows.get(2).get(PERSON)).as("null person reads back as null").isNull();
    }

    /**
     * Inside an open element scope a container path resolves RELATIVE to the element wrapper; a path that does not
     * anchor there must fail with a clear message, rather than producing wrong output.
     */
    @Test
    void openingContainerWithABadRelativePathThrowsClearly() {
        ParquetSchema schema = personWithPhonesSchema();
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);

        builder.beginStruct(PERSON).beginList(PERSON_PHONES).addElement();

        ColumnPath nested = ColumnPath.of("nested");
        assertThatThrownBy(() -> builder.beginList(nested))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("does not start with the expected node");
    }

    // --- value extraction ---

    @SuppressWarnings("unchecked")
    private static List<Object> asList(Object value) {
        return (List<Object>) value;
    }

    private static List<String> stringList(Object value) {
        List<String> out = new ArrayList<>();
        for (Object element : (List<?>) value) {
            out.add(asString(element));
        }
        return out;
    }

    private static Map<?, ?> asMap(Object value) {
        return (Map<?, ?>) value;
    }

    private static Map<String, String> stringMap(Object value) {
        Map<String, String> out = new LinkedHashMap<>();
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
