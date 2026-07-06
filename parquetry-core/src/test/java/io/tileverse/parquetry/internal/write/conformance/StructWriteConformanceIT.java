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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
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
 * Conformance gate for struct-column write output: parquetry writes a nested struct file and parquet-java reads it
 * back, verifying that the Dremel definition levels, group encoding, and footer representation comply with the format
 * spec.
 *
 * <p>Schema under test: {@code message schema { required int32 id; optional group person { optional binary name
 * (STRING); optional int32 age; } }}
 *
 * <p>Four rows cover the interesting null combinations: a fully-present person, a person with one null field, a null
 * person struct, and another fully-present person.
 */
@Tag("conformance")
class StructWriteConformanceIT {

    @TempDir
    Path tempDir;

    @Test
    void structColumnReadsCleanlyThroughParquetJava() throws Exception {
        ParquetSchema schema = personSchema();
        Path file = tempDir.resolve("struct-person.parquet");
        writePersonRows(file, schema);

        // Footer must parse without error.
        WriteConformanceSupport.readFooterViaParquetJava(file);

        List<GenericRecord> rows = WriteConformanceSupport.readWithAvro(file);
        assertThat(rows).hasSize(4);

        // Row 0: id=1, person present, name="Alice", age=30.
        assertThat(((Number) rows.get(0).get("id")).intValue()).isEqualTo(1);
        GenericRecord person0 = (GenericRecord) rows.get(0).get("person");
        assertThat(person0).isNotNull();
        assertThat(toUtf8String(person0.get("name"))).isEqualTo("Alice");
        assertThat(((Number) person0.get("age")).intValue()).isEqualTo(30);

        // Row 1: id=2, person present, name="Bob", age null.
        assertThat(((Number) rows.get(1).get("id")).intValue()).isEqualTo(2);
        GenericRecord person1 = (GenericRecord) rows.get(1).get("person");
        assertThat(person1).isNotNull();
        assertThat(toUtf8String(person1.get("name"))).isEqualTo("Bob");
        assertThat(person1.get("age")).isNull();

        // Row 2: id=3, person struct null.
        assertThat(((Number) rows.get(2).get("id")).intValue()).isEqualTo(3);
        assertThat(rows.get(2).get("person")).isNull();

        // Row 3: id=4, person present, name="Carol", age=25.
        assertThat(((Number) rows.get(3).get("id")).intValue()).isEqualTo(4);
        GenericRecord person3 = (GenericRecord) rows.get(3).get("person");
        assertThat(person3).isNotNull();
        assertThat(toUtf8String(person3.get("name"))).isEqualTo("Carol");
        assertThat(((Number) person3.get("age")).intValue()).isEqualTo(25);
    }

    // --- writer ---

    private void writePersonRows(Path file, ParquetSchema schema) throws IOException {
        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();

            // Row 0: fully-present person.
            appender.setInt(ColumnPath.of("id"), 1)
                    .beginStruct("person")
                    .setString(ColumnPath.of("person", "name"), "Alice")
                    .setInt(ColumnPath.of("person", "age"), 30)
                    .endStruct()
                    .endRow();

            // Row 1: person present, age null.
            appender.setInt(ColumnPath.of("id"), 2)
                    .beginStruct("person")
                    .setString(ColumnPath.of("person", "name"), "Bob")
                    .setNull(ColumnPath.of("person", "age"))
                    .endStruct()
                    .endRow();

            // Row 2: null person struct.
            appender.setInt(ColumnPath.of("id"), 3)
                    .setNull(ColumnPath.of("person"))
                    .endRow();

            // Row 3: another fully-present person.
            appender.setInt(ColumnPath.of("id"), 4)
                    .beginStruct("person")
                    .setString(ColumnPath.of("person", "name"), "Carol")
                    .setInt(ColumnPath.of("person", "age"), 25)
                    .endStruct()
                    .endRow();
        }
    }

    /**
     * FIX 1 conformance: parquet-java must read a file where a row has a null optional struct whose only child is a
     * REQUIRED field. The null struct row must not corrupt the file's definition levels.
     *
     * <p>Schema: {@code message schema { optional group person { required binary name (STRING); optional int32 age; }
     * }}
     *
     * <p>Rows: (0) person present with name "Alice", (1) person null, (2) person present with name "Bob".
     */
    @Test
    void requiredChildOfAbsentOptionalStructPassesParquetJava() throws Exception {
        SchemaNode.Primitive nameColumn = new SchemaNode.Primitive(
                "name",
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Primitive ageColumn = new SchemaNode.Primitive(
                "age", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group personGroup = new SchemaNode.Group(
                "person", Repetition.OPTIONAL, List.of(nameColumn, ageColumn), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(personGroup), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);

        Path file = tempDir.resolve("required-child-absent-struct.parquet");
        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.beginStruct("person")
                    .setString(ColumnPath.of("person", "name"), "Alice")
                    .endStruct()
                    .endRow();
            // Row 1: person null - required name not touched.
            appender.endRow();
            appender.beginStruct("person")
                    .setString(ColumnPath.of("person", "name"), "Bob")
                    .endStruct()
                    .endRow();
        }

        WriteConformanceSupport.readFooterViaParquetJava(file);

        List<GenericRecord> rows = WriteConformanceSupport.readWithAvro(file);
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).get("person")).isNotNull();
        assertThat(toUtf8String(((GenericRecord) rows.get(0).get("person")).get("name")))
                .isEqualTo("Alice");
        assertThat(rows.get(1).get("person")).isNull();
        assertThat(rows.get(2).get("person")).isNotNull();
        assertThat(toUtf8String(((GenericRecord) rows.get(2).get("person")).get("name")))
                .isEqualTo("Bob");
    }

    // --- schema ---

    /**
     * Builds: {@code message schema { required int32 id; optional group person { optional binary name (STRING);
     * optional int32 age; } }}
     */
    private static ParquetSchema personSchema() {
        SchemaNode.Primitive idColumn = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive nameColumn = new SchemaNode.Primitive(
                "name",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Primitive ageColumn = new SchemaNode.Primitive(
                "age", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group personGroup = new SchemaNode.Group(
                "person", Repetition.OPTIONAL, List.of(nameColumn, ageColumn), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group(
                "schema", Repetition.REQUIRED, List.of(idColumn, personGroup), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    // --- value helpers ---

    private static String toUtf8String(Object value) {
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
}
