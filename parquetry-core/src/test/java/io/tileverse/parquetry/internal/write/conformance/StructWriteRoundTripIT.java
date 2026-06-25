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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Round-trip tests for struct-column write support: write rows via {@link ParquetRecordBatchBuilder#beginStruct} /
 * {@link ParquetRecordBatchBuilder#endStruct}, flush through {@link ParquetFileWriter}, read back via
 * {@link ParquetFileReader} and assert row-level equality.
 *
 * <p>These tests verify the engine path for nested columns: the Dremel striper fires, definition levels are baked into
 * data pages, and the assembler reconstructs the same null pattern on read.
 */
class StructWriteRoundTripIT {

    @TempDir
    Path tempDir;

    /**
     * Schema: {@code optional group s { optional int32 f; }}
     *
     * <p>Row 0: s present, f=7. Row 1: s null.
     */
    @Test
    void optionalStructWithOptionalField_twoRows() throws IOException {
        ParquetSchema schema = oneGroupOneField("s", Repetition.OPTIONAL, "f", Repetition.OPTIONAL);
        Path out = tempDir.resolve("struct_optional.parquet");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(out), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.beginStruct("s")
                    .setInt(ColumnPath.of("s", "f"), 7)
                    .endStruct()
                    .endRow();
            appender.setNull(ColumnPath.of("s")).endRow();
        }

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        assertThat(rows).hasSize(2);

        // Row 0: s.f = 7
        assertThat(rows.get(0).isNull(ColumnPath.of("s"))).isFalse();
        assertThat(rows.get(0).getInt(ColumnPath.of("s", "f"))).isEqualTo(7);

        // Row 1: s is null
        assertThat(rows.get(1).isNull(ColumnPath.of("s"))).isTrue();
    }

    /**
     * Schema: {@code required group s { optional int32 f; }}
     *
     * <p>Row 0: s present, f=42. Row 1: s present, f null.
     */
    @Test
    void requiredStructWithOptionalField() throws IOException {
        ParquetSchema schema = oneGroupOneField("s", Repetition.REQUIRED, "f", Repetition.OPTIONAL);
        Path out = tempDir.resolve("struct_required.parquet");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(out), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.beginStruct("s")
                    .setInt(ColumnPath.of("s", "f"), 42)
                    .endStruct()
                    .endRow();
            appender.beginStruct("s")
                    .setNull(ColumnPath.of("s", "f"))
                    .endStruct()
                    .endRow();
        }

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        assertThat(rows).hasSize(2);

        assertThat(rows.get(0).getInt(ColumnPath.of("s", "f"))).isEqualTo(42);
        assertThat(rows.get(1).isNull(ColumnPath.of("s", "f"))).isTrue();
    }

    /**
     * Schema: {@code optional int32 id; optional group s { optional int32 f; }}
     *
     * <p>Flat and struct columns coexist; the flat path must remain byte-identical.
     */
    @Test
    void flatAndStructColumnsRoundTrip() throws IOException {
        SchemaNode.Primitive idLeaf = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive fLeaf = new SchemaNode.Primitive(
                "f", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group sGroup = new SchemaNode.Group("s", Repetition.OPTIONAL, List.of(fLeaf), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(idLeaf, sGroup), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);
        Path out = tempDir.resolve("flat_and_struct.parquet");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(out), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            appender.setInt(ColumnPath.of("id"), 1)
                    .beginStruct("s")
                    .setInt(ColumnPath.of("s", "f"), 10)
                    .endStruct()
                    .endRow();
            appender.setNull(ColumnPath.of("id")).setNull(ColumnPath.of("s")).endRow();
        }

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        assertThat(rows).hasSize(2);

        assertThat(rows.get(0).getInt(ColumnPath.of("id"))).isEqualTo(1);
        assertThat(rows.get(0).getInt(ColumnPath.of("s", "f"))).isEqualTo(10);

        assertThat(rows.get(1).isNull(ColumnPath.of("id"))).isTrue();
        assertThat(rows.get(1).isNull(ColumnPath.of("s"))).isTrue();
    }

    /**
     * Schema: {@code optional group outer { optional group inner { optional int32 x; } }}
     *
     * <p>Verifies definition levels accumulate correctly across two optional struct ancestors.
     */
    @Test
    void doubleNestedStruct() throws IOException {
        SchemaNode.Primitive xLeaf = new SchemaNode.Primitive(
                "x", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group inner =
                new SchemaNode.Group("inner", Repetition.OPTIONAL, List.of(xLeaf), Optional.empty(), -1);
        SchemaNode.Group outer =
                new SchemaNode.Group("outer", Repetition.OPTIONAL, List.of(inner), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(outer), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);
        Path out = tempDir.resolve("double_nested.parquet");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(out), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            // Row 0: outer present, inner present, x=99.
            appender.beginStruct("outer")
                    .beginStruct(ColumnPath.of("outer", "inner"))
                    .setInt(ColumnPath.of("outer", "inner", "x"), 99)
                    .endStruct()
                    .endStruct()
                    .endRow();
            // Row 1: outer present, inner null.
            appender.beginStruct("outer")
                    .setNull(ColumnPath.of("outer", "inner"))
                    .endStruct()
                    .endRow();
            // Row 2: outer null.
            appender.setNull(ColumnPath.of("outer")).endRow();
        }

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        assertThat(rows).hasSize(3);

        assertThat(rows.get(0).getInt(ColumnPath.of("outer", "inner", "x"))).isEqualTo(99);
        assertThat(rows.get(1).isNull(ColumnPath.of("outer", "inner"))).isTrue();
        assertThat(rows.get(2).isNull(ColumnPath.of("outer"))).isTrue();
    }

    /**
     * FIX 1: a REQUIRED child inside an ABSENT optional struct must not throw and must round-trip correctly.
     *
     * <p>Schema: {@code optional group person { required binary name (STRING); optional int32 age; }}
     *
     * <p>Three rows: (0) person present with name set, (1) person null, (2) person present with name set. Row 1 must
     * not throw during write, and must read back as a null person.
     */
    @Test
    void requiredChildOfAbsentOptionalStructDoesNotThrow() throws IOException {
        SchemaNode.Primitive nameField = new SchemaNode.Primitive(
                "name", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive ageField = new SchemaNode.Primitive(
                "age", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group personGroup =
                new SchemaNode.Group("person", Repetition.OPTIONAL, List.of(nameField, ageField), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(personGroup), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);
        Path out = tempDir.resolve("required_child_absent_struct.parquet");

        WriteOptions opts = WriteOptions.builder().tempDir(tempDir).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(out), schema, opts)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            // Row 0: person present, name set.
            appender.beginStruct("person")
                    .setString(ColumnPath.of("person", "name"), "Alice")
                    .endStruct()
                    .endRow();
            // Row 1: person absent - must not throw despite required name not set.
            appender.endRow();
            // Row 2: person present, name set.
            appender.beginStruct("person")
                    .setString(ColumnPath.of("person", "name"), "Bob")
                    .endStruct()
                    .endRow();
        }

        List<ParquetRecord> rows = WriteConformanceSupport.readAll(out);
        assertThat(rows).hasSize(3);
        assertThat(rows.get(0).isNull(ColumnPath.of("person"))).isFalse();
        assertThat(rows.get(0).getString(ColumnPath.of("person", "name"))).isEqualTo("Alice");
        assertThat(rows.get(1).isNull(ColumnPath.of("person"))).isTrue();
        assertThat(rows.get(2).isNull(ColumnPath.of("person"))).isFalse();
        assertThat(rows.get(2).getString(ColumnPath.of("person", "name"))).isEqualTo("Bob");
    }

    // --- helpers ---

    private static ParquetSchema oneGroupOneField(
            String groupName, Repetition groupRep, String fieldName, Repetition fieldRep) {
        SchemaNode.Primitive field = new SchemaNode.Primitive(
                fieldName, fieldRep, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group group = new SchemaNode.Group(groupName, groupRep, List.of(field), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(group), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
