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
package io.tileverse.parquetry.cli.cmd;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.cli.UnsupportedSchemaException;
import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class RecordCopierTest {

    @Test
    void flatPrimitiveSchemaIsSupported() {
        assertThatCode(() -> RecordCopier.requireWritable(Fixtures.citiesSchema()))
                .doesNotThrowAnyException();
    }

    @Test
    void structGroupIsSupported() {
        SchemaNode.Primitive child = new SchemaNode.Primitive(
                "x", Repetition.REQUIRED, PrimitiveKind.FLOAT, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group inner =
                new SchemaNode.Group("addr", Repetition.OPTIONAL, List.of(child), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(inner), Optional.empty(), -1);
        assertThatCode(() -> RecordCopier.requireWritable(new ParquetSchema(root)))
                .doesNotThrowAnyException();
    }

    @Test
    void repeatedGroupIsRejected() {
        SchemaNode.Primitive child = new SchemaNode.Primitive(
                "x", Repetition.REQUIRED, PrimitiveKind.FLOAT, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group inner =
                new SchemaNode.Group("items", Repetition.REPEATED, List.of(child), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(inner), Optional.empty(), -1);
        assertThatThrownBy(() -> RecordCopier.requireWritable(new ParquetSchema(root)))
                .isInstanceOf(UnsupportedSchemaException.class);
    }

    @Test
    void repeatedLeafIsRejected() {
        SchemaNode.Primitive tags = new SchemaNode.Primitive(
                "tags",
                Repetition.REPEATED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(tags), Optional.empty(), -1);
        assertThatThrownBy(() -> RecordCopier.requireWritable(new ParquetSchema(root)))
                .hasMessageContaining("repeated");
    }

    @Test
    void int96IsRejected() {
        SchemaNode.Primitive ts = new SchemaNode.Primitive(
                "ts", Repetition.OPTIONAL, PrimitiveKind.INT96, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(ts), Optional.empty(), -1);
        assertThatThrownBy(() -> RecordCopier.requireWritable(new ParquetSchema(root)))
                .hasMessageContaining("INT96");
    }

    @Test
    void variantIsRejected() {
        SchemaNode.Primitive payload = new SchemaNode.Primitive(
                "payload",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.Variant()),
                -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(payload), Optional.empty(), -1);
        assertThatThrownBy(() -> RecordCopier.requireWritable(new ParquetSchema(root)))
                .hasMessageContaining("Variant");
    }

    @Test
    void variantGroupIsRejected() {
        SchemaNode.Primitive metadata = new SchemaNode.Primitive(
                "metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                "value", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group payload = new SchemaNode.Group(
                "payload", Repetition.OPTIONAL, List.of(metadata, value), Optional.of(new LogicalType.Variant()), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(payload), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);
        assertThatThrownBy(() -> RecordCopier.requireWritable(schema))
                .isInstanceOf(UnsupportedSchemaException.class)
                .hasMessageContaining("Variant");
    }

    @Test
    void copyIntoReproducesLeafValues() {
        ParquetSchema schema = Fixtures.citiesSchema();
        ParquetRecord source = buildOneRow(schema);

        ParquetRecordBatchBuilder copy = ParquetRecordBatchBuilder.forSchema(schema);
        RecordCopier.forSchema(schema).copyInto(copy, source);
        ParquetRecord copied = copy.build().materialize(0);

        assertThat(copied.getInt(Fixtures.ID)).isEqualTo(7);
        assertThat(copied.getString(Fixtures.NAME)).isEqualTo("Rosario");
        assertThat(copied.getLong(Fixtures.POP)).isEqualTo(900_000L);
        assertThat(copied.getBoolean(Fixtures.CAPITAL)).isTrue();
    }

    @Test
    void copyIntoReproducesMixedKindRowIncludingNull() {
        ParquetSchema schema = Fixtures.citiesSchema();
        ParquetRecord source = buildNullNameRow(schema);

        ParquetRecordBatchBuilder copy = ParquetRecordBatchBuilder.forSchema(schema);
        RecordCopier.forSchema(schema).copyInto(copy, source);
        ParquetRecord copied = copy.build().materialize(0);

        assertThat(copied.getInt(Fixtures.ID)).isEqualTo(42);
        assertThat(copied.isNull(Fixtures.NAME)).isTrue();
        assertThat(copied.getLong(Fixtures.POP)).isEqualTo(500L);
        assertThat(copied.getBoolean(Fixtures.CAPITAL)).isFalse();
    }

    @Test
    void appenderAndRecordShareLeafOrder() {
        ParquetSchema schema = Fixtures.citiesSchema();
        ParquetRecord record = buildOneRow(schema);

        List<ColumnPath> leaves = schema.leafColumns();
        for (int i = 0; i < leaves.size(); i++) {
            assertThat(record.columnPath(i)).isEqualTo(leaves.get(i));
        }
    }

    private static ParquetRecord buildOneRow(ParquetSchema schema) {
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        builder.setInt(Fixtures.ID, 7);
        builder.setString(Fixtures.NAME, "Rosario");
        builder.setLong(Fixtures.POP, 900_000L);
        builder.setBoolean(Fixtures.CAPITAL, true);
        builder.endRow();
        ParquetRecordBatch batch = builder.build();
        return batch.materialize(0);
    }

    private static ParquetRecord buildNullNameRow(ParquetSchema schema) {
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        builder.setInt(Fixtures.ID, 42);
        builder.setNull(Fixtures.NAME);
        builder.setLong(Fixtures.POP, 500L);
        builder.setBoolean(Fixtures.CAPITAL, false);
        builder.endRow();
        ParquetRecordBatch batch = builder.build();
        return batch.materialize(0);
    }
}
