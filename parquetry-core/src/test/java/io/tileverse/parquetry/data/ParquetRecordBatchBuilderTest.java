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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class ParquetRecordBatchBuilderTest {

    @Test
    void buildsBatchFromTypedRows() {
        SchemaNode.Primitive idLeaf = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive nameLeaf = new SchemaNode.Primitive(
                "name", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(idLeaf, nameLeaf), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);

        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        builder.setInt(0, 1).setString(1, "a").endRow();
        builder.setInt(0, 2).setNull(1).endRow();
        ParquetRecordBatch batch = builder.build();

        assertThat(batch.rowCount()).isEqualTo(2);
        ParquetRecord r0 = batch.materialize(0);
        assertThat(r0.getInt(0)).isEqualTo(1);
        assertThat(r0.getString(1)).isEqualTo("a");
        assertThat(batch.materialize(1).isNull(1)).isTrue();
    }

    @Test
    void requiredColumnUnsetAtEndRowThrows() {
        SchemaNode.Primitive idLeaf = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(idLeaf), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);

        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        assertThatThrownBy(builder::endRow)
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("id");
    }

    @Test
    void setNullOnRequiredColumnThrowsByIndex() {
        ParquetRecordBatchBuilder builder = builderWithRequiredId();
        assertThatThrownBy(() -> builder.setNull(0))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("id");
    }

    @Test
    void setNullOnRequiredColumnThrowsByPath() {
        ParquetRecordBatchBuilder builder = builderWithRequiredId();
        assertThatThrownBy(() -> builder.setNull(ColumnPath.of("id")))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("id");
    }

    private static ParquetRecordBatchBuilder builderWithRequiredId() {
        SchemaNode.Primitive idLeaf = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(idLeaf), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);
        return ParquetRecordBatchBuilder.forSchema(schema);
    }

    @Test
    void rejectsRepeatedLeaf() {
        SchemaNode.Primitive repeated = new SchemaNode.Primitive(
                "tags", Repetition.REPEATED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(repeated), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);

        assertThatThrownBy(() -> ParquetRecordBatchBuilder.forSchema(schema)).isInstanceOf(ParquetWriteException.class);
    }

    @Test
    void resolvesColumnByPath() {
        SchemaNode.Primitive idLeaf = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(idLeaf), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);

        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        builder.setInt(ColumnPath.of("id"), 42).endRow();
        ParquetRecordBatch batch = builder.build();

        assertThat(batch.materialize(0).getInt(0)).isEqualTo(42);
    }

    @Test
    void buildsMixedKindRowsWithNullAcrossRows() {
        SchemaNode.Primitive flagLeaf = new SchemaNode.Primitive(
                "flag", Repetition.OPTIONAL, PrimitiveKind.BOOLEAN, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive countLeaf = new SchemaNode.Primitive(
                "count", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive ratioLeaf = new SchemaNode.Primitive(
                "ratio", Repetition.OPTIONAL, PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive labelLeaf = new SchemaNode.Primitive(
                "label", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group(
                "schema",
                Repetition.REQUIRED,
                List.of(flagLeaf, countLeaf, ratioLeaf, labelLeaf),
                Optional.empty(),
                -1);
        ParquetSchema schema = new ParquetSchema(root);

        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        builder.setBoolean(0, true)
                .setLong(1, 100L)
                .setDouble(2, 1.5)
                .setString(3, "first")
                .endRow();
        builder.setBoolean(0, false)
                .setLong(1, 200L)
                .setDouble(2, 2.5)
                .setNull(3)
                .endRow();
        ParquetRecordBatch batch = builder.build();

        assertThat(batch.rowCount()).isEqualTo(2);
        ParquetRecord r0 = batch.materialize(0);
        assertThat(r0.getBoolean(0)).isTrue();
        assertThat(r0.getLong(1)).isEqualTo(100L);
        assertThat(r0.getDouble(2)).isEqualTo(1.5);
        assertThat(r0.getString(3)).isEqualTo("first");

        ParquetRecord r1 = batch.materialize(1);
        assertThat(r1.getBoolean(0)).isFalse();
        assertThat(r1.getLong(1)).isEqualTo(200L);
        assertThat(r1.getDouble(2)).isEqualTo(2.5);
        assertThat(r1.isNull(3)).isTrue();
    }
}
