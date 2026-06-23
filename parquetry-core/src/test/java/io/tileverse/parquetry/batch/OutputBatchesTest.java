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
package io.tileverse.parquetry.batch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.OutputColumn;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class OutputBatchesTest {

    @Test
    void shapesIntoOutputOrderWithReuseConstantAndWidening() {
        DefaultParquetRecordBatch base = sourceBatch();

        List<OutputColumn> output = List.of(
                new OutputColumn.Physical(ColumnPath.of("count"), ColumnPath.of("n")),
                new OutputColumn.Constant(ColumnPath.of("label"), new Value.StringVal("x")),
                new OutputColumn.Promoted(ColumnPath.of("big"), ColumnPath.of("n"), PrimitiveKind.INT64));

        ParquetRecordBatch shaped = OutputBatches.shape(base, output);

        assertThat(shaped.projectedSchema().leafColumns())
                .containsExactly(ColumnPath.of("count"), ColumnPath.of("label"), ColumnPath.of("big"));
        assertThat(shaped.columns().get(ColumnPath.of("count")))
                .isSameAs(base.columns().get(ColumnPath.of("n")));
        assertThat(((LongVector) shaped.columns().get(ColumnPath.of("big"))).getLong(0))
                .isEqualTo(10L);
        assertThat(((LongVector) shaped.columns().get(ColumnPath.of("big"))).getLong(1))
                .isEqualTo(20L);
        assertThat(shaped.rowCount()).isEqualTo(2);

        int countFieldId = leafFieldId(shaped, ColumnPath.of("count"));
        int bigFieldId = leafFieldId(shaped, ColumnPath.of("big"));
        assertThat(countFieldId).isNotEqualTo(bigFieldId);
    }

    @Test
    void addsAnAllNullColumn() {
        DefaultParquetRecordBatch base = sourceBatch();

        List<OutputColumn> output = List.of(
                new OutputColumn.Physical(ColumnPath.of("count"), ColumnPath.of("n")),
                new OutputColumn.Null(ColumnPath.of("label"), new Value.StringVal("")));

        ParquetRecordBatch shaped = OutputBatches.shape(base, output);

        assertThat(shaped.projectedSchema().leafColumns())
                .containsExactly(ColumnPath.of("count"), ColumnPath.of("label"));
        ColumnVector label = shaped.columns().get(ColumnPath.of("label"));
        assertThat(label.isNull(0)).isTrue();
        assertThat(label.isNull(1)).isTrue();
    }

    @Test
    void rejectsDuplicateOutputNames() {
        DefaultParquetRecordBatch base = sourceBatch();

        List<OutputColumn> output = List.of(
                new OutputColumn.Physical(ColumnPath.of("count"), ColumnPath.of("n")),
                new OutputColumn.Physical(ColumnPath.of("count"), ColumnPath.of("id")));

        assertThatThrownBy(() -> OutputBatches.shape(base, output))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate output column name");
    }

    private static int leafFieldId(ParquetRecordBatch batch, ColumnPath leaf) {
        SchemaNode node = batch.projectedSchema().find(leaf).orElseThrow();
        return node.fieldId();
    }

    @Test
    void closingShapedClosesBase() {
        DefaultParquetRecordBatch base = sourceBatch();
        AtomicBoolean baseClosed = new AtomicBoolean(false);
        base.attachReleaseAction(() -> baseClosed.set(true));

        ParquetRecordBatch shaped = OutputBatches.shape(
                base, List.of(new OutputColumn.Physical(ColumnPath.of("count"), ColumnPath.of("n"))));

        shaped.close();

        assertThat(baseClosed).isTrue();
    }

    private static DefaultParquetRecordBatch sourceBatch() {
        SchemaNode.Primitive idLeaf = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 1);
        SchemaNode.Primitive nLeaf = new SchemaNode.Primitive(
                "n", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 2);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(idLeaf, nLeaf), Optional.empty(), 0);
        ParquetSchema schema = new ParquetSchema(root);

        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("id"), IntVector.materialized(new int[] {1, 2}, Validity.allValid(2)));
        columns.put(ColumnPath.of("n"), IntVector.materialized(new int[] {10, 20}, Validity.allValid(2)));
        return DefaultParquetRecordBatch.ofHeap(schema, columns, 2);
    }
}
