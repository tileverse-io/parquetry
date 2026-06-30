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
package io.tileverse.parquetry.columnar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.SequencedSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class OutputBatchesTest {

    @Test
    void producesAnAllNullColumn() {
        DefaultParquetRecordBatch base = sourceBatch();
        SequencedSet<Projection.Column> columns = new LinkedHashSet<>(List.of(
                new Projection.Column.Physical(ColumnPath.of("count"), ColumnPath.of("n")),
                new Projection.Column.Null(ColumnPath.of("label"), new Value.StringVal(""))));
        Projection.Of of = (Projection.Of) Projection.of(columns);

        ParquetRecordBatch produced =
                OutputBatches.produce(base, of, OutputBatches.producedSchema(base.projectedSchema(), of));

        assertThat(produced.projectedSchema().leafColumns())
                .containsExactly(ColumnPath.of("count"), ColumnPath.of("label"));
        ColumnVector label = produced.columns().get(ColumnPath.of("label"));
        assertThat(label.isNull(0)).isTrue();
        assertThat(label.isNull(1)).isTrue();
    }

    @Test
    void producesIntoProduceOrderWithConstantAndWidening() {
        DefaultParquetRecordBatch base = sourceBatch();
        SequencedSet<Projection.Column> columns = new LinkedHashSet<>(List.of(
                new Projection.Column.Physical(ColumnPath.of("count"), ColumnPath.of("n")),
                new Projection.Column.Constant(ColumnPath.of("label"), new Value.StringVal("x")),
                new Projection.Column.Promoted(ColumnPath.of("big"), ColumnPath.of("n"), PrimitiveKind.INT64)));
        Projection.Of of = (Projection.Of) Projection.of(columns);

        ParquetSchema producedSchema = OutputBatches.producedSchema(base.projectedSchema(), of);
        ParquetRecordBatch produced = OutputBatches.produce(base, of, producedSchema);

        assertThat(produced.projectedSchema().leafColumns())
                .containsExactly(ColumnPath.of("count"), ColumnPath.of("label"), ColumnPath.of("big"));
        assertThat(produced.columns().get(ColumnPath.of("count")))
                .isSameAs(base.columns().get(ColumnPath.of("n")));
        assertThat(((LongVector) produced.columns().get(ColumnPath.of("big"))).getLong(0))
                .isEqualTo(10L);
        assertThat(produced.rowCount()).isEqualTo(2);
    }

    @Test
    void producesARowPositionColumnFromTheSynthesizedVector() {
        DefaultParquetRecordBatch base = sourceBatchWithRowPosition();
        SequencedSet<Projection.Column> columns = new LinkedHashSet<>(List.of(
                new Projection.Column.Physical(ColumnPath.of("count"), ColumnPath.of("n")),
                new Projection.Column.RowPosition(ColumnPath.of("_pos"), 0L)));
        Projection.Of of = (Projection.Of) Projection.of(columns);

        ParquetSchema producedSchema = OutputBatches.producedSchema(base.projectedSchema(), of);
        ParquetRecordBatch produced = OutputBatches.produce(base, of, producedSchema);

        assertThat(produced.projectedSchema().leafColumns())
                .containsExactly(ColumnPath.of("count"), ColumnPath.of("_pos"));
        SchemaNode.Primitive posLeaf = (SchemaNode.Primitive)
                produced.projectedSchema().find(ColumnPath.of("_pos")).orElseThrow();
        assertThat(posLeaf.kind()).isEqualTo(PrimitiveKind.INT64);
        assertThat(posLeaf.repetition()).isEqualTo(Repetition.REQUIRED);
        LongVector pos = (LongVector) produced.columns().get(ColumnPath.of("_pos"));
        assertThat(pos.getLong(0)).isEqualTo(1000L);
        assertThat(pos.getLong(1)).isEqualTo(1001L);
    }

    @Test
    void selectSubsetsAndReordersByName() {
        DefaultParquetRecordBatch base = sourceBatch();

        ParquetRecordBatch selected = OutputBatches.select(base, List.of(ColumnPath.of("n"), ColumnPath.of("id")));

        assertThat(selected.projectedSchema().leafColumns()).containsExactly(ColumnPath.of("n"), ColumnPath.of("id"));
        assertThat(selected.columns().get(ColumnPath.of("n")))
                .isSameAs(base.columns().get(ColumnPath.of("n")));
    }

    @Test
    void selectRejectsAnUnknownName() {
        DefaultParquetRecordBatch base = sourceBatch();

        assertThatThrownBy(() -> OutputBatches.select(base, List.of(ColumnPath.of("missing"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not produced");
    }

    private static int leafFieldId(ParquetRecordBatch batch, ColumnPath leaf) {
        SchemaNode node = batch.projectedSchema().find(leaf).orElseThrow();
        return node.fieldId();
    }

    @Test
    void closingProducedClosesBase() {
        DefaultParquetRecordBatch base = sourceBatch();
        AtomicBoolean baseClosed = new AtomicBoolean(false);
        base.attachReleaseAction(() -> baseClosed.set(true));
        SequencedSet<Projection.Column> columns = new LinkedHashSet<>(
                List.of(new Projection.Column.Physical(ColumnPath.of("count"), ColumnPath.of("n"))));
        Projection.Of of = (Projection.Of) Projection.of(columns);

        ParquetRecordBatch produced =
                OutputBatches.produce(base, of, OutputBatches.producedSchema(base.projectedSchema(), of));

        produced.close();

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

    private static DefaultParquetRecordBatch sourceBatchWithRowPosition() {
        SchemaNode.Primitive nLeaf = new SchemaNode.Primitive(
                "n", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(nLeaf), Optional.empty(), 0);
        ParquetSchema schema = new ParquetSchema(root);

        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("n"), IntVector.materialized(new int[] {10, 20}, Validity.allValid(2)));
        columns.put(ColumnPath.of("_pos"), LongVector.rowPositions(1000L, Selection.ALL, 2));
        return DefaultParquetRecordBatch.ofHeap(schema, columns, 2);
    }
}
