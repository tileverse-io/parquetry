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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class AugmentedBatchTest {

    @Test
    void appendsConstantColumnUnderAugmentedSchema() {
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                "value", Repetition.OPTIONAL, PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), 1);
        ParquetSchema base = new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(value), Optional.empty(), 0));
        Map<ColumnPath, ColumnVector> baseColumns = new LinkedHashMap<>();
        baseColumns.put(
                ColumnPath.of("value"), DoubleVector.materialized(new double[] {1.0, 2.0}, Validity.allValid(2)));
        DefaultParquetRecordBatch baseBatch = DefaultParquetRecordBatch.ofHeap(base, baseColumns, 2);

        SchemaNode.Primitive yearLeaf = new SchemaNode.Primitive(
                "year", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 2);
        Map<ColumnPath, ColumnVector> constants = new LinkedHashMap<>();
        constants.put(ColumnPath.of("year"), ConstantVectors.of(new Value.IntVal(2024), 2));

        DefaultParquetRecordBatch augmented =
                DefaultParquetRecordBatch.withConstantColumns(baseBatch, List.of(yearLeaf), constants);

        assertThat(augmented.rowCount()).isEqualTo(2);
        assertThat(augmented.projectedSchema().leafColumns())
                .containsExactly(ColumnPath.of("value"), ColumnPath.of("year"));
        assertThat(augmented.columns()).containsKeys(ColumnPath.of("value"), ColumnPath.of("year"));
    }

    @Test
    void closingAugmentedBatchClosesBaseOnce() {
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                "value", Repetition.OPTIONAL, PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), 1);
        ParquetSchema base = new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(value), Optional.empty(), 0));
        Map<ColumnPath, ColumnVector> baseColumns = new LinkedHashMap<>();
        baseColumns.put(
                ColumnPath.of("value"), DoubleVector.materialized(new double[] {1.0, 2.0}, Validity.allValid(2)));
        DefaultParquetRecordBatch baseBatch = DefaultParquetRecordBatch.ofHeap(base, baseColumns, 2);

        AtomicInteger closedCount = new AtomicInteger();
        baseBatch.attachReleaseAction(closedCount::incrementAndGet);

        SchemaNode.Primitive yearLeaf = new SchemaNode.Primitive(
                "year", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 2);
        Map<ColumnPath, ColumnVector> constants = new LinkedHashMap<>();
        constants.put(ColumnPath.of("year"), ConstantVectors.of(new Value.IntVal(2024), 2));

        DefaultParquetRecordBatch augmented =
                DefaultParquetRecordBatch.withConstantColumns(baseBatch, List.of(yearLeaf), constants);

        augmented.close();
        assertThat(closedCount.get()).isEqualTo(1);

        augmented.close();
        assertThat(closedCount.get()).isEqualTo(1);
    }
}
