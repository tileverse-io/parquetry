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
package io.tileverse.parquetry.record;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.Arena;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Pins the nested-path edge cases that {@link NestedColumnPathAccessTest} does not cover: a path three levels deep, a
 * null intermediate struct cell (the struct itself null, not the leaf), a path absent from the projected schema, and a
 * path whose intermediate segment names a non-struct column. The lenient accessors ({@code get}, {@code isNull},
 * {@code readStruct}) read every unreachable leaf as a null cell; the typed accessors throw
 * {@link ParquetSchemaException}.
 */
class DeepColumnPathAccessTest {

    private static final ColumnPath TOP = ColumnPath.of("id");
    private static final ColumnPath OUTER = ColumnPath.of("outer");
    private static final ColumnPath DEEP_XMIN = ColumnPath.of("outer", "inner", "xmin");
    private static final ColumnPath ABSENT = ColumnPath.of("outer", "inner", "nope");
    private static final ColumnPath THROUGH_PRIMITIVE = ColumnPath.of("id", "xmin");

    @Test
    void getResolvesAThreeLevelPath() {
        try (ParquetRecordBatch batch = deepBatch(7, -12.5, false)) {
            ParquetRecord row = batch.materialize(0);

            assertThat(row.get(DEEP_XMIN)).isEqualTo(-12.5);
            assertThat(row.getDouble(DEEP_XMIN)).isEqualTo(-12.5);
            assertThat(row.isNull(DEEP_XMIN)).isFalse();
        }
    }

    @Test
    void getReadsANullIntermediateStructCellAsANullLeaf() {
        try (ParquetRecordBatch batch = deepBatch(7, -12.5, true)) {
            ParquetRecord row = batch.materialize(0);

            assertThat(row.get(DEEP_XMIN)).isNull();
            assertThat(row.isNull(DEEP_XMIN)).isTrue();
        }
    }

    @Test
    void typedAccessorThrowsWhenAnIntermediateStructCellIsNull() {
        try (ParquetRecordBatch batch = deepBatch(7, -12.5, true)) {
            ParquetRecord row = batch.materialize(0);

            assertThatThrownBy(() -> row.getDouble(DEEP_XMIN))
                    .isInstanceOf(ParquetSchemaException.class)
                    .hasMessageContaining("outer.inner.xmin");
        }
    }

    @Test
    void getReturnsNullForAnAbsentPath() {
        try (ParquetRecordBatch batch = deepBatch(7, -12.5, false)) {
            ParquetRecord row = batch.materialize(0);

            assertThat(row.get(ABSENT)).isNull();
            assertThat(row.isNull(ABSENT)).isTrue();
        }
    }

    @Test
    void typedAccessorThrowsForAnAbsentPath() {
        try (ParquetRecordBatch batch = deepBatch(7, -12.5, false)) {
            ParquetRecord row = batch.materialize(0);

            assertThatThrownBy(() -> row.getDouble(ABSENT))
                    .isInstanceOf(ParquetSchemaException.class)
                    .hasMessageContaining("outer.inner.nope");
        }
    }

    @Test
    void getReadsAPathThroughANonStructColumnAsANullLeaf() {
        try (ParquetRecordBatch batch = deepBatch(7, -12.5, false)) {
            ParquetRecord row = batch.materialize(0);

            assertThat(row.get(THROUGH_PRIMITIVE)).isNull();
            assertThat(row.isNull(THROUGH_PRIMITIVE)).isTrue();
        }
    }

    @Test
    void walkReadsEachRowsOwnStructValidity() {
        try (ParquetRecordBatch batch = twoRowBatchWithOuterNullAtRowZero()) {
            ParquetRecord row0 = batch.materialize(0);
            ParquetRecord row1 = batch.materialize(1);

            assertThat(row0.get(DEEP_XMIN)).isNull();
            assertThat(row0.isNull(DEEP_XMIN)).isTrue();
            assertThat(row1.get(DEEP_XMIN)).isEqualTo(4.25);
            assertThat(row1.isNull(DEEP_XMIN)).isFalse();
        }
    }

    /**
     * A two-row batch over the same schema as {@link #deepBatch}: the {@code outer} struct cell is null at row 0 and
     * valid at row 1, while the vectors beneath it hold values for both rows. A walk that hard-codes a row index
     * instead of reading the record's own row reads the wrong validity here.
     */
    private static ParquetRecordBatch twoRowBatchWithOuterNullAtRowZero() {
        IntVector idVec = IntVector.materialized(new int[] {7, 8}, Validity.allValid(2));
        DoubleVector xminVec = DoubleVector.materialized(new double[] {-12.5, 4.25}, Validity.allValid(2));
        StructVector inner = new StructVector(Map.of(ColumnPath.of("xmin"), xminVec), Validity.allValid(2), 2);
        BitSet outerValid = new BitSet();
        outerValid.set(1);
        StructVector outer = new StructVector(Map.of(ColumnPath.of("inner"), inner), Validity.of(outerValid, 2), 2);
        return new DefaultParquetRecordBatch(deepSchema(), Map.of(TOP, idVec, OUTER, outer), 2, Arena.ofConfined());
    }

    /**
     * A one-row batch: {@code id INT32} plus an {@code outer} struct holding an {@code inner} struct with one
     * {@code xmin DOUBLE} child. When {@code outerNull} is set, the {@code outer} struct cell itself is null while the
     * vectors beneath it stay populated.
     */
    private static ParquetRecordBatch deepBatch(int id, double xmin, boolean outerNull) {
        IntVector idVec = IntVector.materialized(new int[] {id}, Validity.allValid(1));
        DoubleVector xminVec = DoubleVector.materialized(new double[] {xmin}, Validity.allValid(1));
        StructVector inner = new StructVector(Map.of(ColumnPath.of("xmin"), xminVec), Validity.allValid(1), 1);
        Validity outerValidity = outerNull ? Validity.of(new BitSet(), 1) : Validity.allValid(1);
        StructVector outer = new StructVector(Map.of(ColumnPath.of("inner"), inner), outerValidity, 1);
        return new DefaultParquetRecordBatch(deepSchema(), Map.of(TOP, idVec, OUTER, outer), 1, Arena.ofConfined());
    }

    private static ParquetSchema deepSchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive xmin = new SchemaNode.Primitive(
                "xmin", Repetition.OPTIONAL, PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group inner =
                new SchemaNode.Group("inner", Repetition.OPTIONAL, List.of(xmin), Optional.empty(), -1);
        SchemaNode.Group outer =
                new SchemaNode.Group("outer", Repetition.OPTIONAL, List.of(inner), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, outer), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
