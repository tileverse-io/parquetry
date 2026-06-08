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

import java.lang.foreign.Arena;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * A column predicate addresses a leaf by its full path. The record-level filter evaluator resolves that path against
 * the assembled record via {@link ParquetRecord#get(ColumnPath)}. A path into a struct (e.g. {@code bbox.xmin}) must
 * resolve to the nested leaf rather than reading as a null cell. The struct vector keys its children by their simple
 * name, matching the assembler.
 */
class NestedColumnPathAccessTest {

    private static final ColumnPath TOP = ColumnPath.of("id");
    private static final ColumnPath STRUCT = ColumnPath.of("bbox");
    private static final ColumnPath NESTED_XMIN = ColumnPath.of("bbox", "xmin");

    @Test
    void getResolvesAStructLeafByItsFullPath() {
        try (ParquetRecordBatch batch = bboxBatch(7, -12.5)) {
            ParquetRecord row = batch.materialize(0);

            assertThat(row.get(NESTED_XMIN)).isEqualTo(-12.5);
        }
    }

    @Test
    void isNullIsFalseForAPresentStructLeaf() {
        try (ParquetRecordBatch batch = bboxBatch(7, -12.5)) {
            ParquetRecord row = batch.materialize(0);

            assertThat(row.isNull(NESTED_XMIN)).isFalse();
        }
    }

    /** A one-row batch: {@code id INT32} plus a {@code bbox} struct with one {@code xmin DOUBLE} child. */
    private static ParquetRecordBatch bboxBatch(int id, double xmin) {
        IntVector idVec = IntVector.materialized(new int[] {id}, Validity.allValid(1));
        DoubleVector xminVec = DoubleVector.materialized(new double[] {xmin}, Validity.allValid(1));
        StructVector bbox = new StructVector(Map.of(ColumnPath.of("xmin"), xminVec), Validity.allValid(1), 1);
        return new DefaultParquetRecordBatch(bboxSchema(), Map.of(TOP, idVec, STRUCT, bbox), 1, Arena.ofConfined());
    }

    private static ParquetSchema bboxSchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive xmin = new SchemaNode.Primitive(
                "xmin", Repetition.OPTIONAL, PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group bbox = new SchemaNode.Group("bbox", Repetition.OPTIONAL, List.of(xmin), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, bbox), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
