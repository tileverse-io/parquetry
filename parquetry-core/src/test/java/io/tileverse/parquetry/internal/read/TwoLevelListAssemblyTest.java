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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Reconstructs the legacy two-level repeated-list encoding, where the repeated field directly under a {@code LIST}
 * group is a primitive instead of an inner wrapper group:
 *
 * <pre>{@code
 * optional group nums (LIST) {
 *   repeated int32 element;
 * }
 * }</pre>
 *
 * <p>This shape arises both from genuine two-level legacy files and from older files that annotate a list with the
 * deprecated {@code converted_type} only; in either case the assembler sees a primitive as the first child of the group
 * it treats as a list. The standard three-level encoding wraps the element in a {@code repeated group list}; that path
 * is covered by {@link DeepNestingAssemblyTest}.
 */
class TwoLevelListAssemblyTest {

    @Test
    void reconstructsValuesNullAndEmptyFromTwoLevelEncoding() {
        ParquetSchema schema = twoLevelListSchema();
        ColumnPath elementLeaf = ColumnPath.of("nums", "element");

        // Row 0 = [10, 20], row 1 = null, row 2 = []. Null and empty rows each emit one phantom leaf entry.
        IntVector element = IntVector.materialized(new int[] {10, 20, 0, 0}, bits(true, true, false, false));
        int[] rep = {0, 1, 0, 0};
        int[] def = {2, 2, 0, 1};

        Map<ColumnPath, ColumnVector> assembled = NestedVectorAssembler.assembleNested(
                schema, Map.of(elementLeaf, element), Map.of(elementLeaf, rep), Map.of(elementLeaf, def), 3);

        ColumnVector nums = assembled.get(ColumnPath.of("nums"));
        assertThat(nums).as("nums wraps to a ListVector").isInstanceOf(ListVector.class);

        ListVector listVec = (ListVector) nums;
        assertThat(listVec.rowOffsetEnd(0) - listVec.rowOffsetStart(0))
                .as("row 0 list has two elements")
                .isEqualTo(2);
        assertThat(listVec.validity().isValid(1)).as("row 1 list is null").isFalse();
        assertThat(listVec.validity().isValid(2)).as("row 2 list is present").isTrue();
        assertThat(listVec.rowOffsetEnd(2) - listVec.rowOffsetStart(2))
                .as("row 2 list is empty")
                .isZero();

        try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, assembled, 3, Arena.ofConfined())) {
            assertThat(batch.materialize(0).get(ColumnPath.of("nums")))
                    .as("row 0 reads back as [10, 20]")
                    .isEqualTo(List.of(10, 20));
            assertThat(batch.materialize(1).get(ColumnPath.of("nums")))
                    .as("row 1 reads back as null")
                    .isNull();
            assertThat(batch.materialize(2).get(ColumnPath.of("nums")))
                    .as("row 2 reads back as an empty list")
                    .isEqualTo(List.of());
        }
    }

    private static ParquetSchema twoLevelListSchema() {
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.REPEATED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group nums = new SchemaNode.Group(
                "nums", Repetition.OPTIONAL, List.of(element), Optional.of(new LogicalType.ListType()), -1);
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(nums), Optional.empty(), -1));
    }

    private static Validity bits(boolean... values) {
        BitSet b = new BitSet(values.length);
        for (int i = 0; i < values.length; i++) {
            if (values[i]) {
                b.set(i);
            }
        }
        return Validity.of(b, values.length);
    }
}
