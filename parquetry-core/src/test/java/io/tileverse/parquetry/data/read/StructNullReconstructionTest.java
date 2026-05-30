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
package io.tileverse.parquetry.data.read;

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
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Reconstructs which rows hold a null struct (the whole group absent) versus a present struct (optionally with null
 * fields), driving {@link NestedVectorAssembler#assembleNested} with a per-leaf definition-level stream and then
 * reading the result back through the record API.
 *
 * <p>Schema under test: an optional {@code info} group with two optional int32 fields {@code x} and {@code y}.
 * Definition levels for a non-repeated struct map one entry per logical row to any descendant leaf:
 *
 * <ul>
 *   <li>{@code info} group definition level = 1 (one optional ancestor: {@code info} itself).
 *   <li>{@code info.x} leaf definition level = 2 (optional {@code info} + optional {@code x}).
 * </ul>
 *
 * Rows:
 *
 * <ul>
 *   <li>Row 0: {@code info} is null (descendant def level 0 &lt; 1).
 *   <li>Row 1: {@code info} present with {@code x}=10 and {@code y} null (descendant def level for {@code x} is 2).
 *   <li>Row 2: {@code info} present with {@code x}=30 and {@code y}=40.
 * </ul>
 */
class StructNullReconstructionTest {

    private static final ColumnPath INFO = ColumnPath.of("info");
    private static final ColumnPath INFO_X = ColumnPath.of("info", "x");
    private static final ColumnPath INFO_Y = ColumnPath.of("info", "y");

    @Test
    void nullStructAndPresentStructWithNullFieldReconstructFromDefLevels() {
        ParquetSchema schema = optionalStructSchema();

        IntVector xLeaf = IntVector.materialized(new int[] {0, 10, 30}, validity(false, true, true));
        IntVector yLeaf = IntVector.materialized(new int[] {0, 0, 40}, validity(false, false, true));
        Map<ColumnPath, ColumnVector> leafVectors = Map.of(INFO_X, xLeaf, INFO_Y, yLeaf);

        int[] xDefLevels = {0, 2, 2};
        int[] yDefLevels = {0, 1, 2};
        Map<ColumnPath, int[]> defLevelsByLeaf = Map.of(INFO_X, xDefLevels, INFO_Y, yDefLevels);

        Map<ColumnPath, ColumnVector> assembled =
                NestedVectorAssembler.assembleNested(schema, leafVectors, Map.of(), defLevelsByLeaf, 3);

        StructVector structVec = (StructVector) assembled.get(INFO);
        assertThat(structVec.validity().get(0))
                .as("row 0 struct is null, validity bit must be clear")
                .isFalse();
        assertThat(structVec.validity().get(1))
                .as("row 1 struct is present, validity bit must be set")
                .isTrue();
        assertThat(structVec.validity().get(2))
                .as("row 2 struct is present, validity bit must be set")
                .isTrue();

        try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, assembled, 3, Arena.ofConfined())) {
            ParquetRecord nullStructRow = batch.materialize(0);
            assertThat(nullStructRow.get(INFO))
                    .as("row 0 struct column reads as null")
                    .isNull();
            assertThat(nullStructRow.getStruct(INFO))
                    .as("row 0 getStruct reads as null")
                    .isNull();

            ParquetRecord presentWithNullFieldRow = batch.materialize(1);
            assertThat(presentWithNullFieldRow.get(INFO))
                    .as("row 1 struct column is present")
                    .isNotNull();
            ParquetRecord nested = presentWithNullFieldRow.getStruct(INFO);
            assertThat(nested.get(ColumnPath.of("x")))
                    .as("row 1 present struct exposes x=10")
                    .isEqualTo(10);
            assertThat(nested.get(ColumnPath.of("y")))
                    .as("row 1 present struct has a null y field")
                    .isNull();

            ParquetRecord presentRow = batch.materialize(2);
            ParquetRecord nested2 = presentRow.getStruct(INFO);
            assertThat(nested2.get(ColumnPath.of("y")))
                    .as("row 2 present struct exposes y=40")
                    .isEqualTo(40);
        }
    }

    private static ParquetSchema optionalStructSchema() {
        SchemaNode.Primitive x = new SchemaNode.Primitive(
                "x", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive y = new SchemaNode.Primitive(
                "y", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group info = new SchemaNode.Group("info", Repetition.OPTIONAL, List.of(x, y), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(info), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static BitSet validity(boolean... bits) {
        BitSet b = new BitSet(bits.length);
        for (int i = 0; i < bits.length; i++) {
            if (bits[i]) {
                b.set(i);
            }
        }
        return b;
    }
}
