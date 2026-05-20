/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;

class NestedVectorAssemblerTest {

    // --- buildList: core cases ---

    @Test
    void buildsListVectorFromLeafChild() {
        // Schema: optional group "items" (LIST) { repeated group list { optional int32 element; } }
        // Two rows: row 0 = [10, 20], row 1 = [30].
        // Leaf "items.list.element" emits 3 elements: 10, 20, 30
        // Rep levels: row 0 boundary (rep=0, val=10), continuation (rep=1, val=20), row 1 boundary (rep=0, val=30)
        IntVector child = IntVector.materialized(new int[] {10, 20, 30}, allValid(3));

        int[] repLevels = {0, 1, 0};

        ListVector vec = NestedVectorAssembler.buildList(child, repLevels, 2);

        assertThat(vec.size()).isEqualTo(2);
        assertThat(vec.rowOffsetStart(0)).isZero();
        assertThat(vec.rowOffsetEnd(0)).isEqualTo(2);
        assertThat(vec.rowOffsetStart(1)).isEqualTo(2);
        assertThat(vec.rowOffsetEnd(1)).isEqualTo(3);
    }

    @Test
    void rowWithSingleElement() {
        IntVector child = IntVector.materialized(new int[] {42}, allValid(1));
        int[] repLevels = {0};

        ListVector vec = NestedVectorAssembler.buildList(child, repLevels, 1);

        assertThat(vec.size()).isEqualTo(1);
        assertThat(vec.rowOffsetEnd(0) - vec.rowOffsetStart(0)).isEqualTo(1);
    }

    @Test
    void multipleRowsWithVaryingLengths() {
        // Row 0: [10, 20, 30], row 1: [40], row 2: [50, 60]
        IntVector child = IntVector.materialized(new int[] {10, 20, 30, 40, 50, 60}, allValid(6));
        int[] repLevels = {0, 1, 1, 0, 0, 1};

        ListVector vec = NestedVectorAssembler.buildList(child, repLevels, 3);

        assertThat(vec.rowOffsetStart(0)).isZero();
        assertThat(vec.rowOffsetEnd(0)).isEqualTo(3);
        assertThat(vec.rowOffsetStart(1)).isEqualTo(3);
        assertThat(vec.rowOffsetEnd(1)).isEqualTo(4);
        assertThat(vec.rowOffsetStart(2)).isEqualTo(4);
        assertThat(vec.rowOffsetEnd(2)).isEqualTo(6);
    }

    @Test
    void validitySetAllTrueForAllRows() {
        // Validity is set all-true regardless of def levels in the current implementation.
        IntVector child = IntVector.materialized(new int[] {1, 2, 3}, allValid(3));
        int[] repLevels = {0, 1, 0};

        ListVector vec = NestedVectorAssembler.buildList(child, repLevels, 2);

        assertThat(vec.validity().get(0)).isTrue();
        assertThat(vec.validity().get(1)).isTrue();
    }

    @Test
    void childVectorAccessibleFromList() {
        IntVector child = IntVector.materialized(new int[] {7, 8, 9}, allValid(3));
        int[] repLevels = {0, 1, 1};

        ListVector vec = NestedVectorAssembler.buildList(child, repLevels, 1);

        assertThat(vec.child()).isSameAs(child);
    }

    // --- wrapStructGroups ---

    @Test
    void wrapsStructGroupIntoStructVector() {
        // Schema: required group "point" { required int32 x; required int32 y; }
        // Leaf vectors: "point.x" and "point.y"
        ParquetSchema schema = structSchema("point", "x", "y");

        IntVector xVec = IntVector.materialized(new int[] {1, 2, 3}, allValid(3));
        IntVector yVec = IntVector.materialized(new int[] {4, 5, 6}, allValid(3));

        Map<ColumnPath, ColumnVector> leafVectors = new HashMap<>();
        leafVectors.put(ColumnPath.of("point", "x"), xVec);
        leafVectors.put(ColumnPath.of("point", "y"), yVec);

        Map<ColumnPath, ColumnVector> result = NestedVectorAssembler.wrapStructGroups(schema, leafVectors, 3);

        // Leaf paths must still be present, and the struct group path too
        assertThat(result)
                .containsKey(ColumnPath.of("point", "x"))
                .containsKey(ColumnPath.of("point", "y"))
                .containsKey(ColumnPath.of("point"));

        ColumnVector structVec = result.get(ColumnPath.of("point"));
        assertThat(structVec).isInstanceOf(StructVector.class);

        StructVector sv = (StructVector) structVec;
        assertThat(sv.size()).isEqualTo(3);
        assertThat(sv.children()).containsKey(ColumnPath.of("x")).containsKey(ColumnPath.of("y"));
        assertThat(sv.children().get(ColumnPath.of("x"))).isSameAs(xVec);
        assertThat(sv.children().get(ColumnPath.of("y"))).isSameAs(yVec);
    }

    @Test
    void flatSchemaLeavesInputUnchanged() {
        // Schema: required int32 a; required int32 b;  (no groups)
        ParquetSchema schema = flatSchema("a", "b");

        IntVector aVec = IntVector.materialized(new int[] {10, 20}, allValid(2));
        IntVector bVec = IntVector.materialized(new int[] {30, 40}, allValid(2));

        Map<ColumnPath, ColumnVector> leafVectors = new HashMap<>();
        leafVectors.put(ColumnPath.of("a"), aVec);
        leafVectors.put(ColumnPath.of("b"), bVec);

        Map<ColumnPath, ColumnVector> result = NestedVectorAssembler.wrapStructGroups(schema, leafVectors, 2);

        // No new keys: a flat schema has nothing to wrap
        assertThat(result).containsOnlyKeys(ColumnPath.of("a"), ColumnPath.of("b"));
    }

    @Test
    void structValiditySetAllTrue() {
        ParquetSchema schema = structSchema("s", "v");
        IntVector vVec = IntVector.materialized(new int[] {1, 2}, allValid(2));
        Map<ColumnPath, ColumnVector> leafVectors = Map.of(ColumnPath.of("s", "v"), vVec);

        Map<ColumnPath, ColumnVector> result = NestedVectorAssembler.wrapStructGroups(schema, leafVectors, 2);

        StructVector sv = (StructVector) result.get(ColumnPath.of("s"));
        assertThat(sv.validity().get(0)).isTrue();
        assertThat(sv.validity().get(1)).isTrue();
    }

    // --- fixture helpers ---

    private static BitSet allValid(int n) {
        BitSet b = new BitSet(n);
        b.set(0, n);
        return b;
    }

    /** Builds a flat schema with the given required INT32 leaf column names under a root group. */
    private static ParquetSchema flatSchema(String... names) {
        List<Field> children = new java.util.ArrayList<>();
        for (String name : names) {
            children.add(new Field.Primitive(
                    name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1));
        }
        Field.Group root = new Field.Group("root", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /**
     * Builds a schema with a single required group named {@code groupName} containing required INT32 leaves for each
     * name in {@code leafNames}.
     */
    private static ParquetSchema structSchema(String groupName, String... leafNames) {
        List<Field> leaves = new java.util.ArrayList<>();
        for (String name : leafNames) {
            leaves.add(new Field.Primitive(
                    name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1));
        }
        Field.Group group = new Field.Group(groupName, Repetition.REQUIRED, leaves, Optional.empty(), -1);
        Field.Group root = new Field.Group("root", Repetition.REQUIRED, List.of(group), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
