/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.MapVector;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class NestedVectorAssemblerTest {

    // --- buildList: core cases ---

    @Test
    void buildsListVectorFromLeafChild() {
        // Schema "items" is an optional LIST group with a repeated child group containing one optional int32 element.
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

        assertThat(vec.validity().isValid(0)).isTrue();
        assertThat(vec.validity().isValid(1)).isTrue();
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
        // Schema "point" is a required group with two required int32 children x and y.
        // Leaf vectors are "point.x" and "point.y"
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
        // Schema is two flat required int32 leaves, a and b, with no surrounding group
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
        assertThat(sv.validity().isValid(0)).isTrue();
        assertThat(sv.validity().isValid(1)).isTrue();
    }

    // --- buildMap ---

    @Test
    void buildsMapVectorFromKeyAndValueChildren() {
        // Row 0 maps "a" to 1 and "b" to 2; row 1 maps "c" to 3.
        // Three key/value pairs total; row 0 covers indices [0, 2) and row 1 covers [2, 3).
        java.lang.foreign.MemorySegment[] keyBytes = {bytesOf("a"), bytesOf("b"), bytesOf("c")};
        BinaryVector keys = BinaryVector.materialized(keyBytes, allValid(3));
        IntVector values = IntVector.materialized(new int[] {1, 2, 3}, allValid(3));
        int[] repLevels = {0, 1, 0};

        MapVector vec = NestedVectorAssembler.buildMap(keys, values, repLevels, 2);

        assertThat(vec.size()).isEqualTo(2);
        assertThat(vec.rowOffsetStart(0)).isZero();
        assertThat(vec.rowOffsetEnd(0)).isEqualTo(2);
        assertThat(vec.rowOffsetStart(1)).isEqualTo(2);
        assertThat(vec.rowOffsetEnd(1)).isEqualTo(3);
        assertThat(vec.keys()).isSameAs(keys);
        assertThat(vec.values()).isSameAs(values);
    }

    // --- assembleNested: LIST wrapping + leaf hiding ---

    @Test
    void assembleNestedWrapsTopLevelListAndHidesLeaf() {
        // Schema "items" is an optional LIST group with a repeated child group containing one optional int32 element.
        // Leaf path is items.list.element. After assembly, items should be a ListVector and the leaf should be hidden.
        ParquetSchema schema = listOfInt32Schema("items", "list", "element");

        IntVector childLeaf = IntVector.materialized(new int[] {10, 20, 30}, allValid(3));
        Map<ColumnPath, ColumnVector> leafVectors = Map.of(ColumnPath.of("items", "list", "element"), childLeaf);
        Map<ColumnPath, int[]> repLevels = Map.of(ColumnPath.of("items", "list", "element"), new int[] {0, 1, 0});

        Map<ColumnPath, ColumnVector> result =
                NestedVectorAssembler.assembleNested(schema, leafVectors, repLevels, Map.of(), 2);

        assertThat(result).containsKey(ColumnPath.of("items"));
        assertThat(result.get(ColumnPath.of("items"))).isInstanceOf(ListVector.class);
        assertThat(result).doesNotContainKey(ColumnPath.of("items", "list", "element"));

        ListVector listVec = (ListVector) result.get(ColumnPath.of("items"));
        assertThat(listVec.size()).isEqualTo(2);
        assertThat(listVec.rowOffsetEnd(0) - listVec.rowOffsetStart(0)).isEqualTo(2);
        assertThat(listVec.rowOffsetEnd(1) - listVec.rowOffsetStart(1)).isEqualTo(1);
    }

    @Test
    void assembleNestedWrapsTopLevelMapAndHidesLeaves() {
        // Schema "m" is an optional MAP group with a repeated key_value child holding a required binary key and an
        // optional int32 value. Two rows: row 0 = {"a" -> 1, "b" -> 2}, row 1 = {"c" -> 3}.
        // Leaf paths are m.key_value.key, m.key_value.value. After assembly, m should be a MapVector and both leaves
        // should be hidden from the result map.
        ParquetSchema schema = mapStringIntSchema("m", "key_value", "key", "value");

        java.lang.foreign.MemorySegment[] keyBytes = {bytesOf("a"), bytesOf("b"), bytesOf("c")};
        BinaryVector keyLeaf = BinaryVector.materialized(keyBytes, allValid(3));
        IntVector valueLeaf = IntVector.materialized(new int[] {1, 2, 3}, allValid(3));

        ColumnPath keyPath = ColumnPath.of("m", "key_value", "key");
        ColumnPath valuePath = ColumnPath.of("m", "key_value", "value");
        Map<ColumnPath, ColumnVector> leafVectors = new HashMap<>();
        leafVectors.put(keyPath, keyLeaf);
        leafVectors.put(valuePath, valueLeaf);
        int[] sharedRepLevels = {0, 1, 0};
        Map<ColumnPath, int[]> repLevels = new HashMap<>();
        repLevels.put(keyPath, sharedRepLevels);
        repLevels.put(valuePath, sharedRepLevels);

        Map<ColumnPath, ColumnVector> result =
                NestedVectorAssembler.assembleNested(schema, leafVectors, repLevels, Map.of(), 2);

        assertThat(result).containsKey(ColumnPath.of("m"));
        assertThat(result.get(ColumnPath.of("m"))).isInstanceOf(MapVector.class);
        assertThat(result).doesNotContainKey(keyPath).doesNotContainKey(valuePath);

        MapVector mapVec = (MapVector) result.get(ColumnPath.of("m"));
        assertThat(mapVec.size()).isEqualTo(2);
        assertThat(mapVec.rowOffsetEnd(0) - mapVec.rowOffsetStart(0)).isEqualTo(2);
        assertThat(mapVec.rowOffsetEnd(1) - mapVec.rowOffsetStart(1)).isEqualTo(1);
        assertThat(mapVec.keys()).isSameAs(keyLeaf);
        assertThat(mapVec.values()).isSameAs(valueLeaf);
    }

    // --- fixture helpers (continued) ---

    private static java.lang.foreign.MemorySegment bytesOf(String s) {
        return java.lang.foreign.MemorySegment.ofArray(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * Builds a parquetry schema for an annotated LIST&lt;INT32&gt; column with the standard 3-level structure used by
     * the corpus fixtures: {@code <listName> (LIST) { repeated <middleName> { optional int32 <elementName>; } }}.
     */
    private static ParquetSchema listOfInt32Schema(String listName, String middleName, String elementName) {
        SchemaNode.Primitive elementLeaf = new SchemaNode.Primitive(
                elementName, Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group middle =
                new SchemaNode.Group(middleName, Repetition.REPEATED, List.of(elementLeaf), Optional.empty(), -1);
        SchemaNode.Group listGroup = new SchemaNode.Group(
                listName, Repetition.OPTIONAL, List.of(middle), Optional.of(new LogicalType.ListType()), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(listGroup), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /**
     * Builds a parquetry schema for an annotated MAP&lt;BINARY, INT32&gt; column with the canonical 3-level structure:
     * {@code <mapName> (MAP) { repeated <middleName> { required binary <keyName>; optional int32 <valueName>; } }}.
     */
    private static ParquetSchema mapStringIntSchema(
            String mapName, String middleName, String keyName, String valueName) {
        SchemaNode.Primitive keyLeaf = new SchemaNode.Primitive(
                keyName, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive valueLeaf = new SchemaNode.Primitive(
                valueName, Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group middle = new SchemaNode.Group(
                middleName, Repetition.REPEATED, List.of(keyLeaf, valueLeaf), Optional.empty(), -1);
        SchemaNode.Group mapGroup = new SchemaNode.Group(
                mapName, Repetition.OPTIONAL, List.of(middle), Optional.of(new LogicalType.MapType()), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(mapGroup), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    // --- fixture helpers ---

    private static Validity allValid(int n) {
        return Validity.allValid(n);
    }

    /** Builds a flat schema with the given required INT32 leaf column names under a root group. */
    private static ParquetSchema flatSchema(String... names) {
        List<SchemaNode> children = new java.util.ArrayList<>();
        for (String name : names) {
            children.add(new SchemaNode.Primitive(
                    name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1));
        }
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /**
     * Builds a schema with a single required group named {@code groupName} containing required INT32 leaves for each
     * name in {@code leafNames}.
     */
    private static ParquetSchema structSchema(String groupName, String... leafNames) {
        List<SchemaNode> leaves = new java.util.ArrayList<>();
        for (String name : leafNames) {
            leaves.add(new SchemaNode.Primitive(
                    name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1));
        }
        SchemaNode.Group group = new SchemaNode.Group(groupName, Repetition.REQUIRED, leaves, Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(group), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
