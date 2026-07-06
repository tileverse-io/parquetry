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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.BooleanVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.MapVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Reconstructs arbitrarily deep nesting from repetition- and definition-level streams by driving
 * {@link NestedVectorAssembler#assembleNested}.
 *
 * <p>The level streams and expected values mirror two corpus fixtures whose contents are read with DuckDB:
 *
 * <ul>
 *   <li>{@code nested_lists.snappy.parquet} column {@code a} is {@code List<List<List<String>>>}; row 0 is
 *       {@code [[["a","b"],["c"]],[null,["d"]]]}.
 *   <li>{@code nested_maps.snappy.parquet} column {@code a} is {@code Map<String, Map<Int,Bool>>}; rows 0-3 are
 *       {@code {"a":{1:true,2:false}}}, {@code {"b":{1:true}}}, {@code {"c":null}}, {@code {"d":{}}}.
 * </ul>
 *
 * <p>Two further cases ({@code List<Struct>} and {@code Struct<List>}) are hand-built to exercise the struct/list
 * combination at depth.
 */
class DeepNestingAssemblyTest {

    @Test
    void listOfListOfListReconstructsRowZeroFromNestedListsFixture() {
        ParquetSchema schema = threeDeepListOfStringSchema();
        ColumnPath leaf = ColumnPath.of("a", "list", "element", "list", "element", "list", "element");

        // Row 0 = [[["a","b"],["c"]],[null,["d"]]]: 5 leaf entries, the 4th is a phantom for the null inner list.
        BinaryVector elementLeaf = BinaryVector.materialized(
                new MemorySegment[] {utf8("a"), utf8("b"), utf8("c"), null, utf8("d")},
                bits(true, true, true, false, true));
        int[] rep = {0, 3, 2, 1, 2};
        int[] def = {7, 7, 7, 4, 7};

        Map<ColumnPath, ColumnVector> assembled = NestedVectorAssembler.assembleNested(
                schema, Map.of(leaf, elementLeaf), Map.of(leaf, rep), Map.of(leaf, def), 1);

        ColumnVector outer = assembled.get(ColumnPath.of("a"));
        assertThat(outer).as("outer column wraps to a ListVector").isInstanceOf(ListVector.class);

        try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, assembled, 1, Arena.ofConfined())) {
            ParquetRecord row = batch.materialize(0);
            Object cell = row.get(ColumnPath.of("a"));
            assertThat(cell).as("row 0 reconstructs as a List").isInstanceOf(List.class);

            List<?> outerList = (List<?>) cell;
            assertThat(outerList).as("row 0 has two outer elements").hasSize(2);

            List<?> middle0 = (List<?>) outerList.get(0);
            assertThat(middle0)
                    .as("first outer element [[a,b],[c]] has two middle elements")
                    .hasSize(2);
            assertThat(asStrings((List<?>) middle0.get(0)))
                    .as("inner list [a,b]")
                    .containsExactly("a", "b");
            assertThat(asStrings((List<?>) middle0.get(1))).as("inner list [c]").containsExactly("c");

            List<?> middle1 = (List<?>) outerList.get(1);
            assertThat(middle1)
                    .as("second outer element [null,[d]] has two middle elements")
                    .hasSize(2);
            assertThat(middle1.get(0))
                    .as("second outer element's first inner list is null")
                    .isNull();
            assertThat(asStrings((List<?>) middle1.get(1))).as("inner list [d]").containsExactly("d");
        }
    }

    @Test
    void mapOfMapReconstructsFromNestedMapsFixture() {
        ParquetSchema schema = mapOfMapSchema();
        ColumnPath outerKey = ColumnPath.of("a", "key_value", "key");
        ColumnPath innerKey = ColumnPath.of("a", "key_value", "value", "key_value", "key");
        ColumnPath innerValue = ColumnPath.of("a", "key_value", "value", "key_value", "value");

        // Outer key: one entry per row, all present.
        BinaryVector outerKeyLeaf = BinaryVector.materialized(
                new MemorySegment[] {utf8("a"), utf8("b"), utf8("c"), utf8("d")}, bits(true, true, true, true));
        int[] outerKeyRep = {0, 0, 0, 0};
        int[] outerKeyDef = {2, 2, 2, 2};

        // Inner key/value: per inner-map entry; rows 0/1 populated, row 2 null inner map, row 3 empty inner map.
        IntVector innerKeyLeaf =
                IntVector.materialized(new int[] {1, 2, 1, 0, 0}, bits(true, true, true, false, false));
        BooleanVector innerValueLeaf = BooleanVector.materialized(
                new boolean[] {true, false, true, false, false}, bits(true, true, true, false, false));
        int[] innerRep = {0, 2, 0, 0, 0};
        int[] innerDef = {4, 4, 4, 2, 3};

        Map<ColumnPath, ColumnVector> leafVectors =
                Map.of(outerKey, outerKeyLeaf, innerKey, innerKeyLeaf, innerValue, innerValueLeaf);
        Map<ColumnPath, int[]> reps = Map.of(outerKey, outerKeyRep, innerKey, innerRep, innerValue, innerRep);
        Map<ColumnPath, int[]> defs = Map.of(outerKey, outerKeyDef, innerKey, innerDef, innerValue, innerDef);

        Map<ColumnPath, ColumnVector> assembled =
                NestedVectorAssembler.assembleNested(schema, leafVectors, reps, defs, 4);

        assertThat(assembled.get(ColumnPath.of("a")))
                .as("outer column wraps to a MapVector")
                .isInstanceOf(MapVector.class);

        try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, assembled, 4, Arena.ofConfined())) {
            Map<?, ?> row0 = (Map<?, ?>) batch.materialize(0).get(ColumnPath.of("a"));
            Map<?, ?> inner0 = (Map<?, ?>) onlyValue(row0);
            assertThat(asString(onlyKey(row0))).as("row 0 outer key").isEqualTo("a");
            assertThat(inner0).as("row 0 inner map has two entries").hasSize(2);
            assertThat(inner0.get(1)).as("row 0 inner[1]").isEqualTo(true);
            assertThat(inner0.get(2)).as("row 0 inner[2]").isEqualTo(false);

            Map<?, ?> row1 = (Map<?, ?>) batch.materialize(1).get(ColumnPath.of("a"));
            Map<?, ?> inner1 = (Map<?, ?>) onlyValue(row1);
            assertThat(inner1).as("row 1 inner map has one entry").hasSize(1);
            assertThat(inner1.get(1)).as("row 1 inner[1]").isEqualTo(true);

            Map<?, ?> row2 = (Map<?, ?>) batch.materialize(2).get(ColumnPath.of("a"));
            assertThat(asString(onlyKey(row2))).as("row 2 outer key").isEqualTo("c");
            assertThat(onlyValue(row2)).as("row 2 inner map is null").isNull();

            Map<?, ?> row3 = (Map<?, ?>) batch.materialize(3).get(ColumnPath.of("a"));
            assertThat(asString(onlyKey(row3))).as("row 3 outer key").isEqualTo("d");
            assertThat((Map<?, ?>) onlyValue(row3))
                    .as("row 3 inner map is present and empty")
                    .isEmpty();
        }
    }

    @Test
    void listOfStructReconstructsFieldsPerElement() {
        ParquetSchema schema = listOfStructSchema();
        ColumnPath xLeaf = ColumnPath.of("pts", "list", "element", "x");
        ColumnPath yLeaf = ColumnPath.of("pts", "list", "element", "y");

        // Row 0 = [{x:1,y:2},{x:3,y:4}], row 1 = [{x:5,y:6}].
        IntVector x = IntVector.materialized(new int[] {1, 3, 5}, bits(true, true, true));
        IntVector y = IntVector.materialized(new int[] {2, 4, 6}, bits(true, true, true));
        // pts optional (+1), list repeated (+1), element struct optional (+1), x/y required (+0) -> leaf def 3.
        int[] rep = {0, 1, 0};
        int[] def = {3, 3, 3};

        Map<ColumnPath, ColumnVector> assembled = NestedVectorAssembler.assembleNested(
                schema, Map.of(xLeaf, x, yLeaf, y), Map.of(xLeaf, rep, yLeaf, rep), Map.of(xLeaf, def, yLeaf, def), 2);

        ColumnVector pts = assembled.get(ColumnPath.of("pts"));
        assertThat(pts).as("pts wraps to a ListVector").isInstanceOf(ListVector.class);
        ListVector listVec = (ListVector) pts;
        assertThat(listVec.child()).as("list child is a StructVector").isInstanceOf(StructVector.class);
        assertThat(listVec.rowOffsetEnd(0) - listVec.rowOffsetStart(0))
                .as("row 0 has two struct elements")
                .isEqualTo(2);
        assertThat(listVec.rowOffsetEnd(1) - listVec.rowOffsetStart(1))
                .as("row 1 has one struct element")
                .isEqualTo(1);

        StructVector child = (StructVector) listVec.child();
        IntVector childX = (IntVector) child.children().get(ColumnPath.of("x"));
        IntVector childY = (IntVector) child.children().get(ColumnPath.of("y"));
        assertThat(childX.getInt(0)).as("element 0 x").isEqualTo(1);
        assertThat(childY.getInt(0)).as("element 0 y").isEqualTo(2);
        assertThat(childX.getInt(2)).as("element 2 x").isEqualTo(5);
        assertThat(childY.getInt(2)).as("element 2 y").isEqualTo(6);
    }

    @Test
    void structOfListReconstructsListInsideStruct() {
        ParquetSchema schema = structOfListSchema();
        ColumnPath nameLeaf = ColumnPath.of("rec", "name");
        ColumnPath tagsLeaf = ColumnPath.of("rec", "tags", "list", "element");

        // Row 0 = {name:1, tags:[10,20]}, row 1 = {name:2, tags:[30]}.
        IntVector name = IntVector.materialized(new int[] {1, 2}, bits(true, true));
        IntVector tags = IntVector.materialized(new int[] {10, 20, 30}, bits(true, true, true));
        int[] tagsRep = {0, 1, 0};
        // rec required (def 0), tags list optional (+1), list repeated (+1), element optional (+1) -> element def 3.
        int[] tagsDef = {3, 3, 3};

        Map<ColumnPath, ColumnVector> assembled = NestedVectorAssembler.assembleNested(
                schema,
                Map.of(nameLeaf, name, tagsLeaf, tags),
                Map.of(tagsLeaf, tagsRep),
                Map.of(tagsLeaf, tagsDef),
                2);

        ColumnVector rec = assembled.get(ColumnPath.of("rec"));
        assertThat(rec).as("rec wraps to a StructVector").isInstanceOf(StructVector.class);
        StructVector structVec = (StructVector) rec;
        ColumnVector tagsVec = structVec.children().get(ColumnPath.of("tags"));
        assertThat(tagsVec).as("tags field inside struct is a ListVector").isInstanceOf(ListVector.class);

        ListVector tagsList = (ListVector) tagsVec;
        assertThat(tagsList.rowOffsetEnd(0) - tagsList.rowOffsetStart(0))
                .as("row 0 tags has two elements")
                .isEqualTo(2);
        assertThat(tagsList.rowOffsetEnd(1) - tagsList.rowOffsetStart(1))
                .as("row 1 tags has one element")
                .isEqualTo(1);

        try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, assembled, 2, Arena.ofConfined())) {
            ParquetRecord nested = batch.materialize(0).readStruct(ColumnPath.of("rec"));
            assertThat(nested.get(ColumnPath.of("name"))).as("row 0 name").isEqualTo(1);
            assertThat(nested.get(ColumnPath.of("tags"))).as("row 0 tags list").isEqualTo(List.of(10, 20));
        }
    }

    // --- schema fixtures ---

    private static ParquetSchema threeDeepListOfStringSchema() {
        SchemaNode.Group level = listLevel(
                "a",
                listLevel(
                        "element",
                        listLevel(
                                "element",
                                new SchemaNode.Primitive(
                                        "element",
                                        Repetition.OPTIONAL,
                                        PrimitiveKind.BYTE_ARRAY,
                                        OptionalInt.empty(),
                                        Optional.of(new LogicalType.StringType()),
                                        -1))));
        return rootOf(level);
    }

    /** Builds {@code <name> (LIST, OPTIONAL) { repeated list { <child> } }}. */
    private static SchemaNode.Group listLevel(String name, SchemaNode child) {
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(child), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
    }

    private static ParquetSchema mapOfMapSchema() {
        SchemaNode.Primitive innerKey = new SchemaNode.Primitive(
                "key", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive innerValue = new SchemaNode.Primitive(
                "value", Repetition.REQUIRED, PrimitiveKind.BOOLEAN, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group innerMap = mapLevel("value", innerKey, innerValue);

        SchemaNode.Primitive outerKey = new SchemaNode.Primitive(
                "key",
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Group outerMap = mapLevel("a", outerKey, innerMap);
        return rootOf(outerMap);
    }

    /** Builds {@code <name> (MAP, OPTIONAL) { repeated key_value { <key>; <value>; } }}. */
    private static SchemaNode.Group mapLevel(String name, SchemaNode key, SchemaNode value) {
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(keyValue), Optional.of(new LogicalType.MapType()), -1);
    }

    private static ParquetSchema listOfStructSchema() {
        SchemaNode.Primitive x = new SchemaNode.Primitive(
                "x", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive y = new SchemaNode.Primitive(
                "y", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group element =
                new SchemaNode.Group("element", Repetition.OPTIONAL, List.of(x, y), Optional.empty(), -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group pts = new SchemaNode.Group(
                "pts", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        return rootOf(pts);
    }

    private static ParquetSchema structOfListSchema() {
        SchemaNode.Primitive name = new SchemaNode.Primitive(
                "name", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group tags = new SchemaNode.Group(
                "tags", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        SchemaNode.Group rec =
                new SchemaNode.Group("rec", Repetition.REQUIRED, List.of(name, tags), Optional.empty(), -1);
        return rootOf(rec);
    }

    private static ParquetSchema rootOf(SchemaNode field) {
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(field), Optional.empty(), -1));
    }

    // --- value helpers ---

    private static MemorySegment utf8(String s) {
        return MemorySegment.ofArray(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String asString(Object value) {
        MemorySegment segment = (MemorySegment) value;
        return new String(segment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    private static List<String> asStrings(List<?> values) {
        return values.stream().map(DeepNestingAssemblyTest::asString).toList();
    }

    private static Object onlyKey(Map<?, ?> map) {
        return map.keySet().iterator().next();
    }

    private static Object onlyValue(Map<?, ?> map) {
        return map.values().iterator().next();
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
