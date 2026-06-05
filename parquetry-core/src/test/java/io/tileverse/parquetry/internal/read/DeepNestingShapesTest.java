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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Reconstructs three deep-nesting shapes that combine repeated collections at more than one level, driving
 * {@link NestedVectorAssembler#assembleNested} with hand-built leaf vectors and per-leaf repetition / definition level
 * streams, then reading the result back through the record API.
 *
 * <p>Each shape exercises the null-versus-empty distinction at an inner level, where the only signal is the definition
 * level at a single phantom leaf entry:
 *
 * <ul>
 *   <li>{@code Map<String, List<Int>>}: a null outer map, an empty-but-present outer map, an entry whose value list is
 *       empty, and an entry whose value list is populated.
 *   <li>{@code List<List<Int>>} across several rows, with an empty middle list inside a non-first outer row.
 *   <li>{@code List<Struct<List<Int>>>} (triple nesting): a list of structs, each struct holding a list field, with one
 *       struct's inner list empty.
 * </ul>
 */
class DeepNestingShapesTest {

    @Test
    void mapOfListReconstructsNullEmptyAndPopulatedValueLists() {
        ParquetSchema schema = mapOfListSchema();
        ColumnPath col = ColumnPath.of("m");
        ColumnPath keyLeaf = ColumnPath.of("m", "key_value", "key");
        ColumnPath valueElement = ColumnPath.of("m", "key_value", "value", "list", "element");

        // Row 0 = null map, row 1 = {} (empty), row 2 = {"x": []}, row 3 = {"y": [10, 20]}.
        // Key leaf: one entry per present map key; rows 0/1 emit a phantom.
        BinaryVector keys = BinaryVector.materialized(
                new MemorySegment[] {null, null, utf8("x"), utf8("y")}, bits(false, false, true, true));
        int[] keyRep = {0, 0, 0, 0};
        // The optional map and its repeated key_value group put a present key at definition level two. An empty map
        // stops at one, and a null map at zero.
        int[] keyDef = {0, 1, 2, 2};

        // Value-element leaf: rows 0/1 phantom; row 2 phantom for the empty value list; row 3 two elements.
        IntVector values = IntVector.materialized(new int[] {0, 0, 0, 10, 20}, bits(false, false, false, true, true));
        int[] valueRep = {0, 0, 0, 0, 2};
        // A present-but-empty value list sits at definition level three, below the value list's own repeated level and
        // its optional element. A populated element reaches five.
        int[] valueDef = {0, 1, 3, 5, 5};

        Map<ColumnPath, ColumnVector> assembled = NestedVectorAssembler.assembleNested(
                schema,
                Map.of(keyLeaf, keys, valueElement, values),
                Map.of(keyLeaf, keyRep, valueElement, valueRep),
                Map.of(keyLeaf, keyDef, valueElement, valueDef),
                4);

        try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, assembled, 4, Arena.ofConfined())) {
            assertThat(batch.materialize(0).get(col))
                    .as("row 0 outer map is null")
                    .isNull();

            assertThat((Map<?, ?>) batch.materialize(1).get(col))
                    .as("row 1 outer map is present and empty")
                    .isEmpty();

            Map<?, ?> row2 = (Map<?, ?>) batch.materialize(2).get(col);
            assertThat(asString(onlyKey(row2))).as("row 2 entry key is x").isEqualTo("x");
            assertThat((List<?>) onlyValue(row2))
                    .as("row 2 entry value list is present and empty")
                    .isEmpty();

            Map<?, ?> row3 = (Map<?, ?>) batch.materialize(3).get(col);
            assertThat(asString(onlyKey(row3))).as("row 3 entry key is y").isEqualTo("y");
            assertThat((List<?>) onlyValue(row3))
                    .as("row 3 entry value list holds [10, 20]")
                    .isEqualTo(List.of(10, 20));
        }
    }

    @Test
    void listOfListReconstructsEmptyMiddleListOnNonFirstRow() {
        ParquetSchema schema = listOfListSchema();
        ColumnPath col = ColumnPath.of("a");
        ColumnPath element = ColumnPath.of("a", "list", "element", "list", "element");

        // Row 0 = [[1, 2], [3]], row 1 = [[], [4]], row 2 = [[5]].
        // Row 1's first inner list is empty: it emits one phantom entry at the inner-list-present def level.
        IntVector values =
                IntVector.materialized(new int[] {1, 2, 3, 0, 4, 5}, bits(true, true, true, false, true, true));
        // rep: 0 starts a new row, 1 a new middle list, 2 a new element in the current inner list.
        int[] rep = {0, 2, 1, 0, 1, 0};
        // Five optional-or-repeated ancestors put a populated inner element at definition level five. An empty inner
        // list, present as an element of the outer list, stops at three.
        int[] def = {5, 5, 5, 3, 5, 5};

        Map<ColumnPath, ColumnVector> assembled = NestedVectorAssembler.assembleNested(
                schema, Map.of(element, values), Map.of(element, rep), Map.of(element, def), 3);

        try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, assembled, 3, Arena.ofConfined())) {
            List<?> row0 = (List<?>) batch.materialize(0).get(col);
            assertThat(row0).as("row 0 has two inner lists").hasSize(2);
            assertThat((List<?>) row0.get(0)).as("row 0 inner list 0").isEqualTo(List.of(1, 2));
            assertThat((List<?>) row0.get(1)).as("row 0 inner list 1").isEqualTo(List.of(3));

            List<?> row1 = (List<?>) batch.materialize(1).get(col);
            assertThat(row1).as("row 1 has two inner lists").hasSize(2);
            assertThat((List<?>) row1.get(0))
                    .as("row 1 first inner list is empty on this non-first outer row")
                    .isEmpty();
            assertThat((List<?>) row1.get(1)).as("row 1 second inner list").isEqualTo(List.of(4));

            List<?> row2 = (List<?>) batch.materialize(2).get(col);
            assertThat(row2).as("row 2 has one inner list").hasSize(1);
            assertThat((List<?>) row2.get(0)).as("row 2 inner list 0").isEqualTo(List.of(5));
        }
    }

    @Test
    void listOfStructOfListReconstructsTripleNesting() {
        ParquetSchema schema = listOfStructOfListSchema();
        ColumnPath col = ColumnPath.of("recs");
        ColumnPath tagElement = ColumnPath.of("recs", "list", "element", "tags", "list", "element");

        // Row 0 = [ {tags:[1,2]}, {tags:[]}, {tags:[3]} ].
        // The middle struct's tags list is empty: one phantom entry at the tags-list-present def level.
        IntVector values = IntVector.materialized(new int[] {1, 2, 0, 3}, bits(true, true, false, true));
        // rep: 0 starts the row, 1 a new struct element in the outer list, 2 a new tag within the current struct.
        int[] rep = {0, 2, 1, 1};
        // Six optional-or-repeated ancestors stack up to a populated tag definition level of six. A struct whose tags
        // list is present but empty stops at four, before the tags repeated level and the tag element are reached.
        int[] def = {6, 6, 4, 6};

        Map<ColumnPath, ColumnVector> assembled = NestedVectorAssembler.assembleNested(
                schema, Map.of(tagElement, values), Map.of(tagElement, rep), Map.of(tagElement, def), 1);

        try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, assembled, 1, Arena.ofConfined())) {
            List<?> structs = (List<?>) batch.materialize(0).get(col);
            assertThat(structs).as("row 0 has three struct elements").hasSize(3);

            ParquetRecord struct0 = (ParquetRecord) structs.get(0);
            assertThat((List<?>) struct0.get(ColumnPath.of("tags")))
                    .as("struct 0 tags list")
                    .isEqualTo(List.of(1, 2));

            ParquetRecord struct1 = (ParquetRecord) structs.get(1);
            assertThat((List<?>) struct1.get(ColumnPath.of("tags")))
                    .as("struct 1 tags list is present and empty")
                    .isEmpty();

            ParquetRecord struct2 = (ParquetRecord) structs.get(2);
            assertThat((List<?>) struct2.get(ColumnPath.of("tags")))
                    .as("struct 2 tags list")
                    .isEqualTo(List.of(3));
        }
    }

    // --- schema fixtures ---

    private static ParquetSchema mapOfListSchema() {
        SchemaNode.Primitive key = new SchemaNode.Primitive(
                "key",
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group valueList = listLevel("value", element);
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, valueList), Optional.empty(), -1);
        SchemaNode.Group map = new SchemaNode.Group(
                "m", Repetition.OPTIONAL, List.of(keyValue), Optional.of(new LogicalType.MapType()), -1);
        return rootOf(map);
    }

    private static ParquetSchema listOfListSchema() {
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group innerList = listLevel("element", element);
        SchemaNode.Group outerList = listLevel("a", innerList);
        return rootOf(outerList);
    }

    private static ParquetSchema listOfStructOfListSchema() {
        SchemaNode.Primitive tag = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group tagsList = listLevel("tags", tag);
        SchemaNode.Group structElement =
                new SchemaNode.Group("element", Repetition.OPTIONAL, List.of(tagsList), Optional.empty(), -1);
        SchemaNode.Group outerList = listLevel("recs", structElement);
        return rootOf(outerList);
    }

    /** Builds {@code <name> (LIST, OPTIONAL) { repeated list { <child> } }}. */
    private static SchemaNode.Group listLevel(String name, SchemaNode child) {
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(child), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
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
