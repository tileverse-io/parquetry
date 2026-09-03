/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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

import java.nio.charset.StandardCharsets;
import java.util.BitSet;
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

/**
 * Assembled struct validity must be truthful for an OPTIONAL struct whose only descendant leaves live under a deeper
 * repeated node. There is no row-aligned leaf to read a definition level from per slot, but the def stream still
 * distinguishes struct-null from deeper nulls at each element's first entry; the assembled {@link StructVector} must
 * reflect that, because batch consumers (the write-side striper, Arrow export, compaction) read
 * {@link StructVector#validity()} rather than the raw levels.
 */
class NestedStructValidityAssemblyTest {

    /**
     * {@code outer: list<element: struct{inner: list<int>}>}, one row of four elements: populated inner, empty inner,
     * null inner, null element struct. Element def stream [6, 4, 3, 2].
     */
    @Test
    void elementStructNullIsDistinguishedFromInnerListNull() {
        ParquetSchema schema = listOfStructOfListSchema();
        ColumnPath leaf = ColumnPath.of("outer", "list", "element", "inner", "list", "element");
        assertThat(schema.maxLevels(leaf).maxDefinitionLevel())
                .as("fixture def arithmetic")
                .isEqualTo(6);

        IntVector leafValues = IntVector.materialized(new int[] {1, 0, 0, 0}, bits(true, false, false, false));
        int[] rep = {0, 1, 1, 1};
        int[] def = {6, 4, 3, 2};

        Map<ColumnPath, ColumnVector> assembled = NestedVectorAssembler.assembleNested(
                schema, Map.of(leaf, leafValues), Map.of(leaf, rep), Map.of(leaf, def), 1);

        ListVector outer = (ListVector) assembled.get(ColumnPath.of("outer"));
        StructVector element = (StructVector) outer.child();
        assertThat(validityBits(element.validity()))
                .as("element struct: present, present, present, NULL")
                .isEqualTo("1110");

        ListVector inner = (ListVector) element.children().get(ColumnPath.of("inner"));
        assertThat(validityBits(inner.validity()))
                .as("inner list: populated, empty-present, null, null-under-null-struct")
                .isEqualTo("1100");
    }

    /**
     * {@code outer: list<element: struct{h: struct{i: list<int>}}>} - the nullable.impala {@code g} shape. One row of
     * five elements: populated, empty i, null element, null i, null h. Element def stream [7, 5, 2, 4, 3].
     */
    @Test
    void stackedOptionalStructsEachKeepTheirOwnNullDepth() {
        ParquetSchema schema = listOfStructOfStructOfListSchema();
        ColumnPath leaf = ColumnPath.of("outer", "list", "element", "h", "i", "list", "element");
        assertThat(schema.maxLevels(leaf).maxDefinitionLevel())
                .as("fixture def arithmetic")
                .isEqualTo(7);

        IntVector leafValues =
                IntVector.materialized(new int[] {2, 0, 0, 0, 0}, bits(true, false, false, false, false));
        int[] rep = {0, 1, 1, 1, 1};
        int[] def = {7, 5, 2, 4, 3};

        Map<ColumnPath, ColumnVector> assembled = NestedVectorAssembler.assembleNested(
                schema, Map.of(leaf, leafValues), Map.of(leaf, rep), Map.of(leaf, def), 1);

        ListVector outer = (ListVector) assembled.get(ColumnPath.of("outer"));
        StructVector element = (StructVector) outer.child();
        assertThat(validityBits(element.validity()))
                .as("element struct null only at the third element")
                .isEqualTo("11011");

        StructVector h = (StructVector) element.children().get(ColumnPath.of("h"));
        assertThat(validityBits(h.validity()))
                .as("h struct null at the third (under null element) and fifth")
                .isEqualTo("11010");

        ListVector i = (ListVector) h.children().get(ColumnPath.of("i"));
        assertThat(validityBits(i.validity()))
                .as("i list present only where h chain is present and i itself is")
                .isEqualTo("11000");
    }

    /**
     * {@code m: map<string, value: struct{i: list<int>}>}, one row of two entries: value present with populated i,
     * value NULL. Value-side def stream [6, 2].
     */
    @Test
    void mapValueStructNullSurvivesAssembly() {
        ParquetSchema schema = mapOfStructOfListSchema();
        ColumnPath keyLeaf = ColumnPath.of("m", "key_value", "key");
        ColumnPath valueLeaf = ColumnPath.of("m", "key_value", "value", "i", "list", "element");
        assertThat(schema.maxLevels(valueLeaf).maxDefinitionLevel())
                .as("fixture def arithmetic")
                .isEqualTo(6);

        BinaryVector keys = BinaryVector.materialized(
                new java.lang.foreign.MemorySegment[] {utf8("a"), utf8("b")}, bits(true, true));
        int[] keyRep = {0, 1};
        int[] keyDef = {2, 2};
        IntVector values = IntVector.materialized(new int[] {7, 0}, bits(true, false));
        int[] valueRep = {0, 1};
        int[] valueDef = {6, 2};

        Map<ColumnPath, ColumnVector> assembled = NestedVectorAssembler.assembleNested(
                schema,
                Map.of(keyLeaf, keys, valueLeaf, values),
                Map.of(keyLeaf, keyRep, valueLeaf, valueRep),
                Map.of(keyLeaf, keyDef, valueLeaf, valueDef),
                1);

        MapVector m = (MapVector) assembled.get(ColumnPath.of("m"));
        StructVector value = (StructVector) m.values();
        assertThat(validityBits(value.validity()))
                .as("map value struct: present, NULL")
                .isEqualTo("10");
    }

    // --- schema fixtures ---

    private static ParquetSchema listOfStructOfListSchema() {
        SchemaNode.Group inner = listGroup("inner", optionalIntLeaf("element"));
        SchemaNode.Group element =
                new SchemaNode.Group("element", Repetition.OPTIONAL, List.of(inner), Optional.empty(), -1);
        return rootOf(listWrapper("outer", element));
    }

    private static ParquetSchema listOfStructOfStructOfListSchema() {
        SchemaNode.Group i = listGroup("i", optionalIntLeaf("element"));
        SchemaNode.Group h = new SchemaNode.Group("h", Repetition.OPTIONAL, List.of(i), Optional.empty(), -1);
        SchemaNode.Group element =
                new SchemaNode.Group("element", Repetition.OPTIONAL, List.of(h), Optional.empty(), -1);
        return rootOf(listWrapper("outer", element));
    }

    private static ParquetSchema mapOfStructOfListSchema() {
        SchemaNode.Primitive key = new SchemaNode.Primitive(
                "key",
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Group i = listGroup("i", optionalIntLeaf("element"));
        SchemaNode.Group value = new SchemaNode.Group("value", Repetition.OPTIONAL, List.of(i), Optional.empty(), -1);
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        SchemaNode.Group m = new SchemaNode.Group(
                "m", Repetition.OPTIONAL, List.of(keyValue), Optional.of(new LogicalType.MapType()), -1);
        return rootOf(m);
    }

    /** Builds {@code <name> (LIST, OPTIONAL) { repeated list { <child> } }}. */
    private static SchemaNode.Group listGroup(String name, SchemaNode child) {
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(child), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
    }

    private static SchemaNode.Group listWrapper(String name, SchemaNode element) {
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
    }

    private static SchemaNode.Primitive optionalIntLeaf(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static ParquetSchema rootOf(SchemaNode field) {
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(field), Optional.empty(), -1));
    }

    // --- helpers ---

    private static java.lang.foreign.MemorySegment utf8(String s) {
        return java.lang.foreign.MemorySegment.ofArray(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String validityBits(Validity validity) {
        StringBuilder sb = new StringBuilder(validity.size());
        for (int i = 0; i < validity.size(); i++) {
            sb.append(validity.isValid(i) ? '1' : '0');
        }
        return sb.toString();
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
