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
package io.tileverse.parquetry.materializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.IntSequence;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.LeafLevels;
import io.tileverse.parquetry.columnar.LevelListVector;
import io.tileverse.parquetry.columnar.Levels;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Exercises {@link LevelListVector} and its cursor-backed view over single-leaf list subtrees, driving the level
 * arithmetic from handcrafted rep/def streams and dense entry-aligned leaf vectors.
 */
// S125: the schema and level-stream derivations are documentation, not code
@SuppressWarnings("java:S125")
class LevelListViewTest {

    // optional list of optional double.
    //   optional group xs (LIST) { repeated group list { optional double element } }
    //   maxDef(xs)=1, maxDef(xs.list)=2, maxDef(element)=3, maxRep(xs.list)=1
    //   Rows: [1.0, 2.0, 3.0], [], null, [4.0], [5.0, null, 6.0]
    @Test
    void optionalListOfOptionalDouble() {
        ParquetSchema schema = listOfDoubleSchema(Repetition.OPTIONAL, Repetition.OPTIONAL);
        ColumnPath leafPath = ColumnPath.of("xs", "list", "element");

        // Entry stream, one slot per entry; phantom entries occupy slots too.
        // row0: 1.0(r0,d3) 2.0(r1,d3) 3.0(r1,d3)
        // row1 empty: phantom(r0,d1)
        // row2 null:  phantom(r0,d0)
        // row3: 4.0(r0,d3)
        // row4: 5.0(r0,d3) null(r1,d2) 6.0(r1,d3)
        double[] values = {1.0, 2.0, 3.0, 0.0, 0.0, 4.0, 5.0, 0.0, 6.0};
        int[] rep = {0, 1, 1, 0, 0, 0, 0, 1, 1};
        int[] def = {3, 3, 3, 1, 0, 3, 3, 2, 3};
        int[] rowStarts = {0, 3, 4, 5, 6, 9};

        BitSet leafValid = new BitSet(values.length);
        for (int i = 0; i < def.length; i++) {
            if (def[i] == 3) {
                leafValid.set(i);
            }
        }
        DoubleVector leaf = DoubleVector.materialized(values, Validity.of(leafValid, values.length));

        Validity rowValidity = rowValidity(new boolean[] {true, true, false, true, true});
        LevelListVector vec = LevelListVector.of(
                schema,
                ColumnPath.of("xs"),
                Map.of(leafPath, leaf),
                Map.of(leafPath, leafLevels(rep, def, rowStarts)),
                rowValidity,
                5);

        assertThat(LevelListMaterializer.materializeAt(vec, 0)).isEqualTo(List.of(1.0, 2.0, 3.0));
        assertThat(LevelListMaterializer.materializeAt(vec, 1)).isEqualTo(List.of());
        assertThat(LevelListMaterializer.materializeAt(vec, 2)).isNull();
        assertThat(LevelListMaterializer.materializeAt(vec, 3)).isEqualTo(List.of(4.0));
        List<?> row4 = LevelListMaterializer.materializeAt(vec, 4);
        assertThat(row4).hasSize(3);
        assertThat(row4.get(0)).isEqualTo(5.0);
        assertThat(row4.get(1)).isNull();
        assertThat(row4.get(2)).isEqualTo(6.0);
    }

    // required list of required elements collapses the optional levels.
    //   required group xs (LIST) { repeated group list { required double element } }
    //   maxDef(xs)=0, maxDef(xs.list)=1, maxDef(element)=1, maxRep(xs.list)=1
    //   Rows: [1.0, 2.0], [3.0]
    @Test
    void requiredListOfRequiredElements() {
        ParquetSchema schema = listOfDoubleSchema(Repetition.REQUIRED, Repetition.REQUIRED);
        ColumnPath leafPath = ColumnPath.of("xs", "list", "element");

        double[] values = {1.0, 2.0, 3.0};
        int[] rep = {0, 1, 0};
        int[] def = {1, 1, 1};
        int[] rowStarts = {0, 2, 3};

        DoubleVector leaf = DoubleVector.materialized(values, Validity.allValid(3));
        LevelListVector vec = LevelListVector.of(
                schema,
                ColumnPath.of("xs"),
                Map.of(leafPath, leaf),
                Map.of(leafPath, leafLevels(rep, def, rowStarts)),
                Validity.allValid(2),
                2);

        assertThat(LevelListMaterializer.materializeAt(vec, 0)).isEqualTo(List.of(1.0, 2.0));
        assertThat(LevelListMaterializer.materializeAt(vec, 1)).isEqualTo(List.of(3.0));
    }

    // list of list of int.
    //   optional group xs (LIST) { repeated group list { optional group element (LIST) {
    //       repeated group list { optional int32 element } } } }
    //   maxDef(xs)=1, maxDef(xs.list)=2, maxDef(outer element)=3, maxDef(inner list)=4, maxDef(inner element)=5
    //   maxRep(outer list)=1, maxRep(inner list)=2
    //   Row0: [[1,2],[],null], Row1: [], Row2: null
    @Test
    void listOfListOfInt() {
        ParquetSchema schema = listOfListOfIntSchema();
        ColumnPath leafPath = ColumnPath.of("xs", "list", "element", "list", "element");

        // Row0:
        //  inner [1,2]: 1(r0,d5) 2(r2,d5)
        //  inner []:    phantom(r1,d3)  (outer element present, inner container empty)
        //  inner null:  phantom(r1,d2)  (outer element present, inner container absent)
        // Row1 outer empty: phantom(r0,d1)
        // Row2 outer null:  phantom(r0,d0)
        int[] vals = {1, 2, 0, 0, 0, 0};
        int[] rep = {0, 2, 1, 1, 0, 0};
        int[] def = {5, 5, 3, 2, 1, 0};
        int[] rowStarts = {0, 4, 5, 6};

        BitSet leafValid = new BitSet(vals.length);
        for (int i = 0; i < def.length; i++) {
            if (def[i] == 5) {
                leafValid.set(i);
            }
        }
        IntVector leaf = IntVector.materialized(vals, Validity.of(leafValid, vals.length));

        Validity rowValidity = rowValidity(new boolean[] {true, true, false});
        LevelListVector vec = LevelListVector.of(
                schema,
                ColumnPath.of("xs"),
                Map.of(leafPath, leaf),
                Map.of(leafPath, leafLevels(rep, def, rowStarts)),
                rowValidity,
                3);

        List<?> row0 = LevelListMaterializer.materializeAt(vec, 0);
        assertThat(row0).hasSize(3);
        assertThat(row0.get(0)).asInstanceOf(InstanceOfAssertFactories.LIST).containsExactly(1, 2);
        assertThat(row0.get(1)).isEqualTo(List.of());
        assertThat(row0.get(2)).isNull();

        assertThat(LevelListMaterializer.materializeAt(vec, 1)).isEqualTo(List.of());
        assertThat(LevelListMaterializer.materializeAt(vec, 2)).isNull();
    }

    // legacy two-level list: the repeated primitive is the element.
    //   optional group xs (LIST) { repeated int32 element }
    //   maxDef(xs)=1, maxDef(element)=2, maxRep(element)=1
    //   Rows: [10, 20], [30]
    @Test
    void legacyTwoLevelList() {
        ParquetSchema schema = twoLevelListSchema();
        ColumnPath leafPath = ColumnPath.of("xs", "element");

        int[] vals = {10, 20, 30};
        int[] rep = {0, 1, 0};
        int[] def = {2, 2, 2};
        int[] rowStarts = {0, 2, 3};

        IntVector leaf = IntVector.materialized(vals, Validity.allValid(3));
        LevelListVector vec = LevelListVector.of(
                schema,
                ColumnPath.of("xs"),
                Map.of(leafPath, leaf),
                Map.of(leafPath, leafLevels(rep, def, rowStarts)),
                Validity.allValid(2),
                2);

        assertThat(LevelListMaterializer.materializeAt(vec, 0)).isEqualTo(List.of(10, 20));
        assertThat(LevelListMaterializer.materializeAt(vec, 1)).isEqualTo(List.of(30));
    }

    // binary elements read back as their backing segments.
    //   optional group xs (LIST) { repeated group list { optional binary element } }
    @Test
    void binaryElements() {
        ParquetSchema schema = listOfBinarySchema();
        ColumnPath leafPath = ColumnPath.of("xs", "list", "element");

        MemorySegment[] vals = {utf8("a"), utf8("b"), utf8("c")};
        int[] rep = {0, 1, 0};
        int[] def = {3, 3, 3};
        int[] rowStarts = {0, 2, 3};

        BinaryVector leaf = BinaryVector.materialized(vals, Validity.allValid(3));
        LevelListVector vec = LevelListVector.of(
                schema,
                ColumnPath.of("xs"),
                Map.of(leafPath, leaf),
                Map.of(leafPath, leafLevels(rep, def, rowStarts)),
                Validity.allValid(2),
                2);

        List<?> row0 = LevelListMaterializer.materializeAt(vec, 0);
        assertThat(row0).hasSize(2);
        assertThat(text((MemorySegment) row0.get(0))).isEqualTo("a");
        assertThat(text((MemorySegment) row0.get(1))).isEqualTo("b");
        assertThat(text((MemorySegment)
                        LevelListMaterializer.materializeAt(vec, 1).get(0)))
                .isEqualTo("c");
    }

    // cursor: sequential iteration, indexed access, and rewind after the tail.
    @Test
    void cursorRewindAndBounds() {
        ParquetSchema schema = listOfDoubleSchema(Repetition.OPTIONAL, Repetition.OPTIONAL);
        ColumnPath leafPath = ColumnPath.of("xs", "list", "element");

        double[] values = {1.0, 2.0, 3.0, 4.0};
        int[] rep = {0, 1, 1, 1};
        int[] def = {3, 3, 3, 3};
        int[] rowStarts = {0, 4};

        DoubleVector leaf = DoubleVector.materialized(values, Validity.allValid(4));
        LevelListVector vec = LevelListVector.of(
                schema,
                ColumnPath.of("xs"),
                Map.of(leafPath, leaf),
                Map.of(leafPath, leafLevels(rep, def, rowStarts)),
                Validity.allValid(1),
                1);

        List<?> list = LevelListMaterializer.materializeAt(vec, 0);
        assertThat(list).hasSize(4);
        assertThat(list).isEqualTo(List.of(1.0, 2.0, 3.0, 4.0));
        // get(size-1) then get(0) forces the cursor to rewind to the window start.
        assertThat(list.get(3)).isEqualTo(4.0);
        assertThat(list.get(0)).isEqualTo(1.0);
        assertThatThrownBy(() -> list.get(4)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> list.get(-1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    // a struct-typed element materializes to a record reading its fields from the descendant leaves.
    //   maxDef(xs.list.element)=3, maxDef(v)=4
    @Test
    void structElementMaterializesToRecord() {
        ParquetSchema schema = listOfStructSchema();
        ColumnPath leafPath = ColumnPath.of("xs", "list", "element", "v");

        int[] vals = {1, 2};
        int[] rep = {0, 1};
        int[] def = {4, 4};
        int[] rowStarts = {0, 2};

        IntVector leaf = IntVector.materialized(vals, Validity.allValid(2));
        LevelListVector vec = LevelListVector.of(
                schema,
                ColumnPath.of("xs"),
                Map.of(leafPath, leaf),
                Map.of(leafPath, leafLevels(rep, def, rowStarts)),
                Validity.allValid(1),
                1);

        List<?> list = LevelListMaterializer.materializeAt(vec, 0);
        assertThat(list).hasSize(2);
        assertThat(((ParquetRecord) list.get(0)).getInt(0)).isEqualTo(1);
        assertThat(((ParquetRecord) list.get(1)).getInt(0)).isEqualTo(2);
    }

    // the record API resolves the level-backed list and reports a list kind on a primitive accessor.
    @Test
    void recordApiReadsListAndReportsKind() {
        ParquetSchema schema = listOfDoubleSchema(Repetition.OPTIONAL, Repetition.OPTIONAL);
        ColumnPath leafPath = ColumnPath.of("xs", "list", "element");
        ColumnPath listPath = ColumnPath.of("xs");

        double[] values = {1.0, 2.0, 3.0};
        int[] rep = {0, 1, 0};
        int[] def = {3, 3, 3};
        int[] rowStarts = {0, 2, 3};

        DoubleVector leaf = DoubleVector.materialized(values, Validity.allValid(3));
        LevelListVector vec = LevelListVector.of(
                schema,
                listPath,
                Map.of(leafPath, leaf),
                Map.of(leafPath, leafLevels(rep, def, rowStarts)),
                Validity.allValid(2),
                2);

        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(listPath, vec);
        DefaultParquetRecordBatch batch = DefaultParquetRecordBatch.ofHeap(schema, columns, 2);
        try {
            ParquetRecord row0 = batch.materialize(0);
            assertThat(row0.get(listPath))
                    .asInstanceOf(InstanceOfAssertFactories.LIST)
                    .containsExactly(1.0, 2.0);
            assertThatThrownBy(() -> row0.getInt(listPath))
                    .isInstanceOf(ParquetSchemaException.class)
                    .hasMessageContaining("is LIST");

            ParquetRecord row1 = batch.materialize(1);
            assertThat(row1.get(listPath))
                    .asInstanceOf(InstanceOfAssertFactories.LIST)
                    .containsExactly(3.0);
        } finally {
            batch.close();
        }
    }

    // a shredded Variant element (a Variant group with a typed_value child) under a list is rejected: the per-element
    // level windows here materialize only the unshredded {metadata, value} pair, never the shredded subtree.
    @Test
    void shreddedVariantElementUnderListIsRejected() {
        ParquetSchema schema = listOfShreddedVariantSchema();
        ColumnPath metadataLeaf = ColumnPath.of("xs", "list", "element", "metadata");

        BinaryVector leaf = BinaryVector.materialized(new MemorySegment[] {utf8("m")}, Validity.allValid(1));
        assertThatThrownBy(() -> LevelListVector.of(
                        schema,
                        ColumnPath.of("xs"),
                        Map.of(metadataLeaf, leaf),
                        Map.of(metadataLeaf, leafLevels(new int[] {0}, new int[] {3}, new int[] {0, 1})),
                        Validity.allValid(1),
                        1))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining("list or map");
    }

    // heap accounting covers children, validity, and the row-start indexes.
    @Test
    void heapBytesCoversChildrenValidityAndRowStarts() {
        ParquetSchema schema = listOfDoubleSchema(Repetition.OPTIONAL, Repetition.OPTIONAL);
        ColumnPath leafPath = ColumnPath.of("xs", "list", "element");

        double[] values = {1.0, 2.0, 3.0};
        int[] rep = {0, 1, 0};
        int[] def = {3, 3, 3};
        int[] rowStarts = {0, 2, 3};

        DoubleVector leaf = DoubleVector.materialized(values, Validity.allValid(3));
        Validity rowValidity = rowValidity(new boolean[] {false, true});
        LeafLevels levels = leafLevels(rep, def, rowStarts);
        LevelListVector vec = LevelListVector.of(
                schema, ColumnPath.of("xs"), Map.of(leafPath, leaf), Map.of(leafPath, levels), rowValidity, 2);

        long expected = leaf.approximateHeapBytes()
                + rowValidity.heapBytes()
                + levels.rowStarts().heapBytes();
        assertThat(vec.approximateHeapBytes()).isEqualTo(expected);
    }

    // --- fixtures ---

    private static LeafLevels leafLevels(int[] rep, int[] def, int[] rowStarts) {
        return new LeafLevels(Levels.of(rep), Levels.of(def), IntSequence.of(rowStarts));
    }

    private static Validity rowValidity(boolean[] valid) {
        BitSet bits = new BitSet(valid.length);
        for (int i = 0; i < valid.length; i++) {
            if (valid[i]) {
                bits.set(i);
            }
        }
        return Validity.of(bits, valid.length);
    }

    private static MemorySegment utf8(String s) {
        return MemorySegment.ofArray(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String text(MemorySegment segment) {
        byte[] out = new byte[(int) segment.byteSize()];
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, 0, out, 0, out.length);
        return new String(out, StandardCharsets.UTF_8);
    }

    private static ParquetSchema listOfDoubleSchema(Repetition listRep, Repetition elementRep) {
        SchemaNode.Primitive element = primitive("element", elementRep, PrimitiveKind.DOUBLE);
        SchemaNode.Group middle = repeatedGroup("list", List.of(element));
        SchemaNode.Group listGroup = listGroup("xs", listRep, List.of(middle));
        return rootSchema(listGroup);
    }

    private static ParquetSchema listOfBinarySchema() {
        SchemaNode.Primitive element = primitive("element", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY);
        SchemaNode.Group middle = repeatedGroup("list", List.of(element));
        SchemaNode.Group listGroup = listGroup("xs", Repetition.OPTIONAL, List.of(middle));
        return rootSchema(listGroup);
    }

    private static ParquetSchema listOfListOfIntSchema() {
        SchemaNode.Primitive innerElement = primitive("element", Repetition.OPTIONAL, PrimitiveKind.INT32);
        SchemaNode.Group innerMiddle = repeatedGroup("list", List.of(innerElement));
        SchemaNode.Group innerList = listGroup("element", Repetition.OPTIONAL, List.of(innerMiddle));
        SchemaNode.Group outerMiddle = repeatedGroup("list", List.of(innerList));
        SchemaNode.Group outerList = listGroup("xs", Repetition.OPTIONAL, List.of(outerMiddle));
        return rootSchema(outerList);
    }

    private static ParquetSchema listOfStructSchema() {
        SchemaNode.Primitive v = primitive("v", Repetition.OPTIONAL, PrimitiveKind.INT32);
        SchemaNode.Group element =
                new SchemaNode.Group("element", Repetition.OPTIONAL, List.of(v), Optional.empty(), -1);
        SchemaNode.Group middle = repeatedGroup("list", List.of(element));
        SchemaNode.Group listGroup = listGroup("xs", Repetition.OPTIONAL, List.of(middle));
        return rootSchema(listGroup);
    }

    private static ParquetSchema listOfShreddedVariantSchema() {
        SchemaNode.Primitive metadata = primitive("metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY);
        SchemaNode.Primitive value = primitive("value", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY);
        SchemaNode.Primitive typedValue = primitive("typed_value", Repetition.OPTIONAL, PrimitiveKind.INT64);
        SchemaNode.Group variant = new SchemaNode.Group(
                "element",
                Repetition.OPTIONAL,
                List.of(metadata, value, typedValue),
                Optional.of(new LogicalType.Variant()),
                -1);
        SchemaNode.Group middle = repeatedGroup("list", List.of(variant));
        SchemaNode.Group listGroup = listGroup("xs", Repetition.OPTIONAL, List.of(middle));
        return rootSchema(listGroup);
    }

    private static ParquetSchema twoLevelListSchema() {
        SchemaNode.Primitive element = primitive("element", Repetition.REPEATED, PrimitiveKind.INT32);
        SchemaNode.Group listGroup = listGroup("xs", Repetition.OPTIONAL, List.of(element));
        return rootSchema(listGroup);
    }

    private static SchemaNode.Primitive primitive(String name, Repetition repetition, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, repetition, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Group repeatedGroup(String name, List<SchemaNode> children) {
        return new SchemaNode.Group(name, Repetition.REPEATED, children, Optional.empty(), -1);
    }

    private static SchemaNode.Group listGroup(String name, Repetition repetition, List<SchemaNode> children) {
        return new SchemaNode.Group(name, repetition, children, Optional.of(new LogicalType.ListType()), -1);
    }

    private static ParquetSchema rootSchema(SchemaNode.Group field) {
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(field), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
