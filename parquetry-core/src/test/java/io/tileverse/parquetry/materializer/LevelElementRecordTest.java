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
package io.tileverse.parquetry.materializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.IntSequence;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.LeafLevels;
import io.tileverse.parquetry.columnar.LevelListVector;
import io.tileverse.parquetry.columnar.Levels;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.columnar.VariantVector;
import io.tileverse.parquetry.data.variant.Variant;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Exercises {@link LevelElementRecord}: struct-typed and Variant-typed list elements materialized from the level form
 * by navigating each descendant leaf's own rep/def streams. The fixtures hand-derive every rep/def number from the
 * schema's level maxima.
 */
// S125: the schema and level-stream derivations are documentation, not code
@SuppressWarnings("java:S125")
class LevelElementRecordTest {

    // list<struct<a: double optional, b: string optional>>.
    //   optional group xs (LIST) { repeated group list { optional group element {
    //       optional double a; optional binary b (STRING); } } }
    //   maxDef(xs)=1, maxDef(xs.list)=2 (d), maxDef(element)=3, maxDef(a)=maxDef(b)=4; maxRep(xs.list)=1 (e)
    //   Rows: [{1.0,"x"},{null,"y"}], [null_struct, {3.0,"z"}], [], null
    @Test
    void structElementsReadFieldsAndNullStruct() {
        ParquetSchema schema = listOfStructAbSchema();
        ColumnPath aPath = ColumnPath.of("xs", "list", "element", "a");
        ColumnPath bPath = ColumnPath.of("xs", "list", "element", "b");

        // leaf a: present struct field reaches def 4; struct present but a null -> def 3;
        //         null struct -> def 2; empty list -> def 1; null row -> def 0.
        double[] aVals = {1.0, 0.0, 0.0, 3.0, 0.0, 0.0};
        int[] aRep = {0, 1, 0, 1, 0, 0};
        int[] aDef = {4, 3, 2, 4, 1, 0};
        int[] rowStarts = {0, 2, 4, 5, 6};
        DoubleVector aLeaf = DoubleVector.materialized(aVals, validityWhere(aDef, 4));

        MemorySegment[] bVals = {utf8("x"), utf8("y"), null, utf8("z"), null, null};
        int[] bRep = {0, 1, 0, 1, 0, 0};
        int[] bDef = {4, 4, 2, 4, 1, 0};
        BinaryVector bLeaf = BinaryVector.materialized(bVals, validityWhere(bDef, 4));

        LevelListVector vec = LevelListVector.of(
                schema,
                ColumnPath.of("xs"),
                Map.of(aPath, aLeaf, bPath, bLeaf),
                Map.of(aPath, leafLevels(aRep, aDef, rowStarts), bPath, leafLevels(bRep, bDef, rowStarts)),
                rowValidity(new boolean[] {true, true, true, false}),
                4);

        List<?> row0 = LevelListMaterializer.materializeAt(vec, 0);
        assertThat(row0).hasSize(2);
        ParquetRecord e00 = (ParquetRecord) row0.get(0);
        assertThat(e00.getDouble(0)).isEqualTo(1.0);
        assertThat(e00.getString(1)).isEqualTo("x");
        assertThat(e00.getDouble(ColumnPath.of("a"))).isEqualTo(1.0);
        assertThat(e00.getString(ColumnPath.of("b"))).isEqualTo("x");

        ParquetRecord e01 = (ParquetRecord) row0.get(1);
        assertThat(e01.isNull(0)).isTrue();
        assertThat(e01.getString(1)).isEqualTo("y");

        List<?> row1 = LevelListMaterializer.materializeAt(vec, 1);
        assertThat(row1).hasSize(2);
        assertThat(row1.get(0)).isNull();
        ParquetRecord e11 = (ParquetRecord) row1.get(1);
        assertThat(e11.getDouble(0)).isEqualTo(3.0);
        assertThat(e11.getString(1)).isEqualTo("z");

        assertThat(LevelListMaterializer.materializeAt(vec, 2)).isEqualTo(List.of());
        assertThat(LevelListMaterializer.materializeAt(vec, 3)).isNull();

        // fresh record per get
        assertThat(row0.get(0)).isNotSameAs(row0.get(0));
    }

    // differing per-leaf entry counts: list<struct<a: int optional, bs: list<double> optional>>.
    //   optional group xs (LIST) { repeated group list { optional group element {
    //       optional int32 a;
    //       optional group bs (LIST) { repeated group list { optional double element } } } } }
    //   maxRep(xs.list)=1 (e), maxRep(bs.list)=2; maxDef(xs)=1, maxDef(xs.list)=2 (d), maxDef(element)=3,
    //   maxDef(a)=4, maxDef(bs)=4, maxDef(bs.list)=5, maxDef(bs.element)=6.
    //   Row0 (after an empty row to prove rowStarts alignment): [ {a:7, bs:[1.0,2.0]}, {a:8, bs:[]},
    //                                                             {a:9, bs:null}, {a:10, bs:[3.0,null]} ]
    @Test
    void differingPerLeafEntryCounts() {
        ParquetSchema schema = listOfStructWithInnerListSchema();
        ColumnPath aPath = ColumnPath.of("xs", "list", "element", "a");
        ColumnPath bsPath = ColumnPath.of("xs", "list", "element", "bs", "list", "element");

        // leaf a has one entry per struct element. Row 0 is empty (phantom); Row 1 holds the four elements.
        int[] aVals = {0, 7, 8, 9, 10};
        int[] aRep = {0, 0, 1, 1, 1};
        int[] aDef = {1, 4, 4, 4, 4};
        int[] aRowStarts = {0, 1, 5};
        IntVector aLeaf = IntVector.materialized(aVals, validityWhere(aDef, 4));

        // leaf bs.element: el0 bs=[1.0,2.0]; el1 bs=[] (def 4); el2 bs=null (def 3); el3 bs=[3.0,null].
        //   rep at the element repetition level e'=2 inside bs; the struct boundary is rep<=1.
        // Row0 empty outer list -> phantom def 1.
        double[] bVals = {0.0, 1.0, 2.0, 0.0, 0.0, 3.0, 0.0};
        int[] bRep = {0, 0, 2, 1, 1, 1, 2};
        int[] bDef = {1, 6, 6, 4, 3, 6, 5};
        int[] bRowStarts = {0, 1, 7};
        DoubleVector bLeaf = DoubleVector.materialized(bVals, validityWhere(bDef, 6));

        LevelListVector vec = LevelListVector.of(
                schema,
                ColumnPath.of("xs"),
                Map.of(aPath, aLeaf, bsPath, bLeaf),
                Map.of(aPath, leafLevels(aRep, aDef, aRowStarts), bsPath, leafLevels(bRep, bDef, bRowStarts)),
                rowValidity(new boolean[] {true, true}),
                2);

        assertThat(LevelListMaterializer.materializeAt(vec, 0)).isEqualTo(List.of());

        List<?> row1 = LevelListMaterializer.materializeAt(vec, 1);
        assertThat(row1).hasSize(4);

        ParquetRecord e0 = (ParquetRecord) row1.get(0);
        assertThat(e0.getInt(0)).isEqualTo(7);
        assertThat(e0.readList(ColumnPath.of("bs")))
                .asInstanceOf(InstanceOfAssertFactories.LIST)
                .containsExactly(1.0, 2.0);

        ParquetRecord e1 = (ParquetRecord) row1.get(1);
        assertThat(e1.getInt(0)).isEqualTo(8);
        assertThat(e1.readList(ColumnPath.of("bs"))).isEqualTo(List.of());

        ParquetRecord e2 = (ParquetRecord) row1.get(2);
        assertThat(e2.getInt(0)).isEqualTo(9);
        assertThat(e2.readList(ColumnPath.of("bs"))).isNull();

        ParquetRecord e3 = (ParquetRecord) row1.get(3);
        assertThat(e3.getInt(0)).isEqualTo(10);
        List<?> e3bs = e3.readList(ColumnPath.of("bs"));
        assertThat(e3bs).hasSize(2);
        assertThat(e3bs.get(0)).isEqualTo(3.0);
        assertThat(e3bs.get(1)).isNull();
    }

    // nested struct: list<struct<inner: struct<x: int>>>.
    //   optional group xs (LIST) { repeated group list { optional group element {
    //       optional group inner { optional int32 x; } } } }
    //   maxDef(xs.list)=2 (d), maxDef(element)=3, maxDef(inner)=4, maxDef(x)=5; maxRep(xs.list)=1.
    //   Row0: [ {inner:{x:5}}, {inner:null} ]
    @Test
    void nestedStructRecursionAndTypedMismatch() {
        ParquetSchema schema = listOfNestedStructSchema();
        ColumnPath xPath = ColumnPath.of("xs", "list", "element", "inner", "x");

        int[] xVals = {5, 0};
        int[] xRep = {0, 1};
        int[] xDef = {5, 3};
        int[] rowStarts = {0, 2};
        IntVector xLeaf = IntVector.materialized(xVals, validityWhere(xDef, 5));

        LevelListVector vec = LevelListVector.of(
                schema,
                ColumnPath.of("xs"),
                Map.of(xPath, xLeaf),
                Map.of(xPath, leafLevels(xRep, xDef, rowStarts)),
                rowValidity(new boolean[] {true}),
                1);

        List<?> row0 = LevelListMaterializer.materializeAt(vec, 0);
        assertThat(row0).hasSize(2);

        ParquetRecord e0 = (ParquetRecord) row0.get(0);
        ParquetRecord inner = e0.readStruct(0);
        assertThat(inner.getInt(0)).isEqualTo(5);
        assertThat(e0.getInt(ColumnPath.of("inner", "x"))).isEqualTo(5);
        assertThat(e0.get(ColumnPath.of("inner", "x"))).isEqualTo(5);
        assertThatThrownBy(() -> e0.getInt(0))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining("is STRUCT");

        ParquetRecord e1 = (ParquetRecord) row0.get(1);
        assertThat(e1.readStruct(0)).isNull();
        assertThat(e1.isNull(0)).isTrue();
        assertThat(e1.get(ColumnPath.of("inner", "x"))).isNull();
    }

    // projection drops field b: a field whose leaf is absent from the leaf map.
    @Test
    void projectionDroppedField() {
        ParquetSchema schema = listOfStructAbSchema();
        ColumnPath aPath = ColumnPath.of("xs", "list", "element", "a");

        double[] aVals = {1.0};
        int[] aRep = {0};
        int[] aDef = {4};
        int[] rowStarts = {0, 1};
        DoubleVector aLeaf = DoubleVector.materialized(aVals, validityWhere(aDef, 4));

        LevelListVector vec = LevelListVector.of(
                schema,
                ColumnPath.of("xs"),
                Map.of(aPath, aLeaf),
                Map.of(aPath, leafLevels(aRep, aDef, rowStarts)),
                rowValidity(new boolean[] {true}),
                1);

        ParquetRecord e0 =
                (ParquetRecord) LevelListMaterializer.materializeAt(vec, 0).get(0);
        assertThat(e0.getDouble(0)).isEqualTo(1.0);
        assertThat(e0.isNull(1)).isTrue();
        assertThat(e0.get(1)).isNull();
        // A binary field reads leniently to null, matching DefaultParquetRecord; a typed numeric accessor rejects it.
        assertThat(e0.getString(1)).isNull();
        assertThatThrownBy(() -> e0.getInt(1))
                .isInstanceOf(ParquetSchemaException.class)
                .hasMessageContaining("is absent");
    }

    // list<variant>: unshredded Variant group with metadata + value binary leaves.
    //   optional group xs (LIST) { repeated group list { optional group element (VARIANT) {
    //       binary metadata; binary value; } } }
    @Test
    void variantElementsMatchVariantVector() {
        ParquetSchema schema = listOfVariantSchema();
        ColumnPath metadataPath = ColumnPath.of("xs", "list", "element", "metadata");
        ColumnPath valuePath = ColumnPath.of("xs", "list", "element", "value");

        MemorySegment metaSeg = emptyVariantMetadata();
        MemorySegment v0 = variantInt32(11);
        MemorySegment v1 = variantInt32(22);

        MemorySegment[] metas = {metaSeg, metaSeg};
        MemorySegment[] values = {v0, v1};
        int[] rep = {0, 1};
        int[] def = {4, 4};
        int[] rowStarts = {0, 2};
        BinaryVector metadataLeaf = BinaryVector.materialized(metas, Validity.allValid(2));
        BinaryVector valueLeaf = BinaryVector.materialized(values, Validity.allValid(2));

        LevelListVector vec = LevelListVector.of(
                schema,
                ColumnPath.of("xs"),
                Map.of(metadataPath, metadataLeaf, valuePath, valueLeaf),
                Map.of(metadataPath, leafLevels(rep, def, rowStarts), valuePath, leafLevels(rep, def, rowStarts)),
                rowValidity(new boolean[] {true}),
                1);

        VariantVector oracle = new VariantVector(metadataLeaf, valueLeaf, Validity.allValid(2), 2);
        List<?> row0 = LevelListMaterializer.materializeAt(vec, 0);
        assertThat(row0).hasSize(2);
        assertThat(((Variant) row0.get(0)).getInt()).isEqualTo(oracle.get(0).getInt());
        assertThat(((Variant) row0.get(1)).getInt()).isEqualTo(oracle.get(1).getInt());
    }

    // detach: a record from the differing-per-leaf fixture survives and equals the live reads.
    @Test
    void detachSurvivesAndEquals() {
        ParquetSchema schema = listOfStructWithInnerListSchema();
        ColumnPath aPath = ColumnPath.of("xs", "list", "element", "a");
        ColumnPath bsPath = ColumnPath.of("xs", "list", "element", "bs", "list", "element");

        int[] aVals = {7, 10};
        int[] aRep = {0, 1};
        int[] aDef = {4, 4};
        int[] aRowStarts = {0, 2};
        IntVector aLeaf = IntVector.materialized(aVals, validityWhere(aDef, 4));

        double[] bVals = {1.0, 2.0, 3.0, 0.0};
        int[] bRep = {0, 2, 1, 2};
        int[] bDef = {6, 6, 6, 5};
        int[] bRowStarts = {0, 4};
        DoubleVector bLeaf = DoubleVector.materialized(bVals, validityWhere(bDef, 6));

        LevelListVector vec = LevelListVector.of(
                schema,
                ColumnPath.of("xs"),
                Map.of(aPath, aLeaf, bsPath, bLeaf),
                Map.of(aPath, leafLevels(aRep, aDef, aRowStarts), bsPath, leafLevels(bRep, bDef, bRowStarts)),
                rowValidity(new boolean[] {true}),
                1);

        List<?> row0 = LevelListMaterializer.materializeAt(vec, 0);
        ParquetRecord live = (ParquetRecord) row0.get(0);
        ParquetRecord detached = live.detach();
        assertThat(detached.getInt(0)).isEqualTo(7);
        assertThat(detached.readList(ColumnPath.of("bs")))
                .asInstanceOf(InstanceOfAssertFactories.LIST)
                .containsExactly(1.0, 2.0);
    }

    // cursor integrity: sequential iteration equals indexed access; get(0) after get(2).
    @Test
    void cursorIntegrityAcrossAccessOrder() {
        ParquetSchema schema = listOfStructAbSchema();
        ColumnPath aPath = ColumnPath.of("xs", "list", "element", "a");
        ColumnPath bPath = ColumnPath.of("xs", "list", "element", "b");

        double[] aVals = {1.0, 2.0, 3.0};
        int[] aRep = {0, 1, 1};
        int[] aDef = {4, 4, 4};
        int[] rowStarts = {0, 3};
        DoubleVector aLeaf = DoubleVector.materialized(aVals, validityWhere(aDef, 4));

        MemorySegment[] bVals = {utf8("p"), utf8("q"), utf8("r")};
        int[] bRep = {0, 1, 1};
        int[] bDef = {4, 4, 4};
        BinaryVector bLeaf = BinaryVector.materialized(bVals, Validity.allValid(3));

        LevelListVector vec = LevelListVector.of(
                schema,
                ColumnPath.of("xs"),
                Map.of(aPath, aLeaf, bPath, bLeaf),
                Map.of(aPath, leafLevels(aRep, aDef, rowStarts), bPath, leafLevels(bRep, bDef, rowStarts)),
                rowValidity(new boolean[] {true}),
                1);

        List<?> row0 = LevelListMaterializer.materializeAt(vec, 0);
        double[] sequential = new double[3];
        for (int i = 0; i < 3; i++) {
            sequential[i] = ((ParquetRecord) row0.get(i)).getDouble(0);
        }
        assertThat(sequential).containsExactly(1.0, 2.0, 3.0);

        double third = ((ParquetRecord) row0.get(2)).getDouble(0);
        double first = ((ParquetRecord) row0.get(0)).getDouble(0);
        assertThat(third).isEqualTo(3.0);
        assertThat(first).isEqualTo(1.0);
    }

    // map-typed fields materialize to an insertion-ordered map.
    @Test
    void mapFieldMaterializes() {
        ParquetSchema schema = listOfStructWithMapSchema();
        ColumnPath kPath = ColumnPath.of("xs", "list", "element", "m", "key_value", "key");
        ColumnPath vPath = ColumnPath.of("xs", "list", "element", "m", "key_value", "value");

        int[] kVals = {1};
        int[] kRep = {0};
        int[] kDef = {5};
        int[] rowStarts = {0, 1};
        IntVector kLeaf = IntVector.materialized(kVals, Validity.allValid(1));
        IntVector vLeaf = IntVector.materialized(new int[] {2}, Validity.allValid(1));

        LevelListVector vec = LevelListVector.of(
                schema,
                ColumnPath.of("xs"),
                Map.of(kPath, kLeaf, vPath, vLeaf),
                Map.of(kPath, leafLevels(kRep, kDef, rowStarts), vPath, leafLevels(kRep, kDef, rowStarts)),
                rowValidity(new boolean[] {true}),
                1);

        ParquetRecord e0 =
                (ParquetRecord) LevelListMaterializer.materializeAt(vec, 0).get(0);
        assertThat(e0.readMap(0)).isEqualTo(Map.of(1, 2));
    }

    // struct elements inside a nested list field: list<struct<a: int, bs: list<struct<x: int>>>>.
    //   optional group xs (LIST) { repeated group list { optional group element {
    //       optional int32 a;
    //       optional group bs (LIST) { repeated group list { optional group element { optional int32 x } } } } } }
    //   maxRep(xs.list)=1 (outer e), maxRep(bs.list)=2 (inner e); maxDef: xs=1, xs.list=2 (outer d), element=3,
    //   a=4, bs=4, bs.list=5, bs inner struct element=6, x=7.
    //   Row0 empty (proves rowStarts alignment); Row1: [ {a:1, bs:[{x:10},{x:20}]}, {a:2, bs:[]},
    //                                                     {a:3, bs:null}, {a:4, bs:[{x:30}]} ]
    @Test
    void structElementsInsideNestedListField() {
        ParquetSchema schema = listOfStructWithListOfStructSchema();
        ColumnPath aPath = ColumnPath.of("xs", "list", "element", "a");
        ColumnPath xPath = ColumnPath.of("xs", "list", "element", "bs", "list", "element", "x");

        // leaf a: one entry per outer struct element; row0's empty outer list is a phantom at def 1.
        int[] aVals = {0, 1, 2, 3, 4};
        int[] aRep = {0, 0, 1, 1, 1};
        int[] aDef = {1, 4, 4, 4, 4};
        int[] aRowStarts = {0, 1, 5};
        IntVector aLeaf = IntVector.materialized(aVals, validityWhere(aDef, 4));

        // leaf x: el0 bs=[{x:10},{x:20}] -> def 7, second entry rep 2; el1 bs=[] -> def 4 (bs present, empty);
        //         el2 bs=null -> def 3 (element present, bs absent); el3 bs=[{x:30}] -> def 7.
        int[] xVals = {0, 10, 20, 0, 0, 30};
        int[] xRep = {0, 0, 2, 1, 1, 1};
        int[] xDef = {1, 7, 7, 4, 3, 7};
        int[] xRowStarts = {0, 1, 6};
        IntVector xLeaf = IntVector.materialized(xVals, validityWhere(xDef, 7));

        LevelListVector vec = LevelListVector.of(
                schema,
                ColumnPath.of("xs"),
                Map.of(aPath, aLeaf, xPath, xLeaf),
                Map.of(aPath, leafLevels(aRep, aDef, aRowStarts), xPath, leafLevels(xRep, xDef, xRowStarts)),
                rowValidity(new boolean[] {true, true}),
                2);

        assertThat(LevelListMaterializer.materializeAt(vec, 0)).isEqualTo(List.of());

        List<?> row1 = LevelListMaterializer.materializeAt(vec, 1);
        assertThat(row1).hasSize(4);

        ParquetRecord e0 = (ParquetRecord) row1.get(0);
        assertThat(e0.getInt(0)).isEqualTo(1);
        List<?> bs0 = e0.readList(ColumnPath.of("bs"));
        assertThat(bs0).hasSize(2);
        assertThat(((ParquetRecord) bs0.get(0)).getInt(0)).isEqualTo(10);
        assertThat(((ParquetRecord) bs0.get(1)).getInt(ColumnPath.of("x"))).isEqualTo(20);

        ParquetRecord e1 = (ParquetRecord) row1.get(1);
        assertThat(e1.getInt(0)).isEqualTo(2);
        assertThat(e1.readList(ColumnPath.of("bs"))).isEqualTo(List.of());

        ParquetRecord e2 = (ParquetRecord) row1.get(2);
        assertThat(e2.getInt(0)).isEqualTo(3);
        assertThat(e2.readList(ColumnPath.of("bs"))).isNull();

        ParquetRecord e3 = (ParquetRecord) row1.get(3);
        assertThat(e3.getInt(0)).isEqualTo(4);
        List<?> bs3 = e3.readList(ColumnPath.of("bs"));
        assertThat(bs3).hasSize(1);
        assertThat(((ParquetRecord) bs3.get(0)).getInt(0)).isEqualTo(30);
    }

    // struct elements of a nested list: list<list<struct<x: int>>>.
    //   optional group xs (LIST) { repeated group list { optional group element (LIST) {
    //       repeated group list { optional group element { optional int32 x } } } } }
    //   maxRep(outer list)=1, maxRep(inner list)=2; maxDef: xs=1, xs.list=2, outer element (inner list group)=3,
    //   inner list=4, inner struct element=5, x=6.
    //   Row0: [ [], [{x:7},{x:8}], null, [{x:9}] ] - the inner structs read after the phantom empty inner list.
    @Test
    void structElementsOfNestedList() {
        ParquetSchema schema = listOfListOfStructSchema();
        ColumnPath xPath = ColumnPath.of("xs", "list", "element", "list", "element", "x");

        // inner []: phantom(r0, d3) - outer element present, inner container empty;
        // inner null: phantom(r1, d2) - outer slot present, inner container absent.
        int[] xVals = {0, 7, 8, 0, 9};
        int[] xRep = {0, 1, 2, 1, 1};
        int[] xDef = {3, 6, 6, 2, 6};
        int[] rowStarts = {0, 5};
        IntVector xLeaf = IntVector.materialized(xVals, validityWhere(xDef, 6));

        LevelListVector vec = LevelListVector.of(
                schema,
                ColumnPath.of("xs"),
                Map.of(xPath, xLeaf),
                Map.of(xPath, leafLevels(xRep, xDef, rowStarts)),
                rowValidity(new boolean[] {true}),
                1);

        List<?> row0 = LevelListMaterializer.materializeAt(vec, 0);
        assertThat(row0).hasSize(4);
        assertThat(row0.get(0)).isEqualTo(List.of());

        List<?> inner1 = (List<?>) row0.get(1);
        assertThat(inner1).hasSize(2);
        assertThat(((ParquetRecord) inner1.get(0)).getInt(0)).isEqualTo(7);
        assertThat(((ParquetRecord) inner1.get(1)).getInt(ColumnPath.of("x"))).isEqualTo(8);

        assertThat(row0.get(2)).isNull();

        List<?> inner3 = (List<?>) row0.get(3);
        assertThat(inner3).hasSize(1);
        assertThat(((ParquetRecord) inner3.get(0)).getInt(0)).isEqualTo(9);
    }

    // --- fixtures ---

    private static LeafLevels leafLevels(int[] rep, int[] def, int[] rowStarts) {
        return new LeafLevels(Levels.of(rep), Levels.of(def), IntSequence.of(rowStarts));
    }

    private static Validity validityWhere(int[] def, int presentLevel) {
        BitSet bits = new BitSet(def.length);
        for (int i = 0; i < def.length; i++) {
            if (def[i] == presentLevel) {
                bits.set(i);
            }
        }
        return Validity.of(bits, def.length);
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

    // Empty Variant metadata: header byte 0x01 (version 1, offset size 1), dictionary size 0, one offset entry.
    private static MemorySegment emptyVariantMetadata() {
        return MemorySegment.ofArray(new byte[] {0x01, 0x00, 0x00});
    }

    // Variant primitive int32: basic type 0 (primitive), type id 5 (int32) -> header (5 << 2) = 0x14, then 4 LE bytes.
    private static MemorySegment variantInt32(int value) {
        return MemorySegment.ofArray(
                new byte[] {0x14, (byte) value, (byte) (value >> 8), (byte) (value >> 16), (byte) (value >> 24)});
    }

    private static ParquetSchema listOfStructAbSchema() {
        SchemaNode.Primitive a = primitive("a", Repetition.OPTIONAL, PrimitiveKind.DOUBLE);
        SchemaNode.Primitive b = stringPrimitive("b", Repetition.OPTIONAL);
        SchemaNode.Group element = structGroup("element", List.of(a, b));
        SchemaNode.Group middle = repeatedGroup("list", List.of(element));
        SchemaNode.Group listGroup = listGroup("xs", Repetition.OPTIONAL, List.of(middle));
        return rootSchema(listGroup);
    }

    private static ParquetSchema listOfStructWithInnerListSchema() {
        SchemaNode.Primitive a = primitive("a", Repetition.OPTIONAL, PrimitiveKind.INT32);
        SchemaNode.Primitive innerElement = primitive("element", Repetition.OPTIONAL, PrimitiveKind.DOUBLE);
        SchemaNode.Group innerMiddle = repeatedGroup("list", List.of(innerElement));
        SchemaNode.Group bs = listGroup("bs", Repetition.OPTIONAL, List.of(innerMiddle));
        SchemaNode.Group element = structGroup("element", List.of(a, bs));
        SchemaNode.Group middle = repeatedGroup("list", List.of(element));
        SchemaNode.Group listGroup = listGroup("xs", Repetition.OPTIONAL, List.of(middle));
        return rootSchema(listGroup);
    }

    private static ParquetSchema listOfStructWithListOfStructSchema() {
        SchemaNode.Primitive a = primitive("a", Repetition.OPTIONAL, PrimitiveKind.INT32);
        SchemaNode.Primitive x = primitive("x", Repetition.OPTIONAL, PrimitiveKind.INT32);
        SchemaNode.Group innerElement = structGroup("element", List.of(x));
        SchemaNode.Group innerMiddle = repeatedGroup("list", List.of(innerElement));
        SchemaNode.Group bs = listGroup("bs", Repetition.OPTIONAL, List.of(innerMiddle));
        SchemaNode.Group element = structGroup("element", List.of(a, bs));
        SchemaNode.Group middle = repeatedGroup("list", List.of(element));
        SchemaNode.Group listGroup = listGroup("xs", Repetition.OPTIONAL, List.of(middle));
        return rootSchema(listGroup);
    }

    private static ParquetSchema listOfListOfStructSchema() {
        SchemaNode.Primitive x = primitive("x", Repetition.OPTIONAL, PrimitiveKind.INT32);
        SchemaNode.Group innerStruct = structGroup("element", List.of(x));
        SchemaNode.Group innerMiddle = repeatedGroup("list", List.of(innerStruct));
        SchemaNode.Group innerList = listGroup("element", Repetition.OPTIONAL, List.of(innerMiddle));
        SchemaNode.Group outerMiddle = repeatedGroup("list", List.of(innerList));
        SchemaNode.Group outerList = listGroup("xs", Repetition.OPTIONAL, List.of(outerMiddle));
        return rootSchema(outerList);
    }

    private static ParquetSchema listOfNestedStructSchema() {
        SchemaNode.Primitive x = primitive("x", Repetition.OPTIONAL, PrimitiveKind.INT32);
        SchemaNode.Group inner = structGroup("inner", List.of(x));
        SchemaNode.Group element = structGroup("element", List.of(inner));
        SchemaNode.Group middle = repeatedGroup("list", List.of(element));
        SchemaNode.Group listGroup = listGroup("xs", Repetition.OPTIONAL, List.of(middle));
        return rootSchema(listGroup);
    }

    private static ParquetSchema listOfVariantSchema() {
        SchemaNode.Primitive metadata = primitive("metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY);
        SchemaNode.Primitive value = primitive("value", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY);
        SchemaNode.Group element = variantGroup("element", List.of(metadata, value));
        SchemaNode.Group middle = repeatedGroup("list", List.of(element));
        SchemaNode.Group listGroup = listGroup("xs", Repetition.OPTIONAL, List.of(middle));
        return rootSchema(listGroup);
    }

    private static ParquetSchema listOfStructWithMapSchema() {
        SchemaNode.Primitive key = primitive("key", Repetition.REQUIRED, PrimitiveKind.INT32);
        SchemaNode.Primitive value = primitive("value", Repetition.OPTIONAL, PrimitiveKind.INT32);
        SchemaNode.Group keyValue = repeatedGroup("key_value", List.of(key, value));
        SchemaNode.Group map = mapGroup("m", List.of(keyValue));
        SchemaNode.Group element = structGroup("element", List.of(map));
        SchemaNode.Group middle = repeatedGroup("list", List.of(element));
        SchemaNode.Group listGroup = listGroup("xs", Repetition.OPTIONAL, List.of(middle));
        return rootSchema(listGroup);
    }

    private static SchemaNode.Primitive primitive(String name, Repetition repetition, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, repetition, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive stringPrimitive(String name, Repetition repetition) {
        return new SchemaNode.Primitive(
                name,
                repetition,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
    }

    private static SchemaNode.Group structGroup(String name, List<SchemaNode> children) {
        return new SchemaNode.Group(name, Repetition.OPTIONAL, children, Optional.empty(), -1);
    }

    private static SchemaNode.Group repeatedGroup(String name, List<SchemaNode> children) {
        return new SchemaNode.Group(name, Repetition.REPEATED, children, Optional.empty(), -1);
    }

    private static SchemaNode.Group listGroup(String name, Repetition repetition, List<SchemaNode> children) {
        return new SchemaNode.Group(name, repetition, children, Optional.of(new LogicalType.ListType()), -1);
    }

    private static SchemaNode.Group mapGroup(String name, List<SchemaNode> children) {
        return new SchemaNode.Group(name, Repetition.OPTIONAL, children, Optional.of(new LogicalType.MapType()), -1);
    }

    private static SchemaNode.Group variantGroup(String name, List<SchemaNode> children) {
        return new SchemaNode.Group(name, Repetition.OPTIONAL, children, Optional.of(new LogicalType.Variant()), -1);
    }

    private static ParquetSchema rootSchema(SchemaNode.Group field) {
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(field), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
