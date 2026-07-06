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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.LevelListVector;
import io.tileverse.parquetry.columnar.LevelMapVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.variant.Variant;

/**
 * Parity oracle for {@link LevelVectorAssembler}. For each nested shape the same handcrafted inputs (schema, dense leaf
 * vectors, whole-stream rep/def streams) feed both the eager {@link NestedVectorAssembler} and the level-form
 * {@link LevelVectorAssembler}; the two are then compared row by row through the record API. The level vectors' Arrow
 * form is compared to the eager vectors value-for-value, and the per-batch level buffer is asserted to release on
 * close.
 */
// S125: the schema and level-stream derivations are documentation, not code
@SuppressWarnings("java:S125")
class LevelVectorAssemblerTest {

    // flat leaf side by side with list<double>: the flat leaf passes through; the list's descendant is hidden.
    //   required int32 id; optional group xs (LIST) { repeated group list { optional double element } }
    @Test
    void flatLeafBesideListOfDouble() {
        SchemaNode.Primitive id = primitive("id", Repetition.REQUIRED, PrimitiveKind.INT32);
        ParquetSchema schema = rootSchema(id, optionalListOf(primitive("element", Repetition.OPTIONAL, DOUBLE)));

        ColumnPath idPath = ColumnPath.of("id");
        ColumnPath elem = ColumnPath.of("xs", "list", "element");

        Map<ColumnPath, ColumnVector> leaves = new LinkedHashMap<>();
        leaves.put(idPath, IntVector.materialized(new int[] {10, 20, 30}, Validity.allValid(3)));
        // row0 [1.0,2.0], row1 [], row2 null
        double[] vals = {1.0, 2.0, 0.0, 0.0};
        leaves.put(elem, doubles(vals, new int[] {3, 3, 1, 0}, 3));

        Map<ColumnPath, int[]> rep = Map.of(elem, new int[] {0, 1, 0, 0});
        Map<ColumnPath, int[]> def = Map.of(elem, new int[] {3, 3, 1, 0});

        assertParity(schema, leaves, rep, def, 3, List.of(idPath, ColumnPath.of("xs")));
    }

    // map<string,int> with an empty-map row, a null-map row, and a null value.
    //   optional group m (MAP) { repeated group key_value { required binary key (STRING); optional int32 value } }
    //   maxDef(m)=1, maxDef(key_value)=2 (d), maxDef(key)=2, maxDef(value)=3; maxRep(key_value)=1.
    //   row0 {"a":1,"b":null}; row1 {} (empty); row2 null.
    @Test
    void mapStringIntWithEmptyNullAndNullValue() {
        ParquetSchema schema = rootSchema(mapStringInt("m"));
        ColumnPath keyPath = ColumnPath.of("m", "key_value", "key");
        ColumnPath valPath = ColumnPath.of("m", "key_value", "value");

        MemorySegment[] keys = {utf8("a"), utf8("b"), null, null};
        int[] keyRep = {0, 1, 0, 0};
        int[] keyDef = {2, 2, 1, 0};

        int[] vals = {1, 0, 0, 0};
        int[] valRep = {0, 1, 0, 0};
        int[] valDef = {3, 2, 1, 0};

        Map<ColumnPath, ColumnVector> leaves = new LinkedHashMap<>();
        leaves.put(keyPath, BinaryVector.materialized(keys, validityWhere(keyDef, 2)));
        leaves.put(valPath, IntVector.materialized(vals, validityWhere(valDef, 3)));

        Map<ColumnPath, int[]> rep = Map.of(keyPath, keyRep, valPath, valRep);
        Map<ColumnPath, int[]> def = Map.of(keyPath, keyDef, valPath, valDef);

        assertParity(schema, leaves, rep, def, 3, List.of(ColumnPath.of("m")));
    }

    // The record API reads a level-backed map through readMap/get and reports a MAP kind on a primitive accessor.
    @Test
    void recordApiReadsLevelMapAndReportsKind() {
        ParquetSchema schema = rootSchema(mapStringInt("m"));
        ColumnPath mapPath = ColumnPath.of("m");
        ColumnPath keyPath = ColumnPath.of("m", "key_value", "key");
        ColumnPath valPath = ColumnPath.of("m", "key_value", "value");

        Map<ColumnPath, ColumnVector> leaves = new LinkedHashMap<>();
        leaves.put(
                keyPath, BinaryVector.materialized(new MemorySegment[] {utf8("a")}, validityWhere(new int[] {2}, 2)));
        leaves.put(valPath, IntVector.materialized(new int[] {1}, validityWhere(new int[] {3}, 3)));
        Map<ColumnPath, LevelSlice> rep = wholeSlices(Map.of(keyPath, new int[] {0}, valPath, new int[] {0}));
        Map<ColumnPath, LevelSlice> def = wholeSlices(Map.of(keyPath, new int[] {2}, valPath, new int[] {3}));

        List<AutoCloseable> acquired = new ArrayList<>();
        Map<ColumnPath, ColumnVector> level = LevelVectorAssembler.assembleLevelForm(
                schema, leaves, rep, def, 1, TestDecodeBuffers.ample(), acquired);
        assertThat(level.get(mapPath)).isInstanceOf(LevelMapVector.class);

        DefaultParquetRecordBatch batch = DefaultParquetRecordBatch.ofHeap(schema, level, 1);
        try {
            ParquetRecord row0 = batch.materialize(0);
            Map<?, ?> map = row0.readMap(mapPath);
            assertThat(map).hasSize(1);
            Map.Entry<?, ?> entry = map.entrySet().iterator().next();
            assertThat(bytes((MemorySegment) entry.getKey())).isEqualTo("a".getBytes(StandardCharsets.UTF_8));
            assertThat(entry.getValue()).isEqualTo(1);
            assertThat(row0.get(mapPath)).isInstanceOf(Map.class);
            assertThatThrownBy(() -> row0.getInt(mapPath)).hasMessageContaining("is MAP");
        } finally {
            batch.close();
            closeAll(acquired);
        }
    }

    // list<struct<a: double, b: string>>.
    @Test
    void listOfStruct() {
        ParquetSchema schema = rootSchema(listOfStructAb("xs"));
        ColumnPath aPath = ColumnPath.of("xs", "list", "element", "a");
        ColumnPath bPath = ColumnPath.of("xs", "list", "element", "b");

        // row0 [{1.0,"x"},{null,"y"}]; row1 [null_struct]; row2 [].
        double[] aVals = {1.0, 0.0, 0.0, 0.0};
        int[] aRep = {0, 1, 0, 0};
        int[] aDef = {4, 3, 2, 1};

        MemorySegment[] bVals = {utf8("x"), utf8("y"), null, null};
        int[] bRep = {0, 1, 0, 0};
        int[] bDef = {4, 4, 2, 1};

        Map<ColumnPath, ColumnVector> leaves = new LinkedHashMap<>();
        leaves.put(aPath, doubles(aVals, aDef, 4));
        leaves.put(bPath, BinaryVector.materialized(bVals, validityWhere(bDef, 4)));

        Map<ColumnPath, int[]> rep = Map.of(aPath, aRep, bPath, bRep);
        Map<ColumnPath, int[]> def = Map.of(aPath, aDef, bPath, bDef);

        assertParity(schema, leaves, rep, def, 3, List.of(ColumnPath.of("xs")));
    }

    // struct{flat, list<int>}: a struct wrapper with an addressable flat leaf and a level-list child.
    //   optional group s { optional int32 f; optional group xs (LIST) { repeated group list { optional int32 element }
    // } }
    //   maxDef(s)=1, maxDef(f)=2, maxDef(xs)=2, maxDef(xs.list)=3, maxDef(element)=4; maxRep(xs.list)=1.
    @Test
    void structWithFlatAndListOfInt() {
        ParquetSchema schema = rootSchema(structFlatAndList("s"));
        ColumnPath fPath = ColumnPath.of("s", "f");
        ColumnPath elem = ColumnPath.of("s", "xs", "list", "element");

        // row0 {f:7, xs:[1,2]}; row1 {f:null, xs:[]}; row2 null struct.
        Map<ColumnPath, ColumnVector> leaves = new LinkedHashMap<>();
        leaves.put(fPath, ints(new int[] {7, 0, 0}, new int[] {2, 1, 0}, 2));
        leaves.put(elem, ints(new int[] {1, 2, 0, 0}, new int[] {4, 4, 2, 0}, 4));

        Map<ColumnPath, int[]> rep = new LinkedHashMap<>();
        rep.put(fPath, new int[] {0, 0, 0});
        rep.put(elem, new int[] {0, 1, 0, 0});
        Map<ColumnPath, int[]> def = new LinkedHashMap<>();
        def.put(fPath, new int[] {2, 1, 0});
        def.put(elem, new int[] {4, 4, 2, 0});

        assertParity(schema, leaves, rep, def, 3, List.of(ColumnPath.of("s")));
    }

    // list<list<int>>.
    @Test
    void listOfListOfInt() {
        ParquetSchema schema = rootSchema(listOfListOfInt("xs"));
        ColumnPath elem = ColumnPath.of("xs", "list", "element", "list", "element");

        // row0 [[1,2],[],null]; row1 []; row2 null.
        int[] vals = {1, 2, 0, 0, 0, 0};
        int[] rep = {0, 2, 1, 1, 0, 0};
        int[] def = {5, 5, 3, 2, 1, 0};

        Map<ColumnPath, ColumnVector> leaves = Map.of(elem, ints(vals, def, 5));
        assertParity(schema, leaves, Map.of(elem, rep), Map.of(elem, def), 3, List.of(ColumnPath.of("xs")));
    }

    // the nested_lists.snappy.parquet shape: an UNANNOTATED optional group over a chain of repeated groups.
    //   optional group a { repeated group list { optional group element {
    //       repeated group list { optional group element { repeated group list { optional int32 element } } } } } }
    //   def: a=1, a.list=2, .element=3, .list=4, .element=5, .list=6, leaf element=7; rep: 1/2/3 at the list groups.
    //
    //   Without a LIST annotation, "a" reads as a STRUCT and the REPEATED group a.list is the list. Its nested-list
    //   element meta resolves to the NEXT repeated group down (a.list.element.list): child.groupDefLevel=4 (that
    //   group's own max def) and child.elementDefLevel=5 (its element wrapper). Row 0's entry stream is
    //   (r0,d7) (r3,d7) (r2,d7) (r1,d4) (r2,d7) - the corpus file's real levels for its row 0, ints in place of
    //   strings. The (r1,d4) entry opens the second outer element with d4 inside the [4,5) gap: the inner container is
    //   present (d4 >= 4) but the opening entry holds no element (d4 < 5), while the NEXT entry (r2,d7) is a real
    //   element of that same inner list. Deciding emptiness from the opener's def alone dropped that trailing element
    //   ([] instead of [[4]]). The annotated three-level shape cannot reproduce this: there the gap collapses
    //   (groupDef=3, elemDef=4) and a below-elemDef opener genuinely terminates the list.
    //   StreamingLevelsParityIT pins the same defect against the corpus file end to end.
    @Test
    void structuralThreeDeepListKeepsElementsAfterPhantomOpener() {
        ParquetSchema schema = rootSchema(structuralListOfListOfListOfInt("a"));
        ColumnPath elem = ColumnPath.of("a", "list", "element", "list", "element", "list", "element");

        // row0: outer element 0 = [[1,2],[3]]; outer element 1 opens with the d4 phantom and continues to [[4]].
        int[] vals = {1, 2, 3, 0, 4};
        int[] rep = {0, 3, 2, 1, 2};
        int[] def = {7, 7, 7, 4, 7};

        Map<ColumnPath, ColumnVector> leaves = Map.of(elem, ints(vals, def, 7));
        Map<ColumnPath, int[]> repMap = Map.of(elem, rep);
        Map<ColumnPath, int[]> defMap = Map.of(elem, def);
        assertParity(schema, leaves, repMap, defMap, 1, List.of(ColumnPath.of("a")));
    }

    // the same def-gap defect at a list's TOP row window, the site materializeAt decides emptiness from.
    //   optional group a (struct) { repeated group list { optional group element { optional int32 v } } }
    //   def: a=1, a.list=2, .element=3, v=4; rep: a.list=1. The LevelListVector lives at a.list: groupDef=2,
    //   elementDef=3 (the struct element wrapper adds a def level below the repeated child, opening the [2,3) gap).
    //
    //   Row 0's outer list is [null_struct, {v:7}]. The first entry opens with d2 (a.list present, element null), a
    //   phantom in the gap; the next entry (r1,d4) is a real element of the SAME row's list. Deciding emptiness from
    //   that opener's def alone dropped the trailing element ([] instead of [null, {v:7}]).
    @Test
    void structuralListKeepsElementsAfterPhantomRowOpener() {
        ParquetSchema schema = rootSchema(structuralListOfStruct("a"));
        ColumnPath v = ColumnPath.of("a", "list", "element", "v");

        // row0: [null_struct (r0,d2), {v:7} (r1,d4)].
        int[] vals = {0, 7};
        int[] rep = {0, 1};
        int[] def = {2, 4};

        Map<ColumnPath, ColumnVector> leaves = Map.of(v, ints(vals, def, 4));
        Map<ColumnPath, int[]> repMap = Map.of(v, rep);
        Map<ColumnPath, int[]> defMap = Map.of(v, def);
        assertParity(schema, leaves, repMap, defMap, 1, List.of(ColumnPath.of("a")));
    }

    // the same def-gap defect at a struct field's nested-list window, the site LevelElementRecord.listField
    //   decides emptiness from.
    //   optional group xs (LIST) { repeated group list { optional group element {
    //       optional int32 a; repeated group bs { optional group element { optional int32 v } } } } }
    //   def: xs=1, xs.list=2, element=3, a=4, bs=4, bs.element=5, v=6; rep: xs.list=1, bs=2. The bs field is a
    //   structural (repeated, unannotated) list: bs.groupDef=4, bs.elementDef=5, opening the [4,5) gap below it.
    //
    //   The single outer element's bs is [null_struct, {v:9}]. Its window opens with d4 (bs present, bs.element null),
    //   a phantom in the gap, then continues to the real {v:9} (r2,d6). Deciding emptiness from the opener's def alone
    //   dropped the trailing element (bs read as [] instead of [null, {v:9}]).
    @Test
    void structFieldNestedListKeepsElementsAfterPhantomOpener() {
        ParquetSchema schema = rootSchema(listOfStructWithStructuralListField("xs"));
        ColumnPath aPath = ColumnPath.of("xs", "list", "element", "a");
        ColumnPath vPath = ColumnPath.of("xs", "list", "element", "bs", "element", "v");

        // outer element a=5; bs=[null_struct (d4), {v:9} (r2,d6)].
        int[] aVals = {5};
        int[] aRep = {0};
        int[] aDef = {4};

        int[] vVals = {0, 9};
        int[] vRep = {0, 2};
        int[] vDef = {4, 6};

        Map<ColumnPath, ColumnVector> leaves = new LinkedHashMap<>();
        leaves.put(aPath, ints(aVals, aDef, 4));
        leaves.put(vPath, ints(vVals, vDef, 6));

        Map<ColumnPath, int[]> rep = Map.of(aPath, aRep, vPath, vRep);
        Map<ColumnPath, int[]> def = Map.of(aPath, aDef, vPath, vDef);
        assertParity(schema, leaves, rep, def, 1, List.of(ColumnPath.of("xs")));
    }

    // the multi-leaf cursor steps over phantom openers among real struct openers in EVERY descendant leaf.
    //   optional group a (struct) { repeated group list { optional group element {
    //       optional int32 v; optional int32 w } } }
    //   def: a=1, a.list=2, .element=3, v=w=4; rep: a.list=1. Both leaves share the row's openers.
    //
    //   Row 0 = [null_struct, {v:7,w:8}, null_struct, {v:9,w:10}]. The phantom struct openers (d2) interleave with the
    //   real ones (d4) in both v and w. The cursor must align element ordinals to the real openers (entries 1 and 3),
    //   not to every opener, or the second real struct would read the wrong leaf positions.
    @Test
    void multiLeafCursorSkipsPhantomOpenersInEveryLeaf() {
        ParquetSchema schema = rootSchema(structuralListOfStructVW("a"));
        ColumnPath v = ColumnPath.of("a", "list", "element", "v");
        ColumnPath w = ColumnPath.of("a", "list", "element", "w");

        int[] vVals = {0, 7, 0, 9};
        int[] wVals = {0, 8, 0, 10};
        int[] rep = {0, 1, 1, 1};
        int[] def = {2, 4, 2, 4};

        Map<ColumnPath, ColumnVector> leaves = new LinkedHashMap<>();
        leaves.put(v, ints(vVals, def, 4));
        leaves.put(w, ints(wVals, def, 4));

        Map<ColumnPath, int[]> repMap = Map.of(v, rep, w, rep);
        Map<ColumnPath, int[]> defMap = Map.of(v, def, w, def);
        assertParity(schema, leaves, repMap, defMap, 1, List.of(ColumnPath.of("a")));
    }

    // map<string, list<int>>.
    //   optional group m (MAP) { repeated group key_value {
    //       required binary key (STRING); optional group value (LIST) { repeated group list { optional int32 element }
    // } } }
    //   maxDef(m)=1, maxDef(key_value)=2 (d), maxDef(key)=2, maxDef(value)=3, maxDef(value.list)=4, maxDef(element)=5;
    //   maxRep(key_value)=1, maxRep(value.list)=2.
    //   row0 {"a":[1,2], "b":[]}; row1 null.
    @Test
    void mapStringListOfInt() {
        ParquetSchema schema = rootSchema(mapStringListOfInt("m"));
        ColumnPath keyPath = ColumnPath.of("m", "key_value", "key");
        ColumnPath elem = ColumnPath.of("m", "key_value", "value", "list", "element");

        MemorySegment[] keys = {utf8("a"), utf8("b"), null};
        int[] keyRep = {0, 1, 0};
        int[] keyDef = {2, 2, 0};

        // "a"->[1,2]: 1(r0,d5) 2(r2,d5); "b"->[] phantom(r1,d3); row1 null map phantom(r0,d0).
        int[] vals = {1, 2, 0, 0};
        int[] valRep = {0, 2, 1, 0};
        int[] valDef = {5, 5, 3, 0};

        Map<ColumnPath, ColumnVector> leaves = new LinkedHashMap<>();
        leaves.put(keyPath, BinaryVector.materialized(keys, validityWhere(keyDef, 2)));
        leaves.put(elem, ints(vals, valDef, 5));

        Map<ColumnPath, int[]> rep = Map.of(keyPath, keyRep, elem, valRep);
        Map<ColumnPath, int[]> def = Map.of(keyPath, keyDef, elem, valDef);

        assertParity(schema, leaves, rep, def, 2, List.of(ColumnPath.of("m")));
    }

    // map<string, struct<x, y>>.
    //   optional group m (MAP) { repeated group key_value {
    //       required binary key (STRING); optional group value { optional int32 x; optional int32 y } } }
    //   maxDef(m)=1, maxDef(key_value)=2 (d), maxDef(key)=2, maxDef(value)=3, maxDef(x)=maxDef(y)=4; maxRep=1.
    //   row0 {"a":{1,2}, "b":null_value}; row1 {} empty.
    @Test
    void mapStringStructXy() {
        ParquetSchema schema = rootSchema(mapStringStructXy("m"));
        ColumnPath keyPath = ColumnPath.of("m", "key_value", "key");
        ColumnPath xPath = ColumnPath.of("m", "key_value", "value", "x");
        ColumnPath yPath = ColumnPath.of("m", "key_value", "value", "y");

        MemorySegment[] keys = {utf8("a"), utf8("b"), null};
        int[] keyRep = {0, 1, 0};
        int[] keyDef = {2, 2, 1};

        int[] xVals = {1, 0, 0};
        int[] xRep = {0, 1, 0};
        int[] xDef = {4, 3, 1};

        int[] yVals = {2, 0, 0};
        int[] yRep = {0, 1, 0};
        int[] yDef = {4, 3, 1};

        Map<ColumnPath, ColumnVector> leaves = new LinkedHashMap<>();
        leaves.put(keyPath, BinaryVector.materialized(keys, validityWhere(keyDef, 2)));
        leaves.put(xPath, ints(xVals, xDef, 4));
        leaves.put(yPath, ints(yVals, yDef, 4));

        Map<ColumnPath, int[]> rep = Map.of(keyPath, keyRep, xPath, xRep, yPath, yRep);
        Map<ColumnPath, int[]> def = Map.of(keyPath, keyDef, xPath, xDef, yPath, yDef);

        assertParity(schema, leaves, rep, def, 2, List.of(ColumnPath.of("m")));
    }

    // list<map<string,int>>.
    //   optional group xs (LIST) { repeated group list { optional group element (MAP) {
    //       repeated group key_value { required binary key (STRING); optional int32 value } } } }
    //   maxDef(xs)=1, maxDef(xs.list)=2 (outer d), maxDef(element/map)=3, maxDef(key_value)=4, maxDef(key)=4,
    //   maxDef(value)=5; maxRep(xs.list)=1, maxRep(key_value)=2.
    //   row0 [ {"a":1}, {} (empty map), null_map ]; row1 null.
    @Test
    void listOfMapStringInt() {
        ParquetSchema schema = rootSchema(listOfMapStringInt("xs"));
        ColumnPath keyPath = ColumnPath.of("xs", "list", "element", "key_value", "key");
        ColumnPath valPath = ColumnPath.of("xs", "list", "element", "key_value", "value");

        // el0 {"a":1}: key a(r0,d4) value 1(r0,d5); el1 {} empty map phantom(r1,d3);
        // el2 null map phantom(r1,d2); row1 null outer phantom(r0,d0).
        MemorySegment[] keys = {utf8("a"), null, null, null};
        int[] keyRep = {0, 1, 1, 0};
        int[] keyDef = {4, 3, 2, 0};

        int[] vals = {1, 0, 0, 0};
        int[] valRep = {0, 1, 1, 0};
        int[] valDef = {5, 3, 2, 0};

        Map<ColumnPath, ColumnVector> leaves = new LinkedHashMap<>();
        leaves.put(keyPath, BinaryVector.materialized(keys, validityWhere(keyDef, 4)));
        leaves.put(valPath, ints(vals, valDef, 5));

        Map<ColumnPath, int[]> rep = Map.of(keyPath, keyRep, valPath, valRep);
        Map<ColumnPath, int[]> def = Map.of(keyPath, keyDef, valPath, valDef);

        assertParity(schema, leaves, rep, def, 2, List.of(ColumnPath.of("xs")));
    }

    // legacy two-level list: the repeated primitive is the element.
    //   optional group xs (LIST) { repeated int32 element } ; maxDef(xs)=1, maxDef(element)=2, maxRep(element)=1.
    @Test
    void legacyTwoLevelList() {
        SchemaNode.Primitive element = primitive("element", Repetition.REPEATED, PrimitiveKind.INT32);
        SchemaNode.Group xs = new SchemaNode.Group(
                "xs", Repetition.OPTIONAL, List.of(element), Optional.of(new LogicalType.ListType()), -1);
        ParquetSchema schema = rootSchema(xs);
        ColumnPath elem = ColumnPath.of("xs", "element");

        int[] vals = {10, 20, 30};
        int[] rep = {0, 1, 0};
        int[] def = {2, 2, 2};

        Map<ColumnPath, ColumnVector> leaves = Map.of(elem, ints(vals, def, 2));
        assertParity(schema, leaves, Map.of(elem, rep), Map.of(elem, def), 2, List.of(ColumnPath.of("xs")));
    }

    // list<variant> (unshredded): metadata + value binary leaves.
    //   optional group xs (LIST) { repeated group list { optional group element (VARIANT) {
    //       required binary metadata; required binary value } } }
    //   maxDef(xs)=1, maxDef(xs.list)=2 (d), maxDef(element)=3; maxRep(xs.list)=1.
    //   row0 [v(11), v(22)]; row1 null.
    @Test
    void listOfVariant() {
        ParquetSchema schema = rootSchema(listOfVariant("xs"));
        ColumnPath metaPath = ColumnPath.of("xs", "list", "element", "metadata");
        ColumnPath valPath = ColumnPath.of("xs", "list", "element", "value");

        MemorySegment meta = emptyVariantMetadata();
        MemorySegment[] metas = {meta, meta, null};
        MemorySegment[] values = {variantInt32(11), variantInt32(22), null};
        int[] rep = {0, 1, 0};
        int[] def = {3, 3, 0};

        Map<ColumnPath, ColumnVector> leaves = new LinkedHashMap<>();
        leaves.put(metaPath, BinaryVector.materialized(metas, validityWhere(def, 3)));
        leaves.put(valPath, BinaryVector.materialized(values, validityWhere(def, 3)));

        Map<ColumnPath, int[]> repMap = Map.of(metaPath, rep, valPath, rep);
        Map<ColumnPath, int[]> defMap = Map.of(metaPath, def, valPath, def);

        assertParity(schema, leaves, repMap, defMap, 2, List.of(ColumnPath.of("xs")));
    }

    // (iii) the per-batch level buffer releases on close: borrows return to baseline.
    @Test
    void batchLevelsReleaseOnClose() {
        ParquetSchema schema = rootSchema(optionalListOf(primitive("element", Repetition.OPTIONAL, DOUBLE)));
        ColumnPath elem = ColumnPath.of("xs", "list", "element");
        Map<ColumnPath, ColumnVector> leaves = Map.of(elem, doubles(new double[] {1.0, 2.0, 3.0}));
        Map<ColumnPath, LevelSlice> rep = wholeSlices(Map.of(elem, new int[] {0, 1, 0}));
        Map<ColumnPath, LevelSlice> def = wholeSlices(Map.of(elem, new int[] {3, 3, 3}));

        SegmentPool pool = SegmentPool.create();
        DecodeBufferAllocator allocator = TestDecodeBuffers.ample(pool);
        long baseline = pool.stats().outstandingBorrows();

        List<AutoCloseable> acquired = new ArrayList<>();
        LevelVectorAssembler.assembleLevelForm(schema, leaves, rep, def, 2, allocator, acquired);
        assertThat(pool.stats().outstandingBorrows()).isGreaterThan(baseline);

        closeAll(acquired);
        assertThat(pool.stats().outstandingBorrows()).isEqualTo(baseline);
    }

    // --- parity driver ---

    private void assertParity(
            ParquetSchema schema,
            Map<ColumnPath, ColumnVector> leaves,
            Map<ColumnPath, int[]> rep,
            Map<ColumnPath, int[]> def,
            int numRows,
            List<ColumnPath> topLevelPaths) {

        Map<ColumnPath, ColumnVector> eager = NestedVectorAssembler.assembleNested(schema, leaves, rep, def, numRows);

        List<AutoCloseable> acquired = new ArrayList<>();
        Map<ColumnPath, ColumnVector> level = LevelVectorAssembler.assembleLevelForm(
                schema, leaves, wholeSlices(rep), wholeSlices(def), numRows, TestDecodeBuffers.ample(), acquired);
        try {
            assertRecordParity(schema, eager, level, numRows, topLevelPaths);
            assertArrowFormParity(schema, eager, level, numRows, topLevelPaths);
        } finally {
            closeAll(acquired);
        }
    }

    private void assertRecordParity(
            ParquetSchema schema,
            Map<ColumnPath, ColumnVector> eager,
            Map<ColumnPath, ColumnVector> level,
            int numRows,
            List<ColumnPath> paths) {
        DefaultParquetRecordBatch eagerBatch = DefaultParquetRecordBatch.ofHeap(schema, eager, numRows);
        DefaultParquetRecordBatch levelBatch = DefaultParquetRecordBatch.ofHeap(schema, level, numRows);
        try {
            for (int row = 0; row < numRows; row++) {
                ParquetRecord eagerRow = eagerBatch.materialize(row);
                ParquetRecord levelRow = levelBatch.materialize(row);
                for (ColumnPath path : paths) {
                    assertDeepEqual(eagerRow.get(path), levelRow.get(path));
                }
            }
        } finally {
            eagerBatch.close();
            levelBatch.close();
        }
    }

    /**
     * Compares the level vectors' Arrow form to the eager vectors value-for-value by wrapping each in a record batch
     * and comparing every top-level cell. The Arrow form is the eager-shape vector the level vector spills to.
     */
    private void assertArrowFormParity(
            ParquetSchema schema,
            Map<ColumnPath, ColumnVector> eager,
            Map<ColumnPath, ColumnVector> level,
            int numRows,
            List<ColumnPath> paths) {
        Map<ColumnPath, ColumnVector> arrow = new LinkedHashMap<>(level);
        for (ColumnPath path : paths) {
            ColumnVector vec = level.get(path);
            if (vec instanceof LevelListVector list) {
                arrow.put(path, LevelVectorAssembler.toArrowForm(list));
            } else if (vec instanceof LevelMapVector map) {
                arrow.put(path, LevelVectorAssembler.toArrowForm(map));
            }
        }
        assertRecordParity(schema, eager, arrow, numRows, paths);
    }

    // --- deep comparison through the record API ---

    private void assertDeepEqual(Object eager, Object level) {
        if (eager == null || level == null) {
            assertThat(level).isEqualTo(eager);
            return;
        }
        if (eager instanceof ParquetRecord eagerRecord) {
            assertRecordEqual(eagerRecord, (ParquetRecord) level);
            return;
        }
        if (eager instanceof Map<?, ?> eagerMap) {
            assertMapEqual(eagerMap, (Map<?, ?>) level);
            return;
        }
        if (eager instanceof List<?> eagerList) {
            assertListEqual(eagerList, (List<?>) level);
            return;
        }
        if (eager instanceof MemorySegment eagerSegment) {
            assertThat(bytes((MemorySegment) level)).isEqualTo(bytes(eagerSegment));
            return;
        }
        if (eager instanceof Variant eagerVariant) {
            assertVariantEqual(eagerVariant, (Variant) level);
            return;
        }
        assertThat(level).isEqualTo(eager);
    }

    private void assertVariantEqual(Variant eager, Variant level) {
        assertThat(level.type()).isEqualTo(eager.type());
        assertThat(level.getInt()).isEqualTo(eager.getInt());
    }

    private void assertRecordEqual(ParquetRecord eager, ParquetRecord level) {
        assertThat(level.columnCount()).isEqualTo(eager.columnCount());
        for (int col = 0; col < eager.columnCount(); col++) {
            ColumnPath path = eager.columnPath(col);
            assertThat(level.isNull(col)).isEqualTo(eager.isNull(col));
            assertDeepEqual(eager.get(path), level.get(path));
        }
    }

    private void assertListEqual(List<?> eager, List<?> level) {
        assertThat(level).hasSameSizeAs(eager);
        for (int i = 0; i < eager.size(); i++) {
            assertDeepEqual(eager.get(i), level.get(i));
        }
    }

    private void assertMapEqual(Map<?, ?> eager, Map<?, ?> level) {
        List<?> eagerKeys = new ArrayList<>(eager.keySet());
        List<?> levelKeys = new ArrayList<>(level.keySet());
        assertThat(levelKeys).hasSameSizeAs(eagerKeys);
        for (int i = 0; i < eagerKeys.size(); i++) {
            Object eagerKey = eagerKeys.get(i);
            Object levelKey = levelKeys.get(i);
            assertDeepEqual(eagerKey, levelKey);
            assertDeepEqual(eager.get(eagerKey), level.get(levelKey));
        }
    }

    private static void closeAll(List<AutoCloseable> closeables) {
        for (AutoCloseable closeable : closeables) {
            try {
                closeable.close();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private static Map<ColumnPath, LevelSlice> wholeSlices(Map<ColumnPath, int[]> levelsByLeaf) {
        Map<ColumnPath, LevelSlice> slices = new LinkedHashMap<>();
        for (Map.Entry<ColumnPath, int[]> entry : levelsByLeaf.entrySet()) {
            slices.put(entry.getKey(), LevelSlice.ofWhole(io.tileverse.parquetry.columnar.Levels.of(entry.getValue())));
        }
        return slices;
    }

    // --- leaf builders ---

    private static final PrimitiveKind DOUBLE = PrimitiveKind.DOUBLE;

    private static IntVector ints(int[] values, int[] def, int presentLevel) {
        return IntVector.materialized(values, validityWhere(def, presentLevel));
    }

    private static DoubleVector doubles(double[] values) {
        return DoubleVector.materialized(values, Validity.allValid(values.length));
    }

    private static DoubleVector doubles(double[] values, int[] def, int presentLevel) {
        return DoubleVector.materialized(values, validityWhere(def, presentLevel));
    }

    private static Validity validityWhere(int[] def, int presentLevel) {
        java.util.BitSet bits = new java.util.BitSet(def.length);
        for (int i = 0; i < def.length; i++) {
            if (def[i] == presentLevel) {
                bits.set(i);
            }
        }
        return Validity.of(bits, def.length);
    }

    private static MemorySegment utf8(String s) {
        return MemorySegment.ofArray(s.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] bytes(MemorySegment segment) {
        byte[] out = new byte[(int) segment.byteSize()];
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, 0, out, 0, out.length);
        return out;
    }

    private static MemorySegment emptyVariantMetadata() {
        return MemorySegment.ofArray(new byte[] {0x01, 0x00, 0x00});
    }

    private static MemorySegment variantInt32(int value) {
        return MemorySegment.ofArray(
                new byte[] {0x14, (byte) value, (byte) (value >> 8), (byte) (value >> 16), (byte) (value >> 24)});
    }

    // --- schema builders ---

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

    private static SchemaNode.Group repeatedGroup(String name, List<SchemaNode> children) {
        return new SchemaNode.Group(name, Repetition.REPEATED, children, Optional.empty(), -1);
    }

    private static SchemaNode.Group structGroup(String name, Repetition repetition, List<SchemaNode> children) {
        return new SchemaNode.Group(name, repetition, children, Optional.empty(), -1);
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

    private static SchemaNode.Group optionalListOf(SchemaNode.Primitive element) {
        return listGroup("xs", Repetition.OPTIONAL, List.of(repeatedGroup("list", List.of(element))));
    }

    private static SchemaNode.Group mapStringInt(String name) {
        SchemaNode.Primitive key = stringPrimitive("key", Repetition.REQUIRED);
        SchemaNode.Primitive value = primitive("value", Repetition.OPTIONAL, PrimitiveKind.INT32);
        return mapGroup(name, List.of(repeatedGroup("key_value", List.of(key, value))));
    }

    private static SchemaNode.Group listOfStructAb(String name) {
        SchemaNode.Primitive a = primitive("a", Repetition.OPTIONAL, DOUBLE);
        SchemaNode.Primitive b = stringPrimitive("b", Repetition.OPTIONAL);
        SchemaNode.Group element = structGroup("element", Repetition.OPTIONAL, List.of(a, b));
        return listGroup(name, Repetition.OPTIONAL, List.of(repeatedGroup("list", List.of(element))));
    }

    private static SchemaNode.Group structFlatAndList(String name) {
        SchemaNode.Primitive f = primitive("f", Repetition.OPTIONAL, PrimitiveKind.INT32);
        SchemaNode.Group xs = listGroup(
                "xs",
                Repetition.OPTIONAL,
                List.of(repeatedGroup(
                        "list", List.of(primitive("element", Repetition.OPTIONAL, PrimitiveKind.INT32)))));
        return structGroup(name, Repetition.OPTIONAL, List.of(f, xs));
    }

    private static SchemaNode.Group listOfListOfInt(String name) {
        SchemaNode.Group innerList = listGroup(
                "element",
                Repetition.OPTIONAL,
                List.of(repeatedGroup(
                        "list", List.of(primitive("element", Repetition.OPTIONAL, PrimitiveKind.INT32)))));
        return listGroup(name, Repetition.OPTIONAL, List.of(repeatedGroup("list", List.of(innerList))));
    }

    /** The nested_lists.snappy.parquet shape: every group unannotated, nesting expressed purely by repetition. */
    private static SchemaNode.Group structuralListOfListOfListOfInt(String name) {
        SchemaNode.Group innerList =
                repeatedGroup("list", List.of(primitive("element", Repetition.OPTIONAL, PrimitiveKind.INT32)));
        SchemaNode.Group midElement = structGroup("element", Repetition.OPTIONAL, List.of(innerList));
        SchemaNode.Group midList = repeatedGroup("list", List.of(midElement));
        SchemaNode.Group outerElement = structGroup("element", Repetition.OPTIONAL, List.of(midList));
        SchemaNode.Group outerList = repeatedGroup("list", List.of(outerElement));
        return structGroup(name, Repetition.OPTIONAL, List.of(outerList));
    }

    /**
     * A structural (unannotated) list whose element is a struct wrapper adding a def level below the repeated child.
     */
    private static SchemaNode.Group structuralListOfStruct(String name) {
        SchemaNode.Group element = structGroup(
                "element", Repetition.OPTIONAL, List.of(primitive("v", Repetition.OPTIONAL, PrimitiveKind.INT32)));
        SchemaNode.Group list = repeatedGroup("list", List.of(element));
        return structGroup(name, Repetition.OPTIONAL, List.of(list));
    }

    /** A structural list whose struct element has two leaves, which drives the multi-leaf cursor. */
    private static SchemaNode.Group structuralListOfStructVW(String name) {
        SchemaNode.Group element = structGroup(
                "element",
                Repetition.OPTIONAL,
                List.of(
                        primitive("v", Repetition.OPTIONAL, PrimitiveKind.INT32),
                        primitive("w", Repetition.OPTIONAL, PrimitiveKind.INT32)));
        SchemaNode.Group list = repeatedGroup("list", List.of(element));
        return structGroup(name, Repetition.OPTIONAL, List.of(list));
    }

    /** list<struct<a:int, bs: structural list<struct<v:int>>>>: bs is a repeated (unannotated) group field. */
    private static SchemaNode.Group listOfStructWithStructuralListField(String name) {
        SchemaNode.Primitive a = primitive("a", Repetition.OPTIONAL, PrimitiveKind.INT32);
        SchemaNode.Group bsElement = structGroup(
                "element", Repetition.OPTIONAL, List.of(primitive("v", Repetition.OPTIONAL, PrimitiveKind.INT32)));
        SchemaNode.Group bs = repeatedGroup("bs", List.of(bsElement));
        SchemaNode.Group element = structGroup("element", Repetition.OPTIONAL, List.of(a, bs));
        return listGroup(name, Repetition.OPTIONAL, List.of(repeatedGroup("list", List.of(element))));
    }

    private static SchemaNode.Group mapStringListOfInt(String name) {
        SchemaNode.Primitive key = stringPrimitive("key", Repetition.REQUIRED);
        SchemaNode.Group value = listGroup(
                "value",
                Repetition.OPTIONAL,
                List.of(repeatedGroup(
                        "list", List.of(primitive("element", Repetition.OPTIONAL, PrimitiveKind.INT32)))));
        return mapGroup(name, List.of(repeatedGroup("key_value", List.of(key, value))));
    }

    private static SchemaNode.Group mapStringStructXy(String name) {
        SchemaNode.Primitive key = stringPrimitive("key", Repetition.REQUIRED);
        SchemaNode.Primitive x = primitive("x", Repetition.OPTIONAL, PrimitiveKind.INT32);
        SchemaNode.Primitive y = primitive("y", Repetition.OPTIONAL, PrimitiveKind.INT32);
        SchemaNode.Group value = structGroup("value", Repetition.OPTIONAL, List.of(x, y));
        return mapGroup(name, List.of(repeatedGroup("key_value", List.of(key, value))));
    }

    private static SchemaNode.Group listOfMapStringInt(String name) {
        SchemaNode.Primitive key = stringPrimitive("key", Repetition.REQUIRED);
        SchemaNode.Primitive value = primitive("value", Repetition.OPTIONAL, PrimitiveKind.INT32);
        SchemaNode.Group element = mapGroup("element", List.of(repeatedGroup("key_value", List.of(key, value))));
        return listGroup(name, Repetition.OPTIONAL, List.of(repeatedGroup("list", List.of(element))));
    }

    private static SchemaNode.Group listOfVariant(String name) {
        SchemaNode.Primitive metadata = primitive("metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY);
        SchemaNode.Primitive value = primitive("value", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY);
        SchemaNode.Group element = variantGroup("element", List.of(metadata, value));
        return listGroup(name, Repetition.OPTIONAL, List.of(repeatedGroup("list", List.of(element))));
    }

    private static ParquetSchema rootSchema(SchemaNode... fields) {
        SchemaNode.Group root =
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(fields), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
