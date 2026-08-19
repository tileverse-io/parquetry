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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.MapVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.columnar.VariantVector;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The striper is the write-side inverse of {@code DremelAssembler}: it walks an eager nested batch and a schema and
 * emits, per leaf, the value vector plus its repetition- and definition-level streams. This phase handles flat REQUIRED
 * / OPTIONAL leaves and STRUCT-nested leaves with definition levels only; repetition levels are all zero.
 */
class DremelStriperTest {

    @Test
    void requiredFlatColumnStripesToZeroLevels() {
        ParquetSchema schema = schemaOf(primitive("n", Repetition.REQUIRED));
        IntVector values = IntVector.materialized(new int[] {1, 2, 3}, Validity.allValid(3));
        ParquetRecordBatch batch = heapBatch(schema, Map.of(ColumnPath.of("n"), values), 3);

        List<StripedLeaf> striped = new DremelStriper(schema).stripe(batch);

        assertThat(striped).hasSize(1);
        StripedLeaf leaf = striped.get(0);
        assertThat(leaf.leaf()).isEqualTo(ColumnPath.of("n"));
        assertThat(leaf.maxDefLevel()).isZero();
        assertThat(leaf.maxRepLevel()).isZero();
        assertThat(leaf.defLevels()).containsExactly(0, 0, 0);
        assertThat(leaf.repLevels()).containsExactly(0, 0, 0);
        assertThat(leaf.valueCount()).isEqualTo(3);
    }

    @Test
    void nonRepeatedLeafStoresNoRepStreamButSynthesizesZeros() {
        ParquetSchema schema = schemaOf(primitive("n", Repetition.REQUIRED));
        IntVector values = IntVector.materialized(new int[] {1, 2, 3}, Validity.allValid(3));
        ParquetRecordBatch batch = heapBatch(schema, Map.of(ColumnPath.of("n"), values), 3);

        StripedLeaf leaf = new DremelStriper(schema).stripe(batch).get(0);

        assertThat(leaf.repLevelsRaw()).isNull();
        assertThat(leaf.repLevels()).containsExactly(0, 0, 0);
    }

    @Test
    void optionalFlatColumnThreeRows() {
        ParquetSchema schema = schemaOf(primitive("n", Repetition.OPTIONAL));
        IntVector values = IntVector.materialized(new int[] {10, 0, 30}, validity(true, false, true));
        ParquetRecordBatch batch = heapBatch(schema, Map.of(ColumnPath.of("n"), values), 3);

        List<StripedLeaf> striped = new DremelStriper(schema).stripe(batch);

        StripedLeaf leaf = striped.get(0);
        assertThat(leaf.maxDefLevel()).isEqualTo(1);
        assertThat(leaf.defLevels()).containsExactly(1, 0, 1);
        assertThat(leaf.repLevels()).containsExactly(0, 0, 0);
        assertThat(leaf.valueCount()).isEqualTo(2);
    }

    @Test
    void optionalFieldUnderOptionalStructStripesThreeDefStates() {
        ParquetSchema schema = optionalFieldUnderOptionalStruct();
        ColumnVector field = IntVector.materialized(new int[] {7, 0, 0}, validity(true, false, false));
        StructVector struct = new StructVector(Map.of(ColumnPath.of("f"), field), validity(true, true, false), 3);
        ParquetRecordBatch batch = heapBatch(schema, Map.of(ColumnPath.of("s"), struct), 3);

        StripedLeaf leaf = new DremelStriper(schema).stripe(batch).get(0);

        assertThat(leaf.leaf()).isEqualTo(ColumnPath.of("s", "f"));
        assertThat(leaf.maxDefLevel()).isEqualTo(2);
        assertThat(leaf.defLevels()).containsExactly(2, 1, 0);
        assertThat(leaf.repLevels()).containsExactly(0, 0, 0);
        assertThat(leaf.valueCount()).isEqualTo(1);
    }

    @Test
    void unshreddedVariantStripesMetadataAndValueLeaves() {
        ParquetSchema schema = optionalVariant();
        // row0: present Variant; row1: null Variant.
        BinaryVector metadata =
                BinaryVector.materialized(new MemorySegment[] {utf8("meta"), utf8("")}, validity(true, false));
        BinaryVector value =
                BinaryVector.materialized(new MemorySegment[] {utf8("val"), utf8("")}, validity(true, false));
        VariantVector variant = new VariantVector(metadata, value, validity(true, false), 2);
        ParquetRecordBatch batch = heapBatch(schema, Map.of(ColumnPath.of("v"), variant), 2);

        List<StripedLeaf> striped = new DremelStriper(schema).stripe(batch);

        StripedLeaf metadataLeaf = leafFor(striped, ColumnPath.of("v", "metadata"));
        assertThat(metadataLeaf.maxDefLevel()).isEqualTo(1);
        // metadata is REQUIRED under the OPTIONAL Variant group: present row def 1, null Variant row def 0.
        assertThat(metadataLeaf.defLevels()).containsExactly(1, 0);
        assertThat(metadataLeaf.repLevels()).containsExactly(0, 0);
        assertThat(metadataLeaf.valueCount()).isEqualTo(1);

        StripedLeaf valueLeaf = leafFor(striped, ColumnPath.of("v", "value"));
        assertThat(valueLeaf.maxDefLevel()).isEqualTo(2);
        // value is OPTIONAL under the OPTIONAL Variant group: present row def 2, null Variant row def 0.
        assertThat(valueLeaf.defLevels()).containsExactly(2, 0);
        assertThat(valueLeaf.repLevels()).containsExactly(0, 0);
        assertThat(valueLeaf.valueCount()).isEqualTo(1);
    }

    @Test
    void nestedStructReflectsFirstNullLevel() {
        ParquetSchema schema = optionalFieldUnderTwoOptionalStructs();
        // row0: a present, b present, f present (def 3)
        // row1: a present, b null              (def 1)
        // row2: a null                         (def 0)
        ColumnVector field = IntVector.materialized(new int[] {5, 0, 0}, validity(true, false, false));
        StructVector b = new StructVector(Map.of(ColumnPath.of("f"), field), validity(true, false, false), 3);
        StructVector a = new StructVector(Map.of(ColumnPath.of("b"), b), validity(true, true, false), 3);
        ParquetRecordBatch batch = heapBatch(schema, Map.of(ColumnPath.of("a"), a), 3);

        StripedLeaf leaf = new DremelStriper(schema).stripe(batch).get(0);

        assertThat(leaf.leaf()).isEqualTo(ColumnPath.of("a", "b", "f"));
        assertThat(leaf.maxDefLevel()).isEqualTo(3);
        assertThat(leaf.defLevels()).containsExactly(3, 1, 0);
        assertThat(leaf.repLevels()).containsExactly(0, 0, 0);
        assertThat(leaf.valueCount()).isEqualTo(1);
    }

    @Test
    void requiredUnderOptionalAndOptionalUnderRequiredContributeCorrectDef() {
        ParquetSchema schema = requiredAndOptionalMixSchema();

        // optional group s { required int32 r; }  -> maxDef(s.r) == 1 (only s is optional)
        ColumnVector required = IntVector.materialized(new int[] {1, 2}, Validity.allValid(2));
        StructVector s = new StructVector(Map.of(ColumnPath.of("r"), required), validity(true, false), 2);

        // required group t { optional int32 o; }  -> maxDef(t.o) == 1 (t is required, contributes nothing)
        ColumnVector optional = IntVector.materialized(new int[] {9, 0}, validity(true, false));
        StructVector t = new StructVector(Map.of(ColumnPath.of("o"), optional), Validity.allValid(2), 2);

        ParquetRecordBatch batch = heapBatch(schema, Map.of(ColumnPath.of("s"), s, ColumnPath.of("t"), t), 2);

        List<StripedLeaf> striped = new DremelStriper(schema).stripe(batch);

        StripedLeaf sr = leafFor(striped, ColumnPath.of("s", "r"));
        assertThat(sr.maxDefLevel()).isEqualTo(1);
        // row0: s present, r required-present (def 1); row1: s null (def 0, r not materialized)
        assertThat(sr.defLevels()).containsExactly(1, 0);
        assertThat(sr.valueCount()).isEqualTo(1);

        StripedLeaf to = leafFor(striped, ColumnPath.of("t", "o"));
        assertThat(to.maxDefLevel()).isEqualTo(1);
        // t is always present (required); only o's own optionality drives the def: row0 present (1), row1 null (0)
        assertThat(to.defLevels()).containsExactly(1, 0);
        assertThat(to.valueCount()).isEqualTo(1);
    }

    @Test
    void listOfOptionalIntStripesFourRowStates() {
        ParquetSchema schema = listOfOptionalInt();
        // row0 [1,2]; row1 []; row2 null; row3 [null]
        IntVector elements = IntVector.materialized(new int[] {1, 2, 0}, validity(true, true, false));
        ListVector list = new ListVector(new int[] {0, 2, 2, 2, 3}, elements, validity(true, true, false, true), 4);
        ParquetRecordBatch batch = heapBatch(schema, Map.of(ColumnPath.of("mylist"), list), 4);

        StripedLeaf leaf = new DremelStriper(schema).stripe(batch).get(0);

        assertThat(leaf.leaf()).isEqualTo(ColumnPath.of("mylist", "list", "element"));
        assertThat(leaf.maxRepLevel()).isEqualTo(1);
        assertThat(leaf.maxDefLevel()).isEqualTo(3);
        assertThat(leaf.repLevels()).containsExactly(0, 1, 0, 0, 0);
        assertThat(leaf.defLevels()).containsExactly(3, 3, 1, 0, 2);
        assertThat(leaf.valueCount()).isEqualTo(2);
        assertThat(intValues(leaf)).containsExactly(1, 2);

        assertListRoundTrips(leaf, list, 4, 1, 2);
    }

    @Test
    void listOfListOfIntStripesNestedRepLevels() {
        ParquetSchema schema = listOfListOfInt();
        // row0 [[1,2],[3]]; row1 [[]]; row2 []; row3 null
        IntVector ints = IntVector.materialized(new int[] {1, 2, 3}, Validity.allValid(3));
        ListVector inner = new ListVector(new int[] {0, 2, 3, 3}, ints, validity(true, true, true), 3);
        ListVector outer = new ListVector(new int[] {0, 2, 3, 3, 3}, inner, validity(true, true, true, false), 4);
        ParquetRecordBatch batch = heapBatch(schema, Map.of(ColumnPath.of("outer"), outer), 4);

        StripedLeaf leaf = new DremelStriper(schema).stripe(batch).get(0);

        assertThat(leaf.leaf()).isEqualTo(ColumnPath.of("outer", "list", "inner", "list", "element"));
        assertThat(leaf.maxRepLevel()).isEqualTo(2);
        assertThat(leaf.maxDefLevel()).isEqualTo(5);
        assertThat(leaf.repLevels()).containsExactly(0, 2, 1, 0, 0, 0);
        assertThat(leaf.defLevels()).containsExactly(5, 5, 5, 3, 1, 0);
        assertThat(leaf.valueCount()).isEqualTo(3);
        assertThat(intValues(leaf)).containsExactly(1, 2, 3);
    }

    @Test
    void mapOfStringToOptionalIntStripesKeyAndValueLeaves() {
        ParquetSchema schema = mapOfStringToOptionalInt();
        // row0 {"a":10, "b":null}; row1 {} (empty); row2 null
        MemorySegment[] keyBytes = {utf8("a"), utf8("b")};
        BinaryVector keys = BinaryVector.materialized(keyBytes, Validity.allValid(2));
        IntVector values = IntVector.materialized(new int[] {10, 0}, validity(true, false));
        MapVector map = new MapVector(new int[] {0, 2, 2, 2}, keys, values, validity(true, true, false), 3);
        ParquetRecordBatch batch = heapBatch(schema, Map.of(ColumnPath.of("mymap"), map), 3);

        List<StripedLeaf> striped = new DremelStriper(schema).stripe(batch);

        StripedLeaf keyLeaf = leafFor(striped, ColumnPath.of("mymap", "key_value", "key"));
        // key REQUIRED: groupDef=1, keyMaxDef=elementDef=2; present entry def=2, empty/null phantom def 1/0
        assertThat(keyLeaf.maxRepLevel()).isEqualTo(1);
        assertThat(keyLeaf.maxDefLevel()).isEqualTo(2);
        assertThat(keyLeaf.repLevels()).containsExactly(0, 1, 0, 0);
        assertThat(keyLeaf.defLevels()).containsExactly(2, 2, 1, 0);
        assertThat(keyLeaf.valueCount()).isEqualTo(2);
        assertThat(stringValues(keyLeaf)).containsExactly("a", "b");

        StripedLeaf valueLeaf = leafFor(striped, ColumnPath.of("mymap", "key_value", "value"));
        // value OPTIONAL: groupDef=1, entryDef=2, valueMaxDef=3; "b" value is null -> def 2 keeps the entry, no value
        assertThat(valueLeaf.maxRepLevel()).isEqualTo(1);
        assertThat(valueLeaf.maxDefLevel()).isEqualTo(3);
        assertThat(valueLeaf.repLevels()).containsExactly(0, 1, 0, 0);
        assertThat(valueLeaf.defLevels()).containsExactly(3, 2, 1, 0);
        assertThat(valueLeaf.valueCount()).isEqualTo(1);
        assertThat(intValues(valueLeaf)).containsExactly(10);

        assertMapRoundTrips(keyLeaf, map, 3, 1, 2);
    }

    @Test
    void requiredListWithNullRowIsRejected() {
        ParquetSchema schema = requiredListOfOptionalInt();
        IntVector elements = IntVector.materialized(new int[] {1}, Validity.allValid(1));
        // row0 [1]; row1 null (illegal for a REQUIRED container)
        ListVector list = new ListVector(new int[] {0, 1, 1}, elements, validity(true, false), 2);
        ParquetRecordBatch batch = heapBatch(schema, Map.of(ColumnPath.of("mylist"), list), 2);

        DremelStriper striper = new DremelStriper(schema);
        assertThatThrownBy(() -> striper.stripe(batch))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("mylist")
                .hasMessageContaining("null");
    }

    @Test
    void repeatedLeafBackedByNonListVectorIsRejected() {
        SchemaNode.Primitive repeated = new SchemaNode.Primitive(
                "tags", Repetition.REPEATED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group list = new SchemaNode.Group(
                "mylist", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        ParquetSchema schema = schemaOf(list);
        // the leaf is repeated but the batch supplies a flat IntVector rather than a ListVector
        IntVector values = IntVector.materialized(new int[] {1}, Validity.allValid(1));
        ParquetRecordBatch batch = heapBatch(schema, Map.of(ColumnPath.of("mylist"), values), 1);

        DremelStriper striper = new DremelStriper(schema);
        assertThatThrownBy(() -> striper.stripe(batch))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("mylist");
    }

    @Test
    void everyRepeatedLeafChainIsBuiltAtConstruction() {
        ParquetSchema schema = listOfOptionalInt();
        int repeatedLeaves = 0;
        for (ColumnPath leaf : schema.leafColumns()) {
            if (schema.maxLevels(leaf).maxRepetitionLevel() > 0) {
                repeatedLeaves++;
            }
        }

        DremelStriper striper = new DremelStriper(schema);

        assertThat(striper.precomputedChainCount())
                .as("chains are schema-derived, hence knowable before any batch arrives")
                .isEqualTo(repeatedLeaves)
                .isGreaterThan(0);
    }

    /**
     * Replays the read-side repeated-layout state machine over the striped streams to prove they invert: the rebuilt
     * per-slot offsets, validity, and kept element values must reproduce the original {@link ListVector}.
     */
    private static void assertListRoundTrips(
            StripedLeaf leaf, ListVector original, int numSlots, int thisRepLevel, int elementDefLevel) {
        RebuiltLayout rebuilt = replayLayout(leaf, numSlots, 0, thisRepLevel, leaf.maxDefLevel() - 2, elementDefLevel);
        for (int slot = 0; slot < numSlots; slot++) {
            assertThat(rebuilt.offsets[slot]).isEqualTo(original.rowOffsetStart(slot));
            assertThat(rebuilt.offsets[slot + 1]).isEqualTo(original.rowOffsetEnd(slot));
            assertThat(rebuilt.validity.get(slot)).isEqualTo(original.validity().isValid(slot));
        }
    }

    private static void assertMapRoundTrips(
            StripedLeaf keyLeaf, MapVector original, int numSlots, int thisRepLevel, int elementDefLevel) {
        int groupDefLevel = elementDefLevel - 1;
        RebuiltLayout rebuilt = replayLayout(keyLeaf, numSlots, 0, thisRepLevel, groupDefLevel, elementDefLevel);
        for (int slot = 0; slot < numSlots; slot++) {
            assertThat(rebuilt.offsets[slot]).isEqualTo(original.rowOffsetStart(slot));
            assertThat(rebuilt.offsets[slot + 1]).isEqualTo(original.rowOffsetEnd(slot));
            assertThat(rebuilt.validity.get(slot)).isEqualTo(original.validity().isValid(slot));
        }
    }

    private static RebuiltLayout replayLayout(
            StripedLeaf leaf,
            int numSlots,
            int parentRepLevel,
            int thisRepLevel,
            int groupDefLevel,
            int elementDefLevel) {
        int[] rep = leaf.repLevels();
        int[] def = leaf.defLevels();
        int[] offsets = new int[numSlots + 1];
        BitSet validity = new BitSet(numSlots);
        int slot = -1;
        int elementCount = 0;
        for (int i = 0; i < rep.length; i++) {
            if (rep[i] <= parentRepLevel) {
                slot++;
                offsets[slot] = elementCount;
                if (def[i] >= groupDefLevel) {
                    validity.set(slot);
                }
            }
            if (rep[i] <= thisRepLevel && def[i] >= elementDefLevel) {
                elementCount++;
            }
        }
        for (int s = slot + 1; s <= numSlots; s++) {
            offsets[s] = elementCount;
        }
        return new RebuiltLayout(offsets, validity);
    }

    private record RebuiltLayout(int[] offsets, BitSet validity) {}

    private static int[] intValues(StripedLeaf leaf) {
        IntVector values = (IntVector) leaf.sourceValues();
        int[] keptOrdinals = leaf.keptOrdinals();
        int[] out = new int[leaf.valueCount()];
        for (int i = 0; i < out.length; i++) {
            out[i] = values.valueAt(keptOrdinals[i]);
        }
        return out;
    }

    private static String[] stringValues(StripedLeaf leaf) {
        BinaryVector vector = (BinaryVector) leaf.sourceValues();
        int[] keptOrdinals = leaf.keptOrdinals();
        String[] out = new String[leaf.valueCount()];
        for (int i = 0; i < out.length; i++) {
            MemorySegment bytes = vector.get(keptOrdinals[i]);
            out[i] = new String(bytes.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
        }
        return out;
    }

    private static MemorySegment utf8(String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        return Arena.ofAuto().allocate(bytes.length).copyFrom(MemorySegment.ofArray(bytes));
    }

    private static StripedLeaf leafFor(List<StripedLeaf> striped, ColumnPath path) {
        return striped.stream()
                .filter(leaf -> leaf.leaf().equals(path))
                .findFirst()
                .orElseThrow();
    }

    private static SchemaNode.Primitive primitive(String name, Repetition repetition) {
        return new SchemaNode.Primitive(
                name, repetition, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static ParquetSchema schemaOf(SchemaNode child) {
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(child), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static ParquetSchema optionalFieldUnderOptionalStruct() {
        SchemaNode.Primitive f = primitive("f", Repetition.OPTIONAL);
        SchemaNode.Group s = new SchemaNode.Group("s", Repetition.OPTIONAL, List.of(f), Optional.empty(), -1);
        return schemaOf(s);
    }

    private static ParquetSchema optionalFieldUnderTwoOptionalStructs() {
        SchemaNode.Primitive f = primitive("f", Repetition.OPTIONAL);
        SchemaNode.Group b = new SchemaNode.Group("b", Repetition.OPTIONAL, List.of(f), Optional.empty(), -1);
        SchemaNode.Group a = new SchemaNode.Group("a", Repetition.OPTIONAL, List.of(b), Optional.empty(), -1);
        return schemaOf(a);
    }

    private static ParquetSchema optionalVariant() {
        SchemaNode.Primitive metadata = new SchemaNode.Primitive(
                "metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                "value", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group variant = new SchemaNode.Group(
                "v", Repetition.OPTIONAL, List.of(metadata, value), Optional.of(new LogicalType.Variant()), -1);
        return schemaOf(variant);
    }

    private static ParquetSchema requiredAndOptionalMixSchema() {
        SchemaNode.Primitive r = primitive("r", Repetition.REQUIRED);
        SchemaNode.Group s = new SchemaNode.Group("s", Repetition.OPTIONAL, List.of(r), Optional.empty(), -1);
        SchemaNode.Primitive o = primitive("o", Repetition.OPTIONAL);
        SchemaNode.Group t = new SchemaNode.Group("t", Repetition.REQUIRED, List.of(o), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(s, t), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static ParquetSchema listOfOptionalInt() {
        SchemaNode.Primitive element = primitive("element", Repetition.OPTIONAL);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group list = new SchemaNode.Group(
                "mylist", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        return schemaOf(list);
    }

    private static ParquetSchema requiredListOfOptionalInt() {
        SchemaNode.Primitive element = primitive("element", Repetition.OPTIONAL);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group list = new SchemaNode.Group(
                "mylist", Repetition.REQUIRED, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        return schemaOf(list);
    }

    private static ParquetSchema listOfListOfInt() {
        SchemaNode.Primitive element = primitive("element", Repetition.OPTIONAL);
        SchemaNode.Group innerRepeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group inner = new SchemaNode.Group(
                "inner", Repetition.OPTIONAL, List.of(innerRepeated), Optional.of(new LogicalType.ListType()), -1);
        SchemaNode.Group outerRepeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(inner), Optional.empty(), -1);
        SchemaNode.Group outer = new SchemaNode.Group(
                "outer", Repetition.OPTIONAL, List.of(outerRepeated), Optional.of(new LogicalType.ListType()), -1);
        return schemaOf(outer);
    }

    private static ParquetSchema mapOfStringToOptionalInt() {
        SchemaNode.Primitive key = new SchemaNode.Primitive(
                "key",
                Repetition.REQUIRED,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
        SchemaNode.Primitive value = primitive("value", Repetition.OPTIONAL);
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        SchemaNode.Group map = new SchemaNode.Group(
                "mymap", Repetition.OPTIONAL, List.of(keyValue), Optional.of(new LogicalType.MapType()), -1);
        return schemaOf(map);
    }

    private static Validity validity(boolean... valid) {
        BitSet bits = new BitSet(valid.length);
        for (int row = 0; row < valid.length; row++) {
            if (valid[row]) {
                bits.set(row);
            }
        }
        return Validity.of(bits, valid.length);
    }

    private static ParquetRecordBatch heapBatch(ParquetSchema schema, Map<ColumnPath, ColumnVector> columns, int rows) {
        return DefaultParquetRecordBatch.ofHeap(schema, columns, rows);
    }
}
