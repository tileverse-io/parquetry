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
import java.lang.foreign.MemorySegment;
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
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.batch.MapVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Reconstructs the null-versus-empty distinction for outermost LIST and MAP columns by driving
 * {@link NestedVectorAssembler#assembleNested} with per-leaf repetition- and definition-level streams and reading the
 * result back through the record API.
 *
 * <p>Each logical row in a Parquet list/map column emits at least one leaf entry. The definition level at that first
 * entry encodes the row's state:
 *
 * <ul>
 *   <li>def below the wrapper group's level -&gt; the cell is null.
 *   <li>def at the wrapper group's level -&gt; the cell is present and empty.
 *   <li>def at the entry-present level -&gt; the cell has at least one element.
 * </ul>
 *
 * <p>Schema under test for the list: {@code optional group items (LIST) { repeated group list { optional int32 element
 * } }}. The {@code items} group definition level is 1, the {@code element} leaf definition level is 3.
 */
class ListMapNullVsEmptyTest {

    private static final ColumnPath ITEMS = ColumnPath.of("items");
    private static final ColumnPath ELEMENT = ColumnPath.of("items", "list", "element");

    private static final ColumnPath MAP = ColumnPath.of("m");
    private static final ColumnPath MAP_KEY = ColumnPath.of("m", "key_value", "key");
    private static final ColumnPath MAP_VALUE = ColumnPath.of("m", "key_value", "value");

    @Test
    void nullEmptyAndPopulatedListsReconstructFromDefLevels() {
        ParquetSchema schema = listOfInt32Schema();

        IntVector elementLeaf = IntVector.materialized(new int[] {0, 0, 5, 6}, validity(false, false, true, true));
        Map<ColumnPath, ColumnVector> leafVectors = Map.of(ELEMENT, elementLeaf);

        int[] repLevels = {0, 0, 0, 1};
        int[] defLevels = {0, 1, 3, 3};
        Map<ColumnPath, int[]> repLevelsByLeaf = Map.of(ELEMENT, repLevels);
        Map<ColumnPath, int[]> defLevelsByLeaf = Map.of(ELEMENT, defLevels);

        Map<ColumnPath, ColumnVector> assembled =
                NestedVectorAssembler.assembleNested(schema, leafVectors, repLevelsByLeaf, defLevelsByLeaf, 3);

        ListVector listVec = (ListVector) assembled.get(ITEMS);
        assertThat(listVec.validity().get(0))
                .as("row 0 list is null, validity bit must be clear")
                .isFalse();
        assertThat(listVec.validity().get(1))
                .as("row 1 list is empty but present, validity bit must be set")
                .isTrue();
        assertThat(listVec.validity().get(2))
                .as("row 2 list is populated, validity bit must be set")
                .isTrue();

        try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, assembled, 3, Arena.ofConfined())) {
            ParquetRecord nullRow = batch.materialize(0);
            assertThat(nullRow.get(ITEMS)).as("row 0 null list reads as null").isNull();

            ParquetRecord emptyRow = batch.materialize(1);
            assertThat(emptyRow.get(ITEMS))
                    .as("row 1 empty list reads as a present, empty List")
                    .isInstanceOf(List.class);
            assertThat((List<?>) emptyRow.get(ITEMS))
                    .as("row 1 empty list has no elements")
                    .isEmpty();

            ParquetRecord populatedRow = batch.materialize(2);
            List<?> populatedList = (List<?>) populatedRow.get(ITEMS);
            assertThat(populatedList)
                    .as("row 2 populated list holds its elements in order")
                    .hasSize(2);
            assertThat(populatedList.get(0)).as("row 2 first element").isEqualTo(5);
            assertThat(populatedList.get(1)).as("row 2 second element").isEqualTo(6);
        }
    }

    @Test
    void nullEmptyAndPopulatedMapsReconstructFromDefLevels() {
        ParquetSchema schema = mapStringIntSchema();

        MemorySegment[] keyBytes = {null, null, bytesOf("k")};
        BinaryVector keyLeaf = BinaryVector.materialized(keyBytes, validity(false, false, true));
        IntVector valueLeaf = IntVector.materialized(new int[] {0, 0, 7}, validity(false, false, true));
        Map<ColumnPath, ColumnVector> leafVectors = Map.of(MAP_KEY, keyLeaf, MAP_VALUE, valueLeaf);

        int[] repLevels = {0, 0, 0};
        int[] keyDefLevels = {0, 1, 2};
        int[] valueDefLevels = {0, 1, 3};
        Map<ColumnPath, int[]> repLevelsByLeaf = Map.of(MAP_KEY, repLevels, MAP_VALUE, repLevels);
        Map<ColumnPath, int[]> defLevelsByLeaf = Map.of(MAP_KEY, keyDefLevels, MAP_VALUE, valueDefLevels);

        Map<ColumnPath, ColumnVector> assembled =
                NestedVectorAssembler.assembleNested(schema, leafVectors, repLevelsByLeaf, defLevelsByLeaf, 3);

        MapVector mapVec = (MapVector) assembled.get(MAP);
        assertThat(mapVec.validity().get(0))
                .as("row 0 map is null, validity bit must be clear")
                .isFalse();
        assertThat(mapVec.validity().get(1))
                .as("row 1 map is empty but present, validity bit must be set")
                .isTrue();
        assertThat(mapVec.validity().get(2))
                .as("row 2 map is populated, validity bit must be set")
                .isTrue();

        try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, assembled, 3, Arena.ofConfined())) {
            ParquetRecord nullRow = batch.materialize(0);
            assertThat(nullRow.get(MAP)).as("row 0 null map reads as null").isNull();

            ParquetRecord emptyRow = batch.materialize(1);
            assertThat(emptyRow.get(MAP))
                    .as("row 1 empty map reads as a present, empty Map")
                    .isInstanceOf(Map.class);
            assertThat((Map<?, ?>) emptyRow.get(MAP))
                    .as("row 1 empty map has no entries")
                    .isEmpty();

            ParquetRecord populatedRow = batch.materialize(2);
            Map<?, ?> populatedMap = (Map<?, ?>) populatedRow.get(MAP);
            assertThat(populatedMap)
                    .as("row 2 populated map holds a single entry")
                    .hasSize(1);
            Map.Entry<?, ?> onlyEntry = populatedMap.entrySet().iterator().next();
            assertThat(asString(onlyEntry.getKey()))
                    .as("row 2 populated map key is k")
                    .isEqualTo("k");
            assertThat(onlyEntry.getValue())
                    .as("row 2 populated map value is 7")
                    .isEqualTo(7);
        }
    }

    private static ParquetSchema listOfInt32Schema() {
        SchemaNode.Primitive elementLeaf = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group middle =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(elementLeaf), Optional.empty(), -1);
        SchemaNode.Group listGroup = new SchemaNode.Group(
                "items", Repetition.OPTIONAL, List.of(middle), Optional.of(new LogicalType.ListType()), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(listGroup), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static ParquetSchema mapStringIntSchema() {
        SchemaNode.Primitive keyLeaf = new SchemaNode.Primitive(
                "key", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive valueLeaf = new SchemaNode.Primitive(
                "value", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group middle = new SchemaNode.Group(
                "key_value", Repetition.REPEATED, List.of(keyLeaf, valueLeaf), Optional.empty(), -1);
        SchemaNode.Group mapGroup = new SchemaNode.Group(
                "m", Repetition.OPTIONAL, List.of(middle), Optional.of(new LogicalType.MapType()), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(mapGroup), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static MemorySegment bytesOf(String s) {
        return MemorySegment.ofArray(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String asString(Object key) {
        MemorySegment segment = (MemorySegment) key;
        return new String(segment.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
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
