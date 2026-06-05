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
package io.tileverse.parquetry.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
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
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class CanonicalRowFromParquetryTest {

    @Test
    void flatPrimitivesAndBinaryAndNull() {
        SchemaNode.Primitive i = new SchemaNode.Primitive(
                "i", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive s = new SchemaNode.Primitive(
                "s", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive opt = new SchemaNode.Primitive(
                "opt", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(i, s, opt), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);

        BitSet presentBits = new BitSet(1);
        presentBits.set(0);
        Validity present = Validity.of(presentBits, 1);
        Validity absent = Validity.of(new BitSet(1), 1);
        IntVector iVec = IntVector.materialized(new int[] {7}, present);
        BinaryVector sVec =
                BinaryVector.materialized(new MemorySegment[] {MemorySegment.ofArray("hi".getBytes())}, present);
        IntVector optVec = IntVector.materialized(new int[] {0}, absent);
        Map<ColumnPath, ColumnVector> cols = Map.of(
                ColumnPath.of("i"), iVec,
                ColumnPath.of("s"), sVec,
                ColumnPath.of("opt"), optVec);

        try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, cols, 1, Arena.ofConfined())) {
            ParquetRecord row = batch.materialize(0);
            Map<String, Object> canonical = CanonicalRow.fromParquetry(row, schema);

            assertThat(canonical.get("i")).as("int leaf").isEqualTo(7);
            assertThat(canonical.get("s")).as("binary leaf as ByteBuffer").isEqualTo(ByteBuffer.wrap("hi".getBytes()));
            assertThat(canonical.get("opt")).as("absent optional is null").isNull();
            assertThat(canonical.keySet()).as("field order preserved").containsExactly("i", "s", "opt");
        }
    }

    @Test
    void nestedStructBecomesNestedMap() {
        SchemaNode.Primitive x = new SchemaNode.Primitive(
                "x", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive y = new SchemaNode.Primitive(
                "y", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group point =
                new SchemaNode.Group("point", Repetition.REQUIRED, List.of(x, y), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(point), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);

        BitSet presentBits = new BitSet(1);
        presentBits.set(0);
        Validity present = Validity.of(presentBits, 1);
        IntVector xVec = IntVector.materialized(new int[] {3}, present);
        IntVector yVec = IntVector.materialized(new int[] {4}, present);
        StructVector pointVec =
                new StructVector(Map.of(ColumnPath.of("x"), xVec, ColumnPath.of("y"), yVec), present, 1);
        Map<ColumnPath, ColumnVector> cols = Map.of(ColumnPath.of("point"), pointVec);

        try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, cols, 1, Arena.ofConfined())) {
            Map<String, Object> canonical = CanonicalRow.fromParquetry(batch.materialize(0), schema);
            assertThat(canonical.get("point")).as("struct as nested map").isEqualTo(Map.of("x", 3, "y", 4));
        }
    }

    @Test
    void mapCellBecomesLinkedHashMap() {
        SchemaNode.Primitive key = new SchemaNode.Primitive(
                "key", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive value = new SchemaNode.Primitive(
                "value", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        SchemaNode.Group mapNode = new SchemaNode.Group(
                "m2i", Repetition.REQUIRED, List.of(keyValue), Optional.of(new LogicalType.MapType()), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(mapNode), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);

        BitSet rowValidBits = new BitSet(1);
        rowValidBits.set(0);
        Validity rowValid = Validity.of(rowValidBits, 1);
        BitSet entryValidBits = new BitSet(2);
        entryValidBits.set(0, 2);
        Validity entryValid = Validity.of(entryValidBits, 2);
        IntVector keys = IntVector.materialized(new int[] {1, 2}, entryValid);
        IntVector values = IntVector.materialized(new int[] {10, 20}, entryValid);
        MapVector mapVec = new MapVector(new int[] {0, 2}, keys, values, rowValid, 1);
        Map<ColumnPath, ColumnVector> cols = Map.of(ColumnPath.of("m2i"), mapVec);

        try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, cols, 1, Arena.ofConfined())) {
            Object cell =
                    CanonicalRow.fromParquetry(batch.materialize(0), schema).get("m2i");
            assertThat(cell).as("map cell type").isInstanceOf(Map.class);
            @SuppressWarnings("unchecked")
            Map<Object, Object> entries = (Map<Object, Object>) cell;
            assertThat(entries)
                    .as("entries")
                    .containsEntry(1, 10)
                    .containsEntry(2, 20)
                    .hasSize(2);
        }
    }

    @Test
    void threeLevelListOfInt() {
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group listWrapper =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group nums = new SchemaNode.Group(
                "nums", Repetition.OPTIONAL, List.of(listWrapper), Optional.of(new LogicalType.ListType()), -1);
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(nums), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);

        BitSet rowValidBits = new BitSet(3);
        rowValidBits.set(0);
        rowValidBits.set(1);
        Validity rowValid = Validity.of(rowValidBits, 3);
        BitSet elemValidBits = new BitSet(2);
        elemValidBits.set(0, 2);
        Validity elemValid = Validity.of(elemValidBits, 2);
        IntVector elems = IntVector.materialized(new int[] {10, 20}, elemValid);
        ListVector listVec = new ListVector(new int[] {0, 2, 2, 2}, elems, rowValid, 3);
        Map<ColumnPath, ColumnVector> cols = Map.of(ColumnPath.of("nums"), listVec);

        try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, cols, 3, Arena.ofConfined())) {
            assertThat(CanonicalRow.fromParquetry(batch.materialize(0), schema).get("nums"))
                    .as("present list")
                    .isEqualTo(List.of(10, 20));
            assertThat(CanonicalRow.fromParquetry(batch.materialize(1), schema).get("nums"))
                    .as("empty list")
                    .isEqualTo(List.of());
            assertThat(CanonicalRow.fromParquetry(batch.materialize(2), schema).get("nums"))
                    .as("null list")
                    .isNull();
        }
    }
}
