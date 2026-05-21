/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.record;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.MapVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;

/**
 * Verifies that {@link BatchBackedParquetRecord} dispatches to the right {@link ColumnVector} subtype, honours
 * validity, copies bytes for binary accessors, materializes list and map cells, and returns
 * {@link BatchBackedSubRecord} views for nested struct cells.
 */
class BatchBackedParquetRecordTest {

    private static final ColumnPath INT_COL = ColumnPath.of("col_int");
    private static final ColumnPath LONG_COL = ColumnPath.of("col_long");
    private static final ColumnPath FLOAT_COL = ColumnPath.of("col_float");
    private static final ColumnPath DOUBLE_COL = ColumnPath.of("col_double");
    private static final ColumnPath BOOL_COL = ColumnPath.of("col_bool");
    private static final ColumnPath BINARY_COL = ColumnPath.of("col_binary");
    private static final ColumnPath LIST_COL = ColumnPath.of("col_list");
    private static final ColumnPath MAP_COL = ColumnPath.of("col_map");
    private static final ColumnPath STRUCT_COL = ColumnPath.of("col_struct");
    private static final ColumnPath STRUCT_CHILD = ColumnPath.of("col_struct", "child_int");

    @Test
    void primitiveAccessorsReturnBoxedValuesPerVectorSubtype() {
        BitSet validity = allValid(2);
        Map<ColumnPath, ColumnVector> cols = new LinkedHashMap<>();
        cols.put(INT_COL, IntVector.materialized(new int[] {7, 11}, validity));
        cols.put(LONG_COL, LongVector.materialized(new long[] {100L, 200L}, validity));
        cols.put(FLOAT_COL, FloatVector.materialized(new float[] {1.5f, 2.5f}, validity));
        cols.put(DOUBLE_COL, DoubleVector.materialized(new double[] {3.5, 4.5}, validity));
        cols.put(BOOL_COL, BooleanVector.materialized(new boolean[] {true, false}, validity));

        try (ParquetRecordBatch batch = new DefaultParquetRecordBatch(primitiveSchema(), cols, 2, Arena.ofConfined())) {
            ParquetRecord row0 = batch.materialize(0);
            ParquetRecord row1 = batch.materialize(1);

            assertThat(row0.getInt(INT_COL)).isEqualTo(7);
            assertThat(row1.getInt(INT_COL)).isEqualTo(11);
            assertThat(row0.getLong(LONG_COL)).isEqualTo(100L);
            assertThat(row0.getFloat(FLOAT_COL)).isEqualTo(1.5f);
            assertThat(row0.getDouble(DOUBLE_COL)).isEqualTo(3.5);
            assertThat(row0.getBoolean(BOOL_COL)).isTrue();
            assertThat(row1.getBoolean(BOOL_COL)).isFalse();
        }
    }

    @Test
    void nullValidityReturnsNullOnGetAndTrueOnIsNull() {
        BitSet validity = new BitSet(2);
        validity.set(0);
        IntVector vec = IntVector.materialized(new int[] {42, 0}, validity);

        try (ParquetRecordBatch batch =
                new DefaultParquetRecordBatch(primitiveSchema(), Map.of(INT_COL, vec), 2, Arena.ofConfined())) {
            ParquetRecord row0 = batch.materialize(0);
            ParquetRecord row1 = batch.materialize(1);

            assertThat(row0.isNull(INT_COL)).isFalse();
            assertThat(row0.get(INT_COL)).isEqualTo(42);

            assertThat(row1.isNull(INT_COL)).isTrue();
            assertThat(row1.get(INT_COL)).isNull();
        }
    }

    @Test
    void getBinaryReturnsFreshArrayThatIsSafeToMutate() {
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        Arena arena = Arena.ofConfined();
        MemorySegment source = arena.allocate(payload.length);
        MemorySegment.copy(MemorySegment.ofArray(payload), 0L, source, 0L, payload.length);
        BinaryVector vec = BinaryVector.materialized(new MemorySegment[] {source}, allValid(1));

        try (ParquetRecordBatch batch =
                new DefaultParquetRecordBatch(binarySchema(), Map.of(BINARY_COL, vec), 1, arena)) {
            ParquetRecord row = batch.materialize(0);

            byte[] copy = row.getBinary(BINARY_COL);
            assertThat(copy).isEqualTo(payload);

            copy[0] = 'X';
            byte[] freshCopy = row.getBinary(BINARY_COL);
            assertThat(freshCopy).as("subsequent getBinary returns a new array").isEqualTo(payload);
            assertThat(source.get(ValueLayout.JAVA_BYTE, 0L))
                    .as("source MemorySegment is untouched by caller mutation")
                    .isEqualTo((byte) 'h');
        }
    }

    @Test
    void getStringDecodesBinaryAsUtf8() {
        byte[] payload = "café".getBytes(StandardCharsets.UTF_8);
        Arena arena = Arena.ofConfined();
        MemorySegment source = arena.allocate(payload.length);
        MemorySegment.copy(MemorySegment.ofArray(payload), 0L, source, 0L, payload.length);
        BinaryVector vec = BinaryVector.materialized(new MemorySegment[] {source}, allValid(1));

        try (ParquetRecordBatch batch =
                new DefaultParquetRecordBatch(binarySchema(), Map.of(BINARY_COL, vec), 1, arena)) {
            ParquetRecord row = batch.materialize(0);
            assertThat(row.getString(BINARY_COL)).isEqualTo("café");
        }
    }

    @Test
    void listCellMaterializesThroughHelper() {
        BitSet listValidity = allValid(2);
        BitSet childValidity = allValid(3);
        IntVector child = IntVector.materialized(new int[] {10, 20, 30}, childValidity);
        ListVector listVec = new ListVector(new int[] {0, 2, 3}, child, listValidity, 2);

        try (ParquetRecordBatch batch =
                new DefaultParquetRecordBatch(listSchema(), Map.of(LIST_COL, listVec), 2, Arena.ofConfined())) {
            ParquetRecord row0 = batch.materialize(0);
            ParquetRecord row1 = batch.materialize(1);

            assertThat(row0.get(LIST_COL)).isEqualTo(List.of(10, 20));
            assertThat(row1.get(LIST_COL)).isEqualTo(List.of(30));
        }
    }

    @Test
    void mapCellMaterializesAsLinkedHashMap() {
        BitSet mapValidity = allValid(1);
        BitSet keyValidity = allValid(2);
        BitSet valueValidity = allValid(2);
        IntVector keys = IntVector.materialized(new int[] {1, 2}, keyValidity);
        IntVector values = IntVector.materialized(new int[] {10, 20}, valueValidity);
        MapVector mapVec = new MapVector(new int[] {0, 2}, keys, values, mapValidity, 1);

        try (ParquetRecordBatch batch =
                new DefaultParquetRecordBatch(mapSchema(), Map.of(MAP_COL, mapVec), 1, Arena.ofConfined())) {
            ParquetRecord row = batch.materialize(0);

            Object cell = row.get(MAP_COL);
            assertThat(cell).isInstanceOf(Map.class);
            @SuppressWarnings("unchecked")
            Map<Integer, Integer> asMap = (Map<Integer, Integer>) cell;
            assertThat(asMap).hasSize(2);
            assertThat(asMap).containsEntry(1, 10).containsEntry(2, 20);
            assertThat(asMap.keySet()).containsExactly(1, 2);
        }
    }

    @Test
    void structCellReturnsSubRecordWithDrillDownAccess() {
        BitSet structValidity = allValid(1);
        BitSet childValidity = allValid(1);
        IntVector childInt = IntVector.materialized(new int[] {123}, childValidity);
        StructVector structVec = new StructVector(Map.of(STRUCT_CHILD, childInt), structValidity, 1);

        try (ParquetRecordBatch batch =
                new DefaultParquetRecordBatch(structSchema(), Map.of(STRUCT_COL, structVec), 1, Arena.ofConfined())) {
            ParquetRecord row = batch.materialize(0);

            ParquetRecord sub = row.getStruct(STRUCT_COL);
            assertThat(sub).isInstanceOf(BatchBackedSubRecord.class);
            assertThat(sub.getInt(STRUCT_CHILD)).isEqualTo(123);
            assertThat(sub.get(STRUCT_CHILD)).isEqualTo(123);
            assertThat(sub.schema()).isEqualTo(batch.projectedSchema());
        }
    }

    @Test
    void structCellReturnsNullWhenValidityClear() {
        BitSet structValidity = new BitSet(1);
        IntVector childInt = IntVector.materialized(new int[] {0}, new BitSet(1));
        StructVector structVec = new StructVector(Map.of(STRUCT_CHILD, childInt), structValidity, 1);

        try (ParquetRecordBatch batch =
                new DefaultParquetRecordBatch(structSchema(), Map.of(STRUCT_COL, structVec), 1, Arena.ofConfined())) {
            ParquetRecord row = batch.materialize(0);

            assertThat(row.getStruct(STRUCT_COL)).isNull();
            assertThat(row.get(STRUCT_COL)).isNull();
            assertThat(row.isNull(STRUCT_COL)).isTrue();
        }
    }

    @Test
    void typedAccessorRejectsKindMismatch() {
        IntVector vec = IntVector.materialized(new int[] {1}, allValid(1));

        try (ParquetRecordBatch batch =
                new DefaultParquetRecordBatch(primitiveSchema(), Map.of(INT_COL, vec), 1, Arena.ofConfined())) {
            ParquetRecord row = batch.materialize(0);

            assertThatThrownBy(() -> row.getLong(INT_COL))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("requested getLong");
        }
    }

    @Test
    void rowIndexOutOfBoundsRejected() {
        IntVector vec = IntVector.materialized(new int[] {1}, allValid(1));

        try (ParquetRecordBatch batch =
                new DefaultParquetRecordBatch(primitiveSchema(), Map.of(INT_COL, vec), 1, Arena.ofConfined())) {
            assertThatThrownBy(() -> batch.materialize(-1)).isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> batch.materialize(1)).isInstanceOf(IndexOutOfBoundsException.class);
        }
    }

    @Test
    void missingColumnReturnsNullFromGetAndIsNull() {
        IntVector vec = IntVector.materialized(new int[] {1}, allValid(1));
        ColumnPath unprojected = ColumnPath.of("col_long");

        try (ParquetRecordBatch batch =
                new DefaultParquetRecordBatch(primitiveSchema(), Map.of(INT_COL, vec), 1, Arena.ofConfined())) {
            ParquetRecord row = batch.materialize(0);

            assertThat(row.get(unprojected)).isNull();
            assertThat(row.isNull(unprojected)).isTrue();
        }
    }

    private static BitSet allValid(int size) {
        BitSet bits = new BitSet(size);
        bits.set(0, size);
        return bits;
    }

    private static ParquetSchema primitiveSchema() {
        return new ParquetSchema(new Field.Group(
                "root",
                Repetition.REQUIRED,
                List.of(
                        primitiveLeaf("col_int", PrimitiveKind.INT32),
                        primitiveLeaf("col_long", PrimitiveKind.INT64),
                        primitiveLeaf("col_float", PrimitiveKind.FLOAT),
                        primitiveLeaf("col_double", PrimitiveKind.DOUBLE),
                        primitiveLeaf("col_bool", PrimitiveKind.BOOLEAN)),
                Optional.empty(),
                -1));
    }

    private static ParquetSchema binarySchema() {
        return new ParquetSchema(new Field.Group(
                "root",
                Repetition.REQUIRED,
                List.of(primitiveLeaf("col_binary", PrimitiveKind.BYTE_ARRAY)),
                Optional.empty(),
                -1));
    }

    private static ParquetSchema listSchema() {
        Field.Primitive element = primitiveLeaf("element", PrimitiveKind.INT32);
        Field.Group listGroup =
                new Field.Group("col_list", Repetition.REQUIRED, List.of(element), Optional.empty(), -1);
        return new ParquetSchema(
                new Field.Group("root", Repetition.REQUIRED, List.of(listGroup), Optional.empty(), -1));
    }

    private static ParquetSchema mapSchema() {
        Field.Primitive keyLeaf = primitiveLeaf("key", PrimitiveKind.INT32);
        Field.Primitive valueLeaf = primitiveLeaf("value", PrimitiveKind.INT32);
        Field.Group mapGroup =
                new Field.Group("col_map", Repetition.REQUIRED, List.of(keyLeaf, valueLeaf), Optional.empty(), -1);
        return new ParquetSchema(new Field.Group("root", Repetition.REQUIRED, List.of(mapGroup), Optional.empty(), -1));
    }

    private static ParquetSchema structSchema() {
        Field.Primitive childInt = primitiveLeaf("child_int", PrimitiveKind.INT32);
        Field.Group structGroup =
                new Field.Group("col_struct", Repetition.REQUIRED, List.of(childInt), Optional.empty(), -1);
        return new ParquetSchema(
                new Field.Group("root", Repetition.REQUIRED, List.of(structGroup), Optional.empty(), -1));
    }

    private static Field.Primitive primitiveLeaf(String name, PrimitiveKind kind) {
        return new Field.Primitive(name, Repetition.REQUIRED, kind, OptionalInt.empty(), Optional.empty(), -1);
    }
}
