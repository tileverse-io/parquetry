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
package io.tileverse.parquetry.arrow.ipc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.Int96Vector;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.MapVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.UnsupportedFeatureException;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class RoundTripNestedTest {

    @Test
    void roundTripsListOfIntWithNullAndEmptyRows() throws Exception {
        SchemaNode.Group list = listGroup("nums", leaf("element", PrimitiveKind.INT32, Optional.empty()));
        ParquetSchema schema = schema(list);
        IntVector elements = IntVector.materialized(new int[] {1, 2, 3}, Validity.allValid(3));
        Validity rows = validity(3, 0, 2); // row 1 null
        ColumnVector column = new ListVector(new int[] {0, 3, 3, 3}, elements, rows, 3);

        roundTrip(schema, "nums", column, 3, root -> {
            org.apache.arrow.vector.complex.ListVector vector =
                    (org.apache.arrow.vector.complex.ListVector) root.getVector("nums");
            assertThat(vector.getObject(0)).isEqualTo(List.of(1, 2, 3));
            assertThat(vector.isNull(1)).isTrue();
            assertThat(vector.getObject(2)).asList().isEmpty();
        });
    }

    @Test
    void roundTripsStructOfIntAndUtf8() throws Exception {
        SchemaNode.Group struct = structGroup(
                "person",
                leaf("age", PrimitiveKind.INT32, Optional.empty()),
                leaf("name", PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType())));
        ParquetSchema schema = schema(struct);
        Map<ColumnPath, ColumnVector> fields = new LinkedHashMap<>();
        fields.put(ColumnPath.of("age"), IntVector.materialized(new int[] {30, 40}, Validity.allValid(2)));
        fields.put(ColumnPath.of("name"), utf8("ann", "bob"));
        ColumnVector column = new StructVector(fields, Validity.allValid(2), 2);

        roundTrip(schema, "person", column, 2, root -> {
            org.apache.arrow.vector.complex.StructVector vector =
                    (org.apache.arrow.vector.complex.StructVector) root.getVector("person");
            org.apache.arrow.vector.IntVector age = (org.apache.arrow.vector.IntVector) vector.getChild("age");
            org.apache.arrow.vector.VarCharVector name =
                    (org.apache.arrow.vector.VarCharVector) vector.getChild("name");
            assertThat(age.get(0)).isEqualTo(30);
            assertThat(age.get(1)).isEqualTo(40);
            assertThat(new String(name.get(0), StandardCharsets.UTF_8)).isEqualTo("ann");
            assertThat(new String(name.get(1), StandardCharsets.UTF_8)).isEqualTo("bob");
        });
    }

    @Test
    void roundTripsMapOfUtf8ToInt64() throws Exception {
        SchemaNode.Group map = mapGroup(
                "props",
                leaf("key", PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType())),
                leaf("value", PrimitiveKind.INT64, Optional.empty()));
        ParquetSchema schema = schema(map);
        BinaryVector keys = utf8("k1", "k2", "k3");
        LongVector values = LongVector.materialized(new long[] {1L, 2L, 3L}, Validity.allValid(3));
        ColumnVector column = new MapVector(new int[] {0, 2, 3}, keys, values, Validity.allValid(2), 2);

        roundTrip(schema, "props", column, 2, root -> {
            org.apache.arrow.vector.complex.MapVector vector =
                    (org.apache.arrow.vector.complex.MapVector) root.getVector("props");
            List<?> row0 = (List<?>) vector.getObject(0);
            List<?> row1 = (List<?>) vector.getObject(1);
            assertThat(row0).hasSize(2);
            assertThat(row1).hasSize(1);
            assertThat(entryKey(row0.get(0))).isEqualTo("k1");
            assertThat(entryValue(row0.get(0))).isEqualTo(1L);
            assertThat(entryKey(row1.get(0))).isEqualTo("k3");
            assertThat(entryValue(row1.get(0))).isEqualTo(3L);
        });
    }

    @Test
    void roundTripsListOfStruct() throws Exception {
        SchemaNode.Group elementStruct = structGroup("element", leaf("x", PrimitiveKind.INT32, Optional.empty()));
        SchemaNode.Group list = listGroup("items", elementStruct);
        ParquetSchema schema = schema(list);
        Map<ColumnPath, ColumnVector> structFields = new LinkedHashMap<>();
        structFields.put(ColumnPath.of("x"), IntVector.materialized(new int[] {1, 2}, Validity.allValid(2)));
        StructVector structs = new StructVector(structFields, Validity.allValid(2), 2);
        ColumnVector column = new ListVector(new int[] {0, 2}, structs, Validity.allValid(1), 1);

        roundTrip(schema, "items", column, 1, root -> {
            org.apache.arrow.vector.complex.ListVector vector =
                    (org.apache.arrow.vector.complex.ListVector) root.getVector("items");
            List<?> row0 = (List<?>) vector.getObject(0);
            assertThat(row0).hasSize(2);
        });
    }

    @Test
    void appliesDecimalTransformInsideAList() throws Exception {
        SchemaNode.Primitive element = leaf("element", PrimitiveKind.INT32, Optional.of(new LogicalType.Decimal(2, 9)));
        SchemaNode.Group list = listGroup("amounts", element);
        ParquetSchema schema = schema(list);
        IntVector unscaled = IntVector.materialized(new int[] {100, 250}, Validity.allValid(2));
        ColumnVector column = new ListVector(new int[] {0, 2}, unscaled, Validity.allValid(1), 1);

        roundTrip(schema, "amounts", column, 1, root -> {
            org.apache.arrow.vector.complex.ListVector vector =
                    (org.apache.arrow.vector.complex.ListVector) root.getVector("amounts");
            org.apache.arrow.vector.DecimalVector decimals =
                    (org.apache.arrow.vector.DecimalVector) vector.getDataVector();
            assertThat(decimals.getObject(0)).isEqualByComparingTo(new BigDecimal("1.00"));
            assertThat(decimals.getObject(1)).isEqualByComparingTo(new BigDecimal("2.50"));
        });
    }

    @Test
    void appliesInt96TransformInsideAStruct() throws Exception {
        SchemaNode.Group struct = structGroup("event", leaf("ts", PrimitiveKind.INT96, Optional.empty()));
        ParquetSchema schema = schema(struct);
        Map<ColumnPath, ColumnVector> fields = new LinkedHashMap<>();
        fields.put(ColumnPath.of("ts"), Int96Vector.of(MemorySegment.ofArray(new byte[12]), Validity.allValid(1)));
        ColumnVector column = new StructVector(fields, Validity.allValid(1), 1);

        roundTrip(schema, "event", column, 1, root -> {
            org.apache.arrow.vector.complex.StructVector vector =
                    (org.apache.arrow.vector.complex.StructVector) root.getVector("event");
            assertThat(vector.getChild("ts")).isInstanceOf(org.apache.arrow.vector.TimeStampMicroVector.class);
        });
    }

    @Test
    void rejectsVariantNestedUnderAList() {
        SchemaNode.Group variant = variantGroup("v");
        SchemaNode.Group list = listGroup("vs", variant);
        ParquetSchema schema = schema(list);
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, Map.of(), 0, Arena.ofShared());
        Stream<ParquetRecordBatch> batches = Stream.of(batch);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        assertThatThrownBy(() -> ArrowIpcWriter.write(schema, Optional.empty(), batches, out))
                .isInstanceOf(UnsupportedFeatureException.class)
                .hasMessageContaining("Variant nested under a list or map");
    }

    private static void roundTrip(
            ParquetSchema schema,
            String columnName,
            ColumnVector vector,
            int rowCount,
            Consumer<VectorSchemaRoot> assertions)
            throws Exception {
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of(columnName), vector);
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, rowCount, Arena.ofShared());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(batch), out);

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader =
                        new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            assertions.accept(reader.getVectorSchemaRoot());
        }
    }

    private static String entryKey(Object entry) {
        return ((Map<?, ?>) entry).get("key").toString();
    }

    private static long entryValue(Object entry) {
        return ((Number) ((Map<?, ?>) entry).get("value")).longValue();
    }

    private static BinaryVector utf8(String... values) {
        MemorySegment[] segments = new MemorySegment[values.length];
        for (int i = 0; i < values.length; i++) {
            segments[i] = MemorySegment.ofArray(values[i].getBytes(StandardCharsets.UTF_8));
        }
        return BinaryVector.materialized(segments, Validity.allValid(values.length));
    }

    private static Validity validity(int rows, int... validIndices) {
        BitSet bits = new BitSet(rows);
        for (int index : validIndices) {
            bits.set(index);
        }
        return Validity.of(bits, rows);
    }

    private static ParquetSchema schema(SchemaNode field) {
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(field), Optional.empty(), -1));
    }

    private static SchemaNode.Primitive leaf(String name, PrimitiveKind kind, Optional<LogicalType> logical) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), logical, 0);
    }

    private static SchemaNode.Group listGroup(String name, SchemaNode element) {
        SchemaNode.Group wrapper =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(wrapper), Optional.of(new LogicalType.ListType()), 1);
    }

    private static SchemaNode.Group structGroup(String name, SchemaNode... fields) {
        return new SchemaNode.Group(name, Repetition.OPTIONAL, List.of(fields), Optional.empty(), 1);
    }

    private static SchemaNode.Group mapGroup(String name, SchemaNode key, SchemaNode value) {
        SchemaNode.Group keyValue =
                new SchemaNode.Group("key_value", Repetition.REPEATED, List.of(key, value), Optional.empty(), -1);
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(keyValue), Optional.of(new LogicalType.MapType()), 1);
    }

    private static SchemaNode.Group variantGroup(String name) {
        SchemaNode.Primitive metadata = leaf("metadata", PrimitiveKind.BYTE_ARRAY, Optional.empty());
        SchemaNode.Primitive value = leaf("value", PrimitiveKind.BYTE_ARRAY, Optional.empty());
        return new SchemaNode.Group(
                name, Repetition.OPTIONAL, List.of(metadata, value), Optional.of(new LogicalType.Variant()), 1);
    }
}
