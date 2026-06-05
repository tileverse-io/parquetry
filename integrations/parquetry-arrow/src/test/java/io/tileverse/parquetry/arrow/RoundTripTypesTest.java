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
package io.tileverse.parquetry.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.FixedLenBinaryVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Per-type round-trips through the canonical Arrow stream reader: each test writes a batch via {@link ArrowIpcWriter}
 * and reads it back with {@link ArrowStreamReader}, asserting both values and null positions survive the IPC encoding.
 */
class RoundTripTypesTest {

    private static ParquetSchema schema(SchemaNode.Primitive... leaves) {
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(leaves), Optional.empty(), -1));
    }

    private static SchemaNode.Primitive leaf(
            String name, PrimitiveKind kind, OptionalInt typeLength, Optional<LogicalType> logical, int fieldId) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, typeLength, logical, fieldId);
    }

    private static MemorySegment[] segments(byte[]... rows) {
        MemorySegment[] result = new MemorySegment[rows.length];
        for (int row = 0; row < rows.length; row++) {
            result[row] = rows[row] == null ? null : MemorySegment.ofArray(rows[row]);
        }
        return result;
    }

    private static byte[] writeSingleColumn(
            ParquetSchema schema, String columnName, ColumnVector column, int rowCount) {
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of(columnName), column);
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, rowCount, Arena.ofShared());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(batch), out);
        return out.toByteArray();
    }

    @Test
    void roundTripsBooleanColumnWithNull() throws Exception {
        SchemaNode.Primitive flag = leaf("flag", PrimitiveKind.BOOLEAN, OptionalInt.empty(), Optional.empty(), 0);
        ParquetSchema schema = schema(flag);
        BitSet validBits = new BitSet();
        validBits.set(0);
        validBits.set(2);
        Validity validity = Validity.of(validBits, 3);
        BooleanVector column = BooleanVector.materialized(new boolean[] {true, false, true}, validity);

        byte[] ipc = writeSingleColumn(schema, "flag", column, 3);

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getRowCount()).isEqualTo(3);
            BitVector vector = (BitVector) root.getVector("flag");
            assertThat(vector.get(0)).isEqualTo(1);
            assertThat(vector.isNull(1)).isTrue();
            assertThat(vector.get(2)).isEqualTo(1);
        }
    }

    @Test
    void roundTripsFloatAndDoubleColumns() throws Exception {
        SchemaNode.Primitive f = leaf("f", PrimitiveKind.FLOAT, OptionalInt.empty(), Optional.empty(), 0);
        SchemaNode.Primitive d = leaf("d", PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), 1);
        ParquetSchema schema = schema(f, d);
        BitSet validBits = new BitSet();
        validBits.set(0, 3);
        Validity validity = Validity.of(validBits, 3);
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("f"), FloatVector.materialized(new float[] {1.5f, 2.25f, 4.75f}, validity));
        columns.put(ColumnPath.of("d"), DoubleVector.materialized(new double[] {1.5, 2.25, 4.75}, validity));
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 3, Arena.ofShared());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(batch), out);

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader =
                        new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getRowCount()).isEqualTo(3);
            Float4Vector floats = (Float4Vector) root.getVector("f");
            Float8Vector doubles = (Float8Vector) root.getVector("d");
            assertThat(floats.get(0)).isEqualTo(1.5f);
            assertThat(floats.get(1)).isEqualTo(2.25f);
            assertThat(floats.get(2)).isEqualTo(4.75f);
            assertThat(doubles.get(0)).isEqualTo(1.5);
            assertThat(doubles.get(1)).isEqualTo(2.25);
            assertThat(doubles.get(2)).isEqualTo(4.75);
        }
    }

    @Test
    void roundTripsUtf8ColumnWithNull() throws Exception {
        SchemaNode.Primitive name = leaf(
                "name", PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.of(new LogicalType.StringType()), 0);
        ParquetSchema schema = schema(name);
        BitSet validBits = new BitSet();
        validBits.set(0);
        validBits.set(2);
        Validity validity = Validity.of(validBits, 3);
        BinaryVector column = BinaryVector.materialized(
                segments("alpha".getBytes(StandardCharsets.UTF_8), null, "gamma".getBytes(StandardCharsets.UTF_8)),
                validity);

        byte[] ipc = writeSingleColumn(schema, "name", column, 3);

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getRowCount()).isEqualTo(3);
            VarCharVector vector = (VarCharVector) root.getVector("name");
            assertThat(new String(vector.get(0), StandardCharsets.UTF_8)).isEqualTo("alpha");
            assertThat(vector.isNull(1)).isTrue();
            assertThat(new String(vector.get(2), StandardCharsets.UTF_8)).isEqualTo("gamma");
        }
    }

    @Test
    void roundTripsBinaryColumnWithNull() throws Exception {
        SchemaNode.Primitive blob = leaf("blob", PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), 0);
        ParquetSchema schema = schema(blob);
        byte[] first = {1, 2, 3};
        byte[] third = {9, 8, 7, 6};
        BitSet validBits = new BitSet();
        validBits.set(0);
        validBits.set(2);
        Validity validity = Validity.of(validBits, 3);
        BinaryVector column = BinaryVector.materialized(segments(first, null, third), validity);

        byte[] ipc = writeSingleColumn(schema, "blob", column, 3);

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getRowCount()).isEqualTo(3);
            VarBinaryVector vector = (VarBinaryVector) root.getVector("blob");
            assertThat(vector.get(0)).containsExactly(1, 2, 3);
            assertThat(vector.isNull(1)).isTrue();
            assertThat(vector.get(2)).containsExactly(9, 8, 7, 6);
        }
    }

    @Test
    void roundTripsFixedSizeBinaryColumnWithNull() throws Exception {
        int width = 4;
        SchemaNode.Primitive key =
                leaf("key", PrimitiveKind.FIXED_LEN_BYTE_ARRAY, OptionalInt.of(width), Optional.empty(), 0);
        ParquetSchema schema = schema(key);
        byte[] first = {10, 11, 12, 13};
        byte[] second = {20, 21, 22, 23};
        BitSet validBits = new BitSet();
        validBits.set(0);
        validBits.set(1);
        Validity validity = Validity.of(validBits, 3);
        FixedLenBinaryVector column = FixedLenBinaryVector.materialized(segments(first, second, null), width, validity);

        byte[] ipc = writeSingleColumn(schema, "key", column, 3);

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getRowCount()).isEqualTo(3);
            FixedSizeBinaryVector vector = (FixedSizeBinaryVector) root.getVector("key");
            assertThat(vector.get(0)).containsExactly(10, 11, 12, 13);
            assertThat(vector.get(1)).containsExactly(20, 21, 22, 23);
            assertThat(vector.isNull(2)).isTrue();
        }
    }

    @Test
    void roundTripsDateAndTimestampColumns() throws Exception {
        SchemaNode.Primitive day =
                leaf("day", PrimitiveKind.INT32, OptionalInt.empty(), Optional.of(new LogicalType.DateType()), 0);
        SchemaNode.Primitive ts = leaf(
                "ts",
                PrimitiveKind.INT64,
                OptionalInt.empty(),
                Optional.of(new LogicalType.Timestamp(true, LogicalType.TimeUnit.MICROS)),
                1);
        ParquetSchema schema = schema(day, ts);
        BitSet validBits = new BitSet();
        validBits.set(0, 2);
        Validity validity = Validity.of(validBits, 2);
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(
                ColumnPath.of("day"),
                io.tileverse.parquetry.batch.IntVector.materialized(new int[] {19000, 19500}, validity));
        columns.put(
                ColumnPath.of("ts"),
                LongVector.materialized(new long[] {1_700_000_000_000_000L, 1_700_000_001_000_000L}, validity));
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 2, Arena.ofShared());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(batch), out);

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader =
                        new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getRowCount()).isEqualTo(2);
            DateDayVector dates = (DateDayVector) root.getVector("day");
            assertThat(dates.get(0)).isEqualTo(19000);
            assertThat(dates.get(1)).isEqualTo(19500);
            TimeStampMicroTZVector timestamps = (TimeStampMicroTZVector) root.getVector("ts");
            assertThat(timestamps.get(0)).isEqualTo(1_700_000_000_000_000L);
            assertThat(timestamps.get(1)).isEqualTo(1_700_000_001_000_000L);
        }
    }

    @Test
    void roundTripsMultipleBatchesForOneSchema() throws Exception {
        SchemaNode.Primitive count = leaf("count", PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 0);
        ParquetSchema schema = schema(count);

        BitSet firstValidBits = new BitSet();
        firstValidBits.set(0, 2);
        Validity firstValidity = Validity.of(firstValidBits, 2);
        Map<ColumnPath, ColumnVector> firstColumns = new LinkedHashMap<>();
        firstColumns.put(
                ColumnPath.of("count"),
                io.tileverse.parquetry.batch.IntVector.materialized(new int[] {10, 20}, firstValidity));
        ParquetRecordBatch firstBatch = new DefaultParquetRecordBatch(schema, firstColumns, 2, Arena.ofShared());

        BitSet secondValidBits = new BitSet();
        secondValidBits.set(0, 3);
        Validity secondValidity = Validity.of(secondValidBits, 3);
        Map<ColumnPath, ColumnVector> secondColumns = new LinkedHashMap<>();
        secondColumns.put(
                ColumnPath.of("count"),
                io.tileverse.parquetry.batch.IntVector.materialized(new int[] {30, 40, 50}, secondValidity));
        ParquetRecordBatch secondBatch = new DefaultParquetRecordBatch(schema, secondColumns, 3, Arena.ofShared());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(firstBatch, secondBatch), out);

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader =
                        new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getRowCount()).isEqualTo(2);
            IntVector firstVector = (IntVector) root.getVector("count");
            assertThat(firstVector.get(0)).isEqualTo(10);
            assertThat(firstVector.get(1)).isEqualTo(20);

            assertThat(reader.loadNextBatch()).isTrue();
            assertThat(root.getRowCount()).isEqualTo(3);
            IntVector secondVector = (IntVector) root.getVector("count");
            assertThat(secondVector.get(0)).isEqualTo(30);
            assertThat(secondVector.get(1)).isEqualTo(40);
            assertThat(secondVector.get(2)).isEqualTo(50);

            assertThat(reader.loadNextBatch()).isFalse();
        }
    }

    @Test
    void roundTripsColumnWhoseTrailingRowsAreAllNull() throws Exception {
        SchemaNode.Primitive value = leaf("value", PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 0);
        ParquetSchema schema = schema(value);
        BitSet validBits = new BitSet();
        validBits.set(0);
        validBits.set(1);
        Validity validity = Validity.of(validBits, 4);
        io.tileverse.parquetry.batch.IntVector column =
                io.tileverse.parquetry.batch.IntVector.materialized(new int[] {100, 200, 0, 0}, validity);

        byte[] ipc = writeSingleColumn(schema, "value", column, 4);

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getRowCount()).isEqualTo(4);
            IntVector vector = (IntVector) root.getVector("value");
            assertThat(vector.get(0)).isEqualTo(100);
            assertThat(vector.get(1)).isEqualTo(200);
            assertThat(vector.isNull(2)).isTrue();
            assertThat(vector.isNull(3)).isTrue();
        }
    }
}
