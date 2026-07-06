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
package io.tileverse.parquetry.arrow.ipc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.Float16;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.BooleanVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.FixedLenBinaryVector;
import io.tileverse.parquetry.columnar.FloatVector;
import io.tileverse.parquetry.columnar.Int96Vector;
import io.tileverse.parquetry.columnar.IntSequence;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
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

    private static byte[] halfFloatLE(float value) {
        short bits = Float16.toFloat16(value);
        return ByteBuffer.allocate(2)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putShort(bits)
                .array();
    }

    @Test
    void roundTripsFloat16Column() throws Exception {
        SchemaNode.Primitive half = leaf(
                "half",
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                OptionalInt.of(2),
                Optional.of(new LogicalType.Float16Type()),
                0);
        ParquetSchema schema = schema(half);
        BitSet validBits = new BitSet();
        validBits.set(0);
        validBits.set(2);
        Validity validity = Validity.of(validBits, 3);
        FixedLenBinaryVector column =
                FixedLenBinaryVector.materialized(segments(halfFloatLE(1.5f), null, halfFloatLE(-2.25f)), 2, validity);

        byte[] ipc = writeSingleColumn(schema, "half", column, 3);

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            Float2Vector vector = (Float2Vector) root.getVector("half");
            assertThat(vector.getValueAsFloat(0)).isEqualTo(1.5f);
            assertThat(vector.isNull(1)).isTrue();
            assertThat(vector.getValueAsFloat(2)).isEqualTo(-2.25f);
        }
    }

    private static byte[] int96Bytes(long nanosOfDay, int julianDay) {
        return ByteBuffer.allocate(12)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(nanosOfDay)
                .putInt(julianDay)
                .array();
    }

    @Test
    void roundTripsInt96TimestampColumn() throws Exception {
        SchemaNode.Primitive ts = leaf("ts", PrimitiveKind.INT96, OptionalInt.empty(), Optional.empty(), 0);
        ParquetSchema schema = schema(ts);
        BitSet validBits = new BitSet();
        validBits.set(0);
        validBits.set(1);
        Validity validity = Validity.of(validBits, 3);
        Int96Vector column = Int96Vector.materialized(
                segments(int96Bytes(0L, 2_440_588), int96Bytes(1_000_000L, 2_440_589), null), validity);

        byte[] ipc = writeSingleColumn(schema, "ts", column, 3);

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            TimeStampMicroVector vector = (TimeStampMicroVector) root.getVector("ts");
            assertThat(vector.get(0)).isZero();
            assertThat(vector.get(1)).isEqualTo(86_400_000_000L + 1_000L);
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
                io.tileverse.parquetry.columnar.IntVector.materialized(new int[] {19000, 19500}, validity));
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
    void roundTripsUnsignedIntColumns() throws Exception {
        SchemaNode.Primitive u32 = leaf(
                "u32",
                PrimitiveKind.INT32,
                OptionalInt.empty(),
                Optional.of(new LogicalType.IntType((byte) 32, false)),
                0);
        SchemaNode.Primitive u64 = leaf(
                "u64",
                PrimitiveKind.INT64,
                OptionalInt.empty(),
                Optional.of(new LogicalType.IntType((byte) 64, false)),
                1);
        ParquetSchema schema = schema(u32, u64);
        BitSet validBits = new BitSet();
        validBits.set(0, 2);
        Validity validity = Validity.of(validBits, 2);
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        // 0xFFFFFFFF as a signed int is -1; an unsigned reader must see 4294967295.
        columns.put(
                ColumnPath.of("u32"),
                io.tileverse.parquetry.columnar.IntVector.materialized(new int[] {-1, 7}, validity));
        // 0xFFFFFFFFFFFFFFFF as a signed long is -1; an unsigned reader must see 18446744073709551615.
        columns.put(ColumnPath.of("u64"), LongVector.materialized(new long[] {-1L, 7L}, validity));
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 2, Arena.ofShared());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(batch), out);

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader =
                        new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            UInt4Vector u32Vector = (UInt4Vector) root.getVector("u32");
            UInt8Vector u64Vector = (UInt8Vector) root.getVector("u64");
            assertThat(u32Vector.getValueAsLong(0)).isEqualTo(4_294_967_295L);
            assertThat(u32Vector.getValueAsLong(1)).isEqualTo(7L);
            assertThat(u64Vector.getObjectNoOverflow(0)).isEqualTo(new BigInteger("18446744073709551615"));
            assertThat(u64Vector.getObjectNoOverflow(1)).isEqualTo(BigInteger.valueOf(7L));
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
                io.tileverse.parquetry.columnar.IntVector.materialized(new int[] {10, 20}, firstValidity));
        ParquetRecordBatch firstBatch = new DefaultParquetRecordBatch(schema, firstColumns, 2, Arena.ofShared());

        BitSet secondValidBits = new BitSet();
        secondValidBits.set(0, 3);
        Validity secondValidity = Validity.of(secondValidBits, 3);
        Map<ColumnPath, ColumnVector> secondColumns = new LinkedHashMap<>();
        secondColumns.put(
                ColumnPath.of("count"),
                io.tileverse.parquetry.columnar.IntVector.materialized(new int[] {30, 40, 50}, secondValidity));
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
        io.tileverse.parquetry.columnar.IntVector column =
                io.tileverse.parquetry.columnar.IntVector.materialized(new int[] {100, 200, 0, 0}, validity);

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

    private static byte[] bigEndian8(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    @Test
    void roundTripsDecimalColumnsFromAllCarriers() throws Exception {
        SchemaNode.Primitive d32 =
                leaf("d32", PrimitiveKind.INT32, OptionalInt.empty(), Optional.of(new LogicalType.Decimal(2, 9)), 0);
        SchemaNode.Primitive d64 =
                leaf("d64", PrimitiveKind.INT64, OptionalInt.empty(), Optional.of(new LogicalType.Decimal(2, 18)), 1);
        SchemaNode.Primitive dFixed = leaf(
                "dFixed",
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                OptionalInt.of(8),
                Optional.of(new LogicalType.Decimal(2, 18)),
                2);
        SchemaNode.Primitive dBytes = leaf(
                "dBytes",
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.Decimal(2, 10)),
                3);
        ParquetSchema schema = schema(d32, d64, dFixed, dBytes);

        BitSet validBits = new BitSet();
        validBits.set(0, 2);
        Validity validity = Validity.of(validBits, 2);
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(
                ColumnPath.of("d32"),
                io.tileverse.parquetry.columnar.IntVector.materialized(new int[] {12345, -678}, validity));
        columns.put(ColumnPath.of("d64"), LongVector.materialized(new long[] {123456789012L, -42L}, validity));
        columns.put(
                ColumnPath.of("dFixed"),
                FixedLenBinaryVector.materialized(segments(bigEndian8(123456789012L), bigEndian8(-42L)), 8, validity));
        columns.put(
                ColumnPath.of("dBytes"),
                BinaryVector.materialized(
                        segments(
                                BigInteger.valueOf(98765).toByteArray(),
                                BigInteger.valueOf(-1).toByteArray()),
                        validity));
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 2, Arena.ofShared());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(batch), out);

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader =
                        new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(((DecimalVector) root.getVector("d32")).getObject(0))
                    .isEqualByComparingTo(new BigDecimal("123.45"));
            assertThat(((DecimalVector) root.getVector("d32")).getObject(1))
                    .isEqualByComparingTo(new BigDecimal("-6.78"));
            assertThat(((DecimalVector) root.getVector("d64")).getObject(0))
                    .isEqualByComparingTo(new BigDecimal("1234567890.12"));
            assertThat(((DecimalVector) root.getVector("dFixed")).getObject(0))
                    .isEqualByComparingTo(new BigDecimal("1234567890.12"));
            assertThat(((DecimalVector) root.getVector("dFixed")).getObject(1))
                    .isEqualByComparingTo(new BigDecimal("-0.42"));
            assertThat(((DecimalVector) root.getVector("dBytes")).getObject(0))
                    .isEqualByComparingTo(new BigDecimal("987.65"));
            assertThat(((DecimalVector) root.getVector("dBytes")).getObject(1))
                    .isEqualByComparingTo(new BigDecimal("-0.01"));
        }
    }

    @Test
    void roundTripsDictionaryEncodedStringColumn() throws Exception {
        SchemaNode.Primitive name = leaf(
                "name", PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.of(new LogicalType.StringType()), 0);
        ParquetSchema schema = schema(name);
        MemorySegment red = MemorySegment.ofArray("red".getBytes(StandardCharsets.UTF_8));
        MemorySegment green = MemorySegment.ofArray("green".getBytes(StandardCharsets.UTF_8));
        MemorySegment[] dictEntries = {red, green};
        IntSequence indices = IntSequence.of(new int[] {0, 1, 0, 0});
        BitSet validBits = new BitSet();
        validBits.set(0);
        validBits.set(1);
        validBits.set(3);
        Validity validity = Validity.of(validBits, 4);
        BinaryVector column = BinaryVector.dictionary(dictEntries, indices, validity);

        byte[] ipc = writeSingleColumn(schema, "name", column, 4);

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getRowCount()).isEqualTo(4);
            VarCharVector vector = (VarCharVector) root.getVector("name");
            assertThat(new String(vector.get(0), StandardCharsets.UTF_8)).isEqualTo("red");
            assertThat(new String(vector.get(1), StandardCharsets.UTF_8)).isEqualTo("green");
            assertThat(vector.isNull(2)).isTrue();
            assertThat(new String(vector.get(3), StandardCharsets.UTF_8)).isEqualTo("red");
        }
    }
}
