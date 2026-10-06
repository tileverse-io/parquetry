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

import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.UUID;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.FloatColumnOrder;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Fixtures for write tests: schemas and their leaf columns, a one-batch {@link ParquetRecordBatch} or a file written
 * from rows expressed as per-leaf value maps, and the cells of the columns compared by the statistics accumulators.
 */
public final class WriteFixtures {

    /** The byte length of a FLOAT16 cell. */
    public static final int HALF_FLOAT_BYTES = HalfFloats.BYTES;

    /** A required FLOAT leaf without annotation. */
    public static final SchemaNode.Primitive FLOAT_LEAF = leaf(PrimitiveKind.FLOAT, null);

    /** A required DOUBLE leaf without annotation. */
    public static final SchemaNode.Primitive DOUBLE_LEAF = leaf(PrimitiveKind.DOUBLE, null);

    /** A required FLOAT16 leaf. */
    public static final SchemaNode.Primitive HALF_FLOAT_LEAF =
            leaf(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, new LogicalType.Float16Type());

    /** The bits of quiet FLOAT NaNs of either sign, without a payload and with one. */
    public static final int FLOAT_NAN = 0x7FC00000;

    public static final int FLOAT_NAN_WITH_PAYLOAD = 0x7FC00001;
    public static final int FLOAT_NEGATIVE_NAN = 0xFFC00000;
    public static final int FLOAT_NEGATIVE_NAN_WITH_PAYLOAD = 0xFFC00001;

    /** The bits of quiet DOUBLE NaNs. */
    public static final long DOUBLE_NAN = 0x7FF8000000000000L;

    public static final long DOUBLE_NAN_WITH_PAYLOAD = 0x7FF8000000000001L;
    public static final long DOUBLE_NEGATIVE_NAN = 0xFFF8000000000000L;

    /** The bits of quiet FLOAT16 NaNs. */
    public static final short HALF_NAN = (short) 0x7E00;

    public static final short HALF_NAN_WITH_PAYLOAD = (short) 0x7E01;
    public static final short HALF_NEGATIVE_NAN = (short) 0xFE00;

    private WriteFixtures() {}

    /**
     * A required leaf of {@code kind} annotated with {@code logicalType}, or with nothing when it is {@code null}. A
     * FLOAT16 annotation gives the leaf its two-byte type length; any other leaf has none.
     */
    public static SchemaNode.Primitive leaf(PrimitiveKind kind, LogicalType logicalType) {
        boolean halfFloat = logicalType instanceof LogicalType.Float16Type;
        OptionalInt typeLength = halfFloat ? OptionalInt.of(HALF_FLOAT_BYTES) : OptionalInt.empty();
        return leaf(kind, typeLength, logicalType);
    }

    /** A required {@code FIXED_LEN_BYTE_ARRAY} leaf of {@code typeLength} bytes annotated with {@code logicalType}. */
    public static SchemaNode.Primitive fixedLeaf(int typeLength, LogicalType logicalType) {
        return leaf(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, OptionalInt.of(typeLength), logicalType);
    }

    private static SchemaNode.Primitive leaf(PrimitiveKind kind, OptionalInt typeLength, LogicalType logicalType) {
        return requiredLeaf("column", kind, typeLength, logicalType);
    }

    /** A required leaf column without annotation. */
    public static SchemaNode.Primitive requiredLeaf(String name, PrimitiveKind kind) {
        return requiredLeaf(name, kind, OptionalInt.empty(), null);
    }

    /** A required leaf column, annotated with {@code logicalType} unless it is {@code null}. */
    public static SchemaNode.Primitive requiredLeaf(
            String name, PrimitiveKind kind, OptionalInt typeLength, LogicalType logicalType) {
        return leaf(name, Repetition.REQUIRED, kind, typeLength, logicalType);
    }

    /** A nullable leaf column without annotation. */
    public static SchemaNode.Primitive optionalLeaf(String name, PrimitiveKind kind) {
        return optionalLeaf(name, kind, OptionalInt.empty(), null);
    }

    /** A nullable leaf column, annotated with {@code logicalType} unless it is {@code null}. */
    public static SchemaNode.Primitive optionalLeaf(
            String name, PrimitiveKind kind, OptionalInt typeLength, LogicalType logicalType) {
        return leaf(name, Repetition.OPTIONAL, kind, typeLength, logicalType);
    }

    private static SchemaNode.Primitive leaf(
            String name, Repetition repetition, PrimitiveKind kind, OptionalInt typeLength, LogicalType logicalType) {
        return new SchemaNode.Primitive(name, repetition, kind, typeLength, Optional.ofNullable(logicalType), -1);
    }

    /** A schema of the given top-level columns under a {@code message schema { ... }} root. */
    public static ParquetSchema schemaOf(SchemaNode... columns) {
        return schemaOf(List.of(columns));
    }

    /** A schema of the given top-level columns under a {@code message schema { ... }} root. */
    public static ParquetSchema schemaOf(List<SchemaNode> columns) {
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, columns, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /**
     * A fresh statistics accumulator for a required leaf of {@code kind} annotated with {@code logicalType}, or with
     * nothing when it is {@code null}, in the type-defined order of the leaf.
     */
    public static StatisticsAccumulator accumulator(PrimitiveKind kind, LogicalType logicalType) {
        return accumulator(leaf(kind, logicalType));
    }

    /** A fresh statistics accumulator for {@code leaf} in its type-defined order. */
    public static StatisticsAccumulator accumulator(SchemaNode.Primitive leaf) {
        return StatisticsAccumulator.forColumn(leaf, BoundsOrder.of(leaf));
    }

    /**
     * A fresh statistics accumulator for {@code leaf} in a file declaring {@code floatOrder} for its floating-point
     * columns.
     */
    public static StatisticsAccumulator accumulator(SchemaNode.Primitive leaf, FloatColumnOrder floatOrder) {
        return StatisticsAccumulator.forColumn(leaf, BoundsOrder.of(leaf, floatOrder));
    }

    /** The cell of a FLOAT16 column holding {@code value}. */
    public static MemorySegment halfFloat(float value) {
        return MemorySegment.ofArray(halfFloatBytes(Float.floatToFloat16(value)))
                .asReadOnly();
    }

    /** The cell of a FLOAT16 column holding the half float with the given bits. */
    public static MemorySegment halfFloatCell(short bits) {
        return MemorySegment.ofArray(halfFloatBytes(bits)).asReadOnly();
    }

    /** The two little-endian bytes of the half float with the given bits. */
    public static byte[] halfFloatBytes(short bits) {
        return new byte[] {(byte) bits, (byte) (bits >> Byte.SIZE)};
    }

    /** The bits of the half float held by the two little-endian bytes of {@code cell}. */
    public static short halfFloatBits(MemorySegment cell) {
        int low = cell.get(JAVA_BYTE, 0) & 0xFF;
        int high = cell.get(JAVA_BYTE, 1) & 0xFF;
        return (short) (high << Byte.SIZE | low);
    }

    /** A quiet FLOAT NaN with a random sign and payload. */
    public static float randomFloatNaN(Random random) {
        return Float.intBitsToFloat(0x7FC00000 | (random.nextInt() & 0x803FFFFF));
    }

    /** A quiet DOUBLE NaN with a random sign and payload. */
    public static double randomDoubleNaN(Random random) {
        return Double.longBitsToDouble(0x7FF8000000000000L | (random.nextLong() & 0x8007FFFFFFFFFFFFL));
    }

    /** The bits of a quiet FLOAT16 NaN with a random sign and payload. */
    public static short randomHalfFloatNaN(Random random) {
        return (short) (0x7E00 | (random.nextInt() & 0x81FF));
    }

    /** The bits of the PLAIN-encoded FLOAT {@code bound}. */
    public static int floatBits(MemorySegment bound) {
        return bound.get(INT32, 0);
    }

    /** The bits of the PLAIN-encoded DOUBLE {@code bound}. */
    public static long doubleBits(MemorySegment bound) {
        return bound.get(INT64, 0);
    }

    /** {@code number} as {@code length} big-endian two's complement bytes, extended on the left with its sign. */
    public static byte[] signedBytes(BigInteger number, int length) {
        byte[] minimal = number.toByteArray();
        byte[] extended = new byte[length];
        Arrays.fill(extended, (byte) (number.signum() < 0 ? 0xFF : 0x00));
        System.arraycopy(minimal, 0, extended, length - minimal.length, minimal.length);
        return extended;
    }

    /** Writes {@code rows} to the file {@code target}; a row names its non-null cells by leaf column. */
    public static Path writeRows(
            Path target, ParquetSchema schema, WriteOptions options, List<Map<ColumnPath, Object>> rows)
            throws IOException {
        try (OutputStream out = Files.newOutputStream(target);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(1);
            for (Map<ColumnPath, Object> row : rows) {
                appendRow(appender, schema, row);
            }
        }
        return target;
    }

    public static ParquetRecordBatch batch(ParquetSchema schema, List<Map<ColumnPath, Object>> rows) {
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.forSchema(schema);
        for (Map<ColumnPath, Object> row : rows) {
            appendRow(builder, schema, row);
        }
        return builder.build();
    }

    /**
     * Stages one row's cells onto an existing builder and closes the row. Lets writer-bound appenders reuse the same
     * value-to-setter dispatch as {@link #batch} while keeping heap bounded for streaming producers.
     */
    public static void appendRow(ParquetRecordBatchBuilder builder, ParquetSchema schema, Map<ColumnPath, Object> row) {
        List<ColumnPath> leaves = schema.leafColumns();
        for (int i = 0; i < leaves.size(); i++) {
            setCell(builder, i, row.get(leaves.get(i)));
        }
        builder.endRow();
    }

    private static void setCell(ParquetRecordBatchBuilder builder, int col, Object value) {
        switch (value) {
            case null -> builder.setNull(col);
            case Boolean booleanValue -> builder.setBoolean(col, booleanValue);
            case Integer intValue -> builder.setInt(col, intValue);
            case Long longValue -> builder.setLong(col, longValue);
            case Float floatValue -> builder.setFloat(col, floatValue);
            case Double doubleValue -> builder.setDouble(col, doubleValue);
            case String stringValue -> builder.setString(col, stringValue);
            case UUID uuidValue -> builder.setUuid(col, uuidValue);
            case MemorySegment segmentValue -> builder.setBinary(col, segmentValue);
            case byte[] bytesValue -> builder.setBinary(col, MemorySegment.ofArray(bytesValue));
            default -> throw new IllegalArgumentException("Unsupported test value type: " + value.getClass());
        }
    }
}
