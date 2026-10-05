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
package io.tileverse.parquetry.internal.write.conformance;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Random;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.FloatColumnOrder;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * The rows shared by the floating-point conformance ITs and the two writers producing files of them, in the
 * type-defined order or in IEEE 754 total order. The nullable FLOAT, DOUBLE and FLOAT16 columns hold the same values
 * over five row groups of four pages each:
 *
 * <ol>
 *   <li>numbers, with one page of nulls;
 *   <li>numbers mixed with NaN cells and nulls in each page;
 *   <li>numbers, with one page holding nothing but NaN;
 *   <li>NaN only;
 *   <li>zeros: a page of {@code +0.0}, a page of {@code -0.0}, a page of zeros and positive numbers, a page of zeros
 *       and negative numbers.
 * </ol>
 */
final class FloatConformanceFixture {

    static final int GROUPS = 5;
    static final int ROWS_PER_GROUP = 400;
    static final int ROWS_PER_PAGE = 100;
    static final int NUMBERS_GROUP = 0;
    static final int MIXED_GROUP = 1;
    static final int NAN_PAGE_GROUP = 2;
    static final int NAN_GROUP = 3;
    static final int ZEROS_GROUP = 4;
    static final int NULL_PAGE_OF_NUMBERS_GROUP = 1;
    static final int NAN_PAGE_OF_NAN_PAGE_GROUP = 2;
    static final int POSITIVE_ZERO_PAGE_OF_ZEROS_GROUP = 0;
    static final int NEGATIVE_ZERO_PAGE_OF_ZEROS_GROUP = 1;
    static final int ZEROS_AND_POSITIVE_NUMBERS_PAGE_OF_ZEROS_GROUP = 2;
    static final int ZEROS_AND_NEGATIVE_NUMBERS_PAGE_OF_ZEROS_GROUP = 3;

    static final String ID = "id";
    static final String F = "f";
    static final String D = "d";
    static final String H = "h";
    static final List<String> FLOATING = List.of(F, D, H);

    /** The PLAIN bytes of the two FLOAT zeros. */
    static final byte[] FLOAT_NEGATIVE_ZERO = {0x00, 0x00, 0x00, (byte) 0x80};

    static final byte[] FLOAT_POSITIVE_ZERO = {0x00, 0x00, 0x00, 0x00};

    private static final long SEED = 20261004L;

    private FloatConformanceFixture() {}

    /**
     * One generated row. Its three floating-point cells are null together, and {@code h} holds the bits of a half
     * float.
     */
    record Row(int id, boolean isNull, float f, double d, short h) {

        static Row ofNulls(int id) {
            return new Row(id, true, 0f, 0d, (short) 0);
        }

        static Row of(int id, float f, double d, short h) {
            return new Row(id, false, f, d, h);
        }
    }

    static List<Row> generateRows() {
        Random random = new Random(SEED);
        List<Row> generated = new ArrayList<>(GROUPS * ROWS_PER_GROUP);
        for (int id = 0; id < GROUPS * ROWS_PER_GROUP; id++) {
            int group = id / ROWS_PER_GROUP;
            int page = (id % ROWS_PER_GROUP) / ROWS_PER_PAGE;
            generated.add(row(id, cellKind(group, page, random), random));
        }
        return generated;
    }

    /** The rows of row group {@code group}. */
    static List<Row> rowsOf(List<Row> rows, int group) {
        return rows.subList(group * ROWS_PER_GROUP, (group + 1) * ROWS_PER_GROUP);
    }

    private enum CellKind {
        NUMBER,
        POSITIVE_NUMBER,
        NEGATIVE_NUMBER,
        NAN,
        POSITIVE_ZERO,
        NEGATIVE_ZERO,
        NULL
    }

    private static CellKind cellKind(int group, int page, Random random) {
        return switch (group) {
            case NUMBERS_GROUP -> page == NULL_PAGE_OF_NUMBERS_GROUP ? CellKind.NULL : CellKind.NUMBER;
            case MIXED_GROUP -> mixedCellKind(random);
            case NAN_PAGE_GROUP -> page == NAN_PAGE_OF_NAN_PAGE_GROUP ? CellKind.NAN : CellKind.NUMBER;
            case NAN_GROUP -> CellKind.NAN;
            case ZEROS_GROUP -> zerosCellKind(page, random);
            default -> throw new IllegalArgumentException("the fixture has no row group " + group);
        };
    }

    private static CellKind zerosCellKind(int page, Random random) {
        CellKind eitherZero = random.nextBoolean() ? CellKind.POSITIVE_ZERO : CellKind.NEGATIVE_ZERO;
        boolean number = random.nextInt(4) == 0;
        return switch (page) {
            case POSITIVE_ZERO_PAGE_OF_ZEROS_GROUP -> CellKind.POSITIVE_ZERO;
            case NEGATIVE_ZERO_PAGE_OF_ZEROS_GROUP -> CellKind.NEGATIVE_ZERO;
            case ZEROS_AND_POSITIVE_NUMBERS_PAGE_OF_ZEROS_GROUP -> number ? CellKind.POSITIVE_NUMBER : eitherZero;
            case ZEROS_AND_NEGATIVE_NUMBERS_PAGE_OF_ZEROS_GROUP -> number ? CellKind.NEGATIVE_NUMBER : eitherZero;
            default -> throw new IllegalArgumentException("the row group of zeros has no page " + page);
        };
    }

    private static CellKind mixedCellKind(Random random) {
        int draw = random.nextInt(10);
        if (draw < 3) {
            return CellKind.NAN;
        }
        return draw == 3 ? CellKind.NULL : CellKind.NUMBER;
    }

    private static Row row(int id, CellKind kind, Random random) {
        return switch (kind) {
            case NUMBER -> numberRow(id, random.nextFloat(-100f, 100f));
            case POSITIVE_NUMBER -> numberRow(id, random.nextFloat(1f, 100f));
            case NEGATIVE_NUMBER -> numberRow(id, random.nextFloat(-100f, -1f));
            case NAN -> nanRow(id, random);
            case POSITIVE_ZERO -> numberRow(id, 0.0f);
            case NEGATIVE_ZERO -> numberRow(id, -0.0f);
            case NULL -> Row.ofNulls(id);
        };
    }

    /** A row holding {@code number} in its three floating-point cells, as exactly as each type allows. */
    private static Row numberRow(int id, float number) {
        double asDouble = number;
        return Row.of(id, number, asDouble, Float.floatToFloat16(number));
    }

    /** A row holding a NaN of a random sign and payload in each of its three floating-point cells. */
    private static Row nanRow(int id, Random random) {
        float f = WriteFixtures.randomFloatNaN(random);
        double d = WriteFixtures.randomDoubleNaN(random);
        short h = WriteFixtures.randomHalfFloatNaN(random);
        return Row.of(id, f, d, h);
    }

    // --- parquetry ---

    /**
     * Writes {@code rows} with parquetry in the type-defined order, a row group and a page of the fixture sizes at a
     * time.
     */
    static Path writeWithParquetry(Path target, Path tempDir, List<Row> rows) throws IOException {
        return writeWithParquetry(target, tempDir, rows, FloatColumnOrder.TYPE_DEFINED);
    }

    /** Writes {@code rows} with parquetry, with the statistics of the floating-point columns in {@code floatOrder}. */
    static Path writeWithParquetry(Path target, Path tempDir, List<Row> rows, FloatColumnOrder floatOrder)
            throws IOException {
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .pageValueLimit(ROWS_PER_PAGE)
                .floatColumnOrder(floatOrder)
                .build();
        List<Map<ColumnPath, Object>> cells =
                rows.stream().map(FloatConformanceFixture::cells).toList();
        return WriteFixtures.writeRows(target, schema(), options, cells);
    }

    private static Map<ColumnPath, Object> cells(Row row) {
        Map<ColumnPath, Object> cells = new HashMap<>();
        cells.put(ColumnPath.of(ID), row.id());
        if (!row.isNull()) {
            cells.put(ColumnPath.of(F), row.f());
            cells.put(ColumnPath.of(D), row.d());
            cells.put(ColumnPath.of(H), WriteFixtures.halfFloatBytes(row.h()));
        }
        return cells;
    }

    private static ParquetSchema schema() {
        OptionalInt halfFloatBytes = OptionalInt.of(WriteFixtures.HALF_FLOAT_BYTES);
        return WriteFixtures.schemaOf(
                WriteFixtures.requiredLeaf(ID, PrimitiveKind.INT32),
                WriteFixtures.optionalLeaf(F, PrimitiveKind.FLOAT),
                WriteFixtures.optionalLeaf(D, PrimitiveKind.DOUBLE),
                WriteFixtures.optionalLeaf(
                        H, PrimitiveKind.FIXED_LEN_BYTE_ARRAY, halfFloatBytes, new LogicalType.Float16Type()));
    }

    // --- parquet-java ---

    /**
     * Writes each row group of the fixture as its own file with the parquet-java writer, with {@code floatOrder}
     * declared for the floating-point columns. The files are named after {@code prefix} and their row group.
     */
    static List<Path> writeReferences(Path dir, String prefix, List<Row> rows, FloatColumnOrder floatOrder)
            throws IOException {
        List<Path> references = new ArrayList<>();
        for (int group = 0; group < GROUPS; group++) {
            Path reference = dir.resolve(prefix + "-" + group + ".parquet");
            references.add(writeWithParquetJava(reference, rowsOf(rows, group), floatOrder));
        }
        return references;
    }

    /**
     * Writes {@code groupRows} as one row group with the parquet-java writer, paged like the parquetry file, with
     * {@code floatOrder} declared for the floating-point columns.
     */
    private static Path writeWithParquetJava(Path target, List<Row> groupRows, FloatColumnOrder floatOrder)
            throws IOException {
        MessageType schema = parquetJavaSchema(parquetJavaOrder(floatOrder));
        SimpleGroupFactory groups = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(target))
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withPageRowCountLimit(ROWS_PER_PAGE)
                .withDictionaryEncoding(false)
                .build()) {
            for (Row row : groupRows) {
                writer.write(group(groups, row));
            }
        }
        return target;
    }

    /** The parquet-java column order matching {@code floatOrder}. */
    static ColumnOrder parquetJavaOrder(FloatColumnOrder floatOrder) {
        return switch (floatOrder) {
            case TYPE_DEFINED -> ColumnOrder.typeDefined();
            case IEEE_754_TOTAL_ORDER -> ColumnOrder.ieee754TotalOrder();
        };
    }

    private static MessageType parquetJavaSchema(ColumnOrder floatOrder) {
        return Types.buildMessage()
                .required(PrimitiveTypeName.INT32)
                .named(ID)
                .optional(PrimitiveTypeName.FLOAT)
                .columnOrder(floatOrder)
                .named(F)
                .optional(PrimitiveTypeName.DOUBLE)
                .columnOrder(floatOrder)
                .named(D)
                .optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                .length(WriteFixtures.HALF_FLOAT_BYTES)
                .as(LogicalTypeAnnotation.float16Type())
                .columnOrder(floatOrder)
                .named(H)
                .named("schema");
    }

    private static Group group(SimpleGroupFactory groups, Row row) {
        Group group = groups.newGroup().append(ID, row.id());
        if (row.isNull()) {
            return group;
        }
        return group.append(F, row.f())
                .append(D, row.d())
                .append(H, Binary.fromConstantByteArray(WriteFixtures.halfFloatBytes(row.h())));
    }

    /** The fixture row held by a group read back by parquet-java. */
    static Row rowOf(Group group) {
        int id = group.getInteger(ID, 0);
        if (group.getFieldRepetitionCount(F) == 0) {
            return Row.ofNulls(id);
        }
        return Row.of(
                id,
                group.getFloat(F, 0),
                group.getDouble(D, 0),
                group.getBinary(H, 0).get2BytesLittleEndian());
    }
}
