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
package io.tileverse.parquetry.data;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.LongPredicate;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.ReadFixtures;

/**
 * A literal of the physical type of a column compares with the stored cells, also where the statistics of the column
 * decode to its logical type: an integer on a timestamp column, bytes on a fixed-length decimal column. The count is
 * the same with and without pruning.
 *
 * <p>The file has 300 rows in three row groups of four pages: a timestamp column holding its row number, with a null
 * cell in one row out of four of the first row group, and a decimal column holding its row number in four big-endian
 * bytes.
 */
class PhysicalLiteralOnLogicalColumnTest {

    private static final ColumnPath TS = ColumnPath.of("ts");
    private static final ColumnPath AMOUNT = ColumnPath.of("amount");
    private static final int ROWS = 300;
    private static final int ROWS_PER_GROUP = 100;
    private static final int ROWS_PER_PAGE = 25;
    private static final int ROWS_PER_NULL = 4;

    @TempDir
    static Path tempDir;

    private static Path file;

    @BeforeAll
    static void writeFile() throws IOException {
        LogicalType timestamp = new LogicalType.Timestamp(true, LogicalType.TimeUnit.MICROS);
        SchemaNode.Primitive ts = WriteFixtures.optionalLeaf("ts", PrimitiveKind.INT64, OptionalInt.empty(), timestamp);
        SchemaNode.Primitive amount = WriteFixtures.requiredLeaf(
                "amount",
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                OptionalInt.of(Integer.BYTES),
                new LogicalType.Decimal(2, 9));
        ParquetSchema schema = WriteFixtures.schemaOf(ts, amount);
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .pageValueLimit(ROWS_PER_PAGE)
                .build();
        file = WriteFixtures.writeRows(tempDir.resolve("values.parquet"), schema, options, rows());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("integerComparisonsOfTheTimestampColumn")
    void integerLiteralOnATimestampColumnComparesWithTheStoredIntegers(
            String name, Predicate predicate, long expected) {
        ReadFixtures.assertCountWithAndWithoutPruning(file, predicate, expected);
    }

    static Stream<Arguments> integerComparisonsOfTheTimestampColumn() {
        Value five = new Value.LongVal(5L);
        Value hundred = new Value.LongVal(100L);
        Value oneFifty = new Value.LongVal(150L);
        return Stream.of(
                Arguments.of("ts = 5", new Predicate.Eq(TS, five), timestamps(cell -> cell == 5L)),
                Arguments.of("ts <> 5", new Predicate.NotEq(TS, five), timestamps(cell -> cell != 5L)),
                Arguments.of("ts < 100", new Predicate.Lt(TS, hundred), timestamps(cell -> cell < 100L)),
                Arguments.of("ts <= 100", new Predicate.LtEq(TS, hundred), timestamps(cell -> cell <= 100L)),
                Arguments.of("ts > 150", new Predicate.Gt(TS, oneFifty), timestamps(cell -> cell > 150L)),
                Arguments.of("ts >= 150", new Predicate.GtEq(TS, oneFifty), timestamps(cell -> cell >= 150L)),
                Arguments.of(
                        "ts IN (5, 150)",
                        new Predicate.In(TS, List.of(five, oneFifty)),
                        timestamps(cell -> cell == 5L || cell == 150L)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("bytesComparisonsOfTheDecimalColumn")
    void bytesLiteralOnADecimalColumnComparesWithTheStoredBytes(String name, Predicate predicate, long expected) {
        ReadFixtures.assertCountWithAndWithoutPruning(file, predicate, expected);
    }

    static Stream<Arguments> bytesComparisonsOfTheDecimalColumn() {
        Value five = new Value.BinaryVal(MemorySegment.ofArray(bigEndian(5)));
        return Stream.of(
                Arguments.of("amount = bytes of 5", new Predicate.Eq(AMOUNT, five), 1L),
                Arguments.of("amount <> bytes of 5", new Predicate.NotEq(AMOUNT, five), ROWS - 1L));
    }

    /** The number of non-null timestamp cells matching {@code test}. */
    private static long timestamps(LongPredicate test) {
        return LongStream.range(0, ROWS)
                .filter(row -> !hasNullTimestamp(row))
                .filter(test)
                .count();
    }

    private static boolean hasNullTimestamp(long row) {
        boolean inTheFirstRowGroup = row < ROWS_PER_GROUP;
        return inTheFirstRowGroup && row % ROWS_PER_NULL == 0;
    }

    private static List<Map<ColumnPath, Object>> rows() {
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int row = 0; row < ROWS; row++) {
            Map<ColumnPath, Object> cells = new HashMap<>();
            cells.put(AMOUNT, bigEndian(row));
            if (!hasNullTimestamp(row)) {
                cells.put(TS, (long) row);
            }
            rows.add(cells);
        }
        return rows;
    }

    private static byte[] bigEndian(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }
}
