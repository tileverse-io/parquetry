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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.BoundaryOrder;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.testsupport.ReadFixtures;

/**
 * The statistics written for columns ordered by their logical type hold up under the parquet-java reader: binary
 * decimals in signed order, unsigned integers in unsigned order, half floats by value. The footer statistics and page
 * bounds read by parquet-java enclose the cells of each row group and page, and its filtered reads prune by them and
 * return the matching rows.
 */
@Tag("conformance")
class StatisticsOrderConformanceIT {

    private static final int ROWS = 2_000;
    private static final int ROWS_PER_GROUP = 500;
    private static final int ROWS_PER_PAGE = 100;
    private static final int DECIMAL_BYTES = 9;
    private static final int DECIMAL_SCALE = 2;
    private static final int DECIMAL_PRECISION = 20;

    private static final String ID = "id";
    private static final String AMOUNT = "amount";
    private static final String BALANCE = "balance";
    private static final String U32 = "u32";
    private static final String U64 = "u64";
    private static final String HALF = "half";

    @TempDir
    Path tempDir;

    private List<Row> rows;
    private Path file;

    /**
     * One generated row. {@code amount} is a decimal's unscaled value written as fixed-length bytes, {@code balance}
     * the same kind of value written in its fewest bytes.
     */
    private record Row(int id, long amount, long balance, int u32, long u64, short half) {

        float halfValue() {
            return Float.float16ToFloat(half);
        }
    }

    /** The rows expected from a filtered read. */
    @FunctionalInterface
    private interface Matching {
        boolean test(Row row);
    }

    /** A parquet-java filter and the test telling the rows expected from it. */
    private record FilterCase(FilterPredicate filter, Matching matching) {}

    @BeforeEach
    void writeRandomRows() throws IOException {
        rows = randomRows(new Random(20261004L));
        file = write(tempDir.resolve("random.parquet"), rows);
    }

    @Test
    void footerBoundsEncloseEachRowGroup() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            List<BlockMetaData> rowGroups = reader.getFooter().getBlocks();
            assertThat(rowGroups).hasSize(ROWS / ROWS_PER_GROUP);

            for (int group = 0; group < rowGroups.size(); group++) {
                List<Row> groupRows = rows.subList(group * ROWS_PER_GROUP, (group + 1) * ROWS_PER_GROUP);
                for (ColumnChunkMetaData chunk : rowGroups.get(group).getColumns()) {
                    assertChunkBoundsEnclose(chunk, groupRows, "row group " + group);
                }
            }
        }
    }

    @Test
    void pageBoundsEncloseEachPage() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            List<BlockMetaData> rowGroups = reader.getFooter().getBlocks();
            for (int group = 0; group < rowGroups.size(); group++) {
                for (ColumnChunkMetaData chunk : rowGroups.get(group).getColumns()) {
                    ColumnIndex index = reader.readColumnIndex(chunk);
                    assertThat(index).as("column index of %s", chunk.getPath()).isNotNull();
                    assertPageBoundsEnclose(chunk, index, group * ROWS_PER_GROUP);
                }
            }
        }
    }

    @Test
    void parquetJavaFilteredReadsReturnTheMatchingRows() throws IOException {
        assertFilteredReadsMatch(file, rows);
    }

    @Test
    void parquetJavaFilteredReadsReturnTheMatchingRowsOfSortedColumns() throws IOException {
        for (Column column : Column.ORDERED) {
            List<Row> sorted = new ArrayList<>(rows);
            sorted.sort(column.order());
            Path sortedFile = write(tempDir.resolve("sorted-" + column.name() + ".parquet"), sorted);

            assertThat(boundaryOrders(sortedFile, column.name()))
                    .as("boundary order of sorted column %s", column.name())
                    .containsOnly(BoundaryOrder.ASCENDING);
            assertFilteredReadsMatch(sortedFile, sorted);
        }
    }

    @Test
    void parquetryPrunesByItsOwnDecimalBounds() {
        Value thousand = new Value.DecimalVal(new BigDecimal("1000.00"));
        Value minusThousand = new Value.DecimalVal(new BigDecimal("-1000.00"));
        ColumnPath amount = ColumnPath.of(AMOUNT);

        assertParquetryCount(new Predicate.Gt(amount, thousand), row -> row.amount() > 100_000L);
        assertParquetryCount(new Predicate.Lt(amount, minusThousand), row -> row.amount() < -100_000L);
        assertParquetryCount(
                new Predicate.GtEq(amount, minusThousand).and(new Predicate.LtEq(amount, thousand)),
                row -> row.amount() >= -100_000L && row.amount() <= 100_000L);
    }

    // --- parquet-java assertions ---

    private void assertChunkBoundsEnclose(ColumnChunkMetaData chunk, List<Row> chunkRows, String where) {
        Column column = Column.named(chunk.getPath().toDotString());
        Statistics<?> statistics = chunk.getStatistics();
        assertThat(statistics.hasNonNullValue())
                .as("%s of %s has bounds", column.name(), where)
                .isTrue();

        assertThat(column.decode(statistics.getMinBytes()))
                .as("min of %s in %s", column.name(), where)
                .isEqualTo(column.least(chunkRows));
        assertThat(column.decode(statistics.getMaxBytes()))
                .as("max of %s in %s", column.name(), where)
                .isEqualTo(column.greatest(chunkRows));
    }

    private void assertPageBoundsEnclose(ColumnChunkMetaData chunk, ColumnIndex index, int firstRowOfGroup) {
        Column column = Column.named(chunk.getPath().toDotString());
        List<ByteBuffer> minValues = index.getMinValues();
        List<ByteBuffer> maxValues = index.getMaxValues();
        assertThat(minValues).hasSize(ROWS_PER_GROUP / ROWS_PER_PAGE);

        for (int page = 0; page < minValues.size(); page++) {
            int firstRow = firstRowOfGroup + page * ROWS_PER_PAGE;
            List<Row> pageRows = rows.subList(firstRow, firstRow + ROWS_PER_PAGE);

            assertThat(column.decode(WriteConformanceSupport.bytesOf(minValues.get(page))))
                    .as("min of %s in the page starting at row %d", column.name(), firstRow)
                    .isEqualTo(column.least(pageRows));
            assertThat(column.decode(WriteConformanceSupport.bytesOf(maxValues.get(page))))
                    .as("max of %s in the page starting at row %d", column.name(), firstRow)
                    .isEqualTo(column.greatest(pageRows));
        }
    }

    private static List<BoundaryOrder> boundaryOrders(Path parquetFile, String columnName) throws IOException {
        List<BoundaryOrder> orders = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(parquetFile))) {
            for (BlockMetaData rowGroup : reader.getFooter().getBlocks()) {
                for (ColumnChunkMetaData chunk : rowGroup.getColumns()) {
                    if (chunk.getPath().toDotString().equals(columnName)) {
                        orders.add(reader.readColumnIndex(chunk).getBoundaryOrder());
                    }
                }
            }
        }
        return orders;
    }

    private static void assertFilteredReadsMatch(Path parquetFile, List<Row> fileRows) throws IOException {
        for (FilterCase filter : filterCases()) {
            List<Integer> expected = fileRows.stream()
                    .filter(filter.matching()::test)
                    .map(Row::id)
                    .toList();
            List<Integer> read = WriteConformanceSupport.idsReadByParquetJava(parquetFile, ID, filter.filter(), true);

            assertThat(expected).as("rows matching %s", filter.filter()).isNotEmpty();
            assertThat(read)
                    .as("ids read by parquet-java for %s", filter.filter())
                    .containsExactlyElementsOf(expected);
        }
    }

    /** The parquet-java filters run against the files, paired with the test telling their matching rows. */
    private static List<FilterCase> filterCases() {
        Binary thousand = Binary.fromConstantByteArray(fixedDecimal(100_000L));
        Binary minusThousand = Binary.fromConstantByteArray(fixedDecimal(-100_000L));
        Binary smallBalance = Binary.fromConstantByteArray(minimalDecimal(-250_000L));
        Binary zeroHalf = Binary.fromConstantByteArray(WriteFixtures.halfFloatBytes(Float.floatToFloat16(0f)));
        int threeBillion = (int) 3_000_000_000L;
        long aboveSignedRange = Long.parseUnsignedLong("12000000000000000000");

        return List.of(
                new FilterCase(FilterApi.gt(FilterApi.binaryColumn(AMOUNT), thousand), row -> row.amount() > 100_000L),
                new FilterCase(
                        FilterApi.lt(FilterApi.binaryColumn(AMOUNT), minusThousand), row -> row.amount() < -100_000L),
                new FilterCase(
                        FilterApi.ltEq(FilterApi.binaryColumn(BALANCE), smallBalance),
                        row -> row.balance() <= -250_000L),
                new FilterCase(
                        FilterApi.gt(FilterApi.intColumn(U32), threeBillion),
                        row -> Integer.compareUnsigned(row.u32(), threeBillion) > 0),
                new FilterCase(
                        FilterApi.lt(FilterApi.intColumn(U32), 1_000_000_000),
                        row -> Integer.compareUnsigned(row.u32(), 1_000_000_000) < 0),
                new FilterCase(
                        FilterApi.gtEq(FilterApi.longColumn(U64), aboveSignedRange),
                        row -> Long.compareUnsigned(row.u64(), aboveSignedRange) >= 0),
                new FilterCase(
                        FilterApi.lt(FilterApi.longColumn(U64), 1L << 62),
                        row -> Long.compareUnsigned(row.u64(), 1L << 62) < 0),
                new FilterCase(FilterApi.lt(FilterApi.binaryColumn(HALF), zeroHalf), row -> row.halfValue() < 0f),
                new FilterCase(FilterApi.gtEq(FilterApi.binaryColumn(HALF), zeroHalf), row -> row.halfValue() >= 0f));
    }

    // --- parquetry assertions ---

    private void assertParquetryCount(Predicate predicate, Matching matching) {
        long expected = rows.stream().filter(matching::test).count();
        long pruned = WriteConformanceSupport.rowsReadByParquetry(file, predicate, ReadOptions.DEFAULTS);
        long scanned = WriteConformanceSupport.rowsReadByParquetry(file, predicate, ReadFixtures.METADATA_PRUNING_OFF);

        assertThat(expected).as("rows matching %s", predicate).isPositive();
        assertThat(pruned).as("rows read for %s", predicate).isEqualTo(scanned).isEqualTo(expected);
    }

    // --- columns under test ---

    /**
     * A column ordered by its logical type: how to decode one of its bounds into a comparable value, and the same value
     * taken from a generated row.
     */
    private record Column(String name, Function<byte[], Comparable<?>> decoder, Function<Row, Comparable<?>> value) {

        static final Column AMOUNT_COLUMN =
                new Column(AMOUNT, BigInteger::new, row -> BigInteger.valueOf(row.amount()));
        static final Column BALANCE_COLUMN =
                new Column(BALANCE, BigInteger::new, row -> BigInteger.valueOf(row.balance()));
        static final Column U32_COLUMN = new Column(
                U32,
                bytes -> Integer.toUnsignedLong(littleEndian(bytes).getInt()),
                row -> Integer.toUnsignedLong(row.u32()));
        static final Column U64_COLUMN =
                new Column(U64, bytes -> unsigned(littleEndian(bytes).getLong()), row -> unsigned(row.u64()));
        static final Column HALF_COLUMN = new Column(
                HALF, bytes -> Float.float16ToFloat(littleEndian(bytes).getShort()), Row::halfValue);
        static final Column ID_COLUMN =
                new Column(ID, bytes -> littleEndian(bytes).getInt(), Row::id);

        static final List<Column> ORDERED = List.of(AMOUNT_COLUMN, BALANCE_COLUMN, U32_COLUMN, U64_COLUMN, HALF_COLUMN);

        static Column named(String name) {
            return Stream.concat(ORDERED.stream(), Stream.of(ID_COLUMN))
                    .filter(column -> column.name().equals(name))
                    .findFirst()
                    .orElseThrow();
        }

        Comparable<?> decode(byte[] bound) {
            return decoder.apply(bound);
        }

        Comparable<?> least(List<Row> rows) {
            return rows.stream().map(value).min(naturalOrder()).orElseThrow();
        }

        Comparable<?> greatest(List<Row> rows) {
            return rows.stream().map(value).max(naturalOrder()).orElseThrow();
        }

        Comparator<Row> order() {
            return Comparator.comparing(value, naturalOrder());
        }

        // The values of one column share a type; the wildcard hides it from the compiler.
        @SuppressWarnings({"unchecked", "rawtypes"})
        private static Comparator<Comparable<?>> naturalOrder() {
            return (a, b) -> ((Comparable) a).compareTo(b);
        }

        private static ByteBuffer littleEndian(byte[] bytes) {
            return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        }

        private static BigInteger unsigned(long value) {
            return new BigInteger(Long.toUnsignedString(value));
        }
    }

    // --- fixture ---

    private static List<Row> randomRows(Random random) {
        List<Row> generated = new ArrayList<>(ROWS);
        for (int id = 0; id < ROWS; id++) {
            long amount = random.nextLong(-500_000L, 500_001L);
            long balance = random.nextLong(-500_000L, 500_001L);
            short half = Float.floatToFloat16(random.nextFloat(-100f, 100f));
            generated.add(new Row(id, amount, balance, random.nextInt(), random.nextLong(), half));
        }
        return generated;
    }

    private Path write(Path target, List<Row> fileRows) throws IOException {
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .pageValueLimit(ROWS_PER_PAGE)
                .build();
        List<Map<ColumnPath, Object>> cells =
                fileRows.stream().map(StatisticsOrderConformanceIT::cells).toList();
        return WriteFixtures.writeRows(target, schema(), options, cells);
    }

    private static Map<ColumnPath, Object> cells(Row row) {
        Map<ColumnPath, Object> cells = new HashMap<>();
        cells.put(ColumnPath.of(ID), row.id());
        cells.put(ColumnPath.of(AMOUNT), fixedDecimal(row.amount()));
        cells.put(ColumnPath.of(BALANCE), minimalDecimal(row.balance()));
        cells.put(ColumnPath.of(U32), row.u32());
        cells.put(ColumnPath.of(U64), row.u64());
        cells.put(ColumnPath.of(HALF), WriteFixtures.halfFloatBytes(row.half()));
        return cells;
    }

    private static ParquetSchema schema() {
        LogicalType decimal = new LogicalType.Decimal(DECIMAL_SCALE, DECIMAL_PRECISION);
        OptionalInt none = OptionalInt.empty();
        OptionalInt halfFloatBytes = OptionalInt.of(WriteFixtures.HALF_FLOAT_BYTES);
        return WriteFixtures.schemaOf(
                WriteFixtures.requiredLeaf(ID, PrimitiveKind.INT32),
                WriteFixtures.requiredLeaf(
                        AMOUNT, PrimitiveKind.FIXED_LEN_BYTE_ARRAY, OptionalInt.of(DECIMAL_BYTES), decimal),
                WriteFixtures.requiredLeaf(BALANCE, PrimitiveKind.BYTE_ARRAY, none, decimal),
                WriteFixtures.requiredLeaf(U32, PrimitiveKind.INT32, none, new LogicalType.IntType((byte) 32, false)),
                WriteFixtures.requiredLeaf(U64, PrimitiveKind.INT64, none, new LogicalType.IntType((byte) 64, false)),
                WriteFixtures.requiredLeaf(
                        HALF, PrimitiveKind.FIXED_LEN_BYTE_ARRAY, halfFloatBytes, new LogicalType.Float16Type()));
    }

    /** A decimal's unscaled value as fixed-length big-endian two's complement bytes. */
    private static byte[] fixedDecimal(long unscaled) {
        return WriteFixtures.signedBytes(BigInteger.valueOf(unscaled), DECIMAL_BYTES);
    }

    /** A decimal's unscaled value in the fewest big-endian two's complement bytes. */
    private static byte[] minimalDecimal(long unscaled) {
        return BigInteger.valueOf(unscaled).toByteArray();
    }
}
