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
package io.tileverse.parquetry.conformance;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Stream;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Proves that pruning never changes the rows matched by a predicate on the {@code apache/parquet-testing} fixtures with
 * statistics in an order other than the byte order of their cells. For a grid of predicates built from each column's
 * own cells, {@code read} with all tiers on, {@code read} with pruning off, {@code readBatches} both ways, and
 * {@code count} select the rows computed independently by this test.
 */
class StatisticsPruningParityIT {

    private static final String DATA = "parquet-testing/data/";

    private static final ReadOptions PRUNING_OFF = ReadOptions.builder()
            .useStatsFilter(false)
            .useDictionaryFilter(false)
            .useColumnIndexFilter(false)
            .useBloomFilter(false)
            .build();

    @TempDir
    Path tempDir;

    @Test
    void halfFloatColumnsMatchTheSameRowsWithAndWithoutPruning() {
        assertPruningParity(
                "floating_orders_nan_count.parquet",
                List.of("float16_ieee754", "float16_typedef"),
                StatisticsPruningParityIT::binaryLiterals);
    }

    @Test
    void wideTimestampColumnsMatchTheSameRowsWithAndWithoutPruning() {
        assertPruningParity(
                "flba12_timestamp.parquet",
                List.of("timestamp_millis", "timestamp_micros", "timestamp_nanos"),
                StatisticsPruningParityIT::binaryLiterals);
    }

    @Test
    void uuidAnnotatedIntegersMatchTheSameRowsWithAndWithoutPruning() {
        assertPruningParity(
                "int32_with_uuid_logical_type.parquet",
                List.of("int32_uuid"),
                StatisticsPruningParityIT::integerLiterals);
    }

    @Test
    void int96NullChecksMatchTheSameRowsWithAndWithoutPruning() {
        assertPruningParity("int96_timestamp_order.parquet", List.of("ts"), cells -> List.of());
    }

    // --- the parity check ---

    private void assertPruningParity(
            String fileName, List<String> columns, Function<List<Object>, List<Value>> literalsOf) {
        Path file = TestCorpus.extractFile(DATA + fileName, tempDir);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            List<Map<ColumnPath, Object>> rows = readRows(reader, Predicate.ALWAYS_TRUE, PRUNING_OFF);
            SoftAssertions softly = new SoftAssertions();
            for (String column : columns) {
                ColumnPath path = ColumnPath.of(column);
                List<Value> literals = literalsOf.apply(cellsOf(rows, path));
                for (Predicate predicate : predicateGrid(path, literals)) {
                    List<String> expected = expectedRowKeys(rows, path, predicate);
                    assertReadPathsSelect(softly, reader, predicate, expected);
                }
            }
            softly.assertAll();
        }
    }

    private static void assertReadPathsSelect(
            SoftAssertions softly, ParquetFileReader reader, Predicate predicate, List<String> expected) {
        softly.assertThat(keysOf(readRows(reader, predicate, ReadOptions.DEFAULTS)))
                .as("read with pruning: %s", describe(predicate))
                .isEqualTo(expected);
        softly.assertThat(keysOf(readRows(reader, predicate, PRUNING_OFF)))
                .as("read without pruning: %s", describe(predicate))
                .isEqualTo(expected);
        softly.assertThat(keysOf(readBatchRows(reader, predicate, ReadOptions.DEFAULTS)))
                .as("readBatches with pruning: %s", describe(predicate))
                .isEqualTo(expected);
        softly.assertThat(keysOf(readBatchRows(reader, predicate, PRUNING_OFF)))
                .as("readBatches without pruning: %s", describe(predicate))
                .isEqualTo(expected);
        softly.assertThat(reader.count(predicate, ReadOptions.DEFAULTS))
                .as("count: %s", describe(predicate))
                .isEqualTo(expected.size());
    }

    /** A readable form of a grid predicate, rendering a binary literal as hex. */
    private static String describe(Predicate predicate) {
        return switch (predicate) {
            case Predicate.Eq eq -> "Eq " + eq.col().dot() + " " + renderLiteral(eq.v());
            case Predicate.NotEq notEq -> "NotEq " + notEq.col().dot() + " " + renderLiteral(notEq.v());
            case Predicate.Lt lt -> "Lt " + lt.col().dot() + " " + renderLiteral(lt.v());
            case Predicate.LtEq ltEq -> "LtEq " + ltEq.col().dot() + " " + renderLiteral(ltEq.v());
            case Predicate.Gt gt -> "Gt " + gt.col().dot() + " " + renderLiteral(gt.v());
            case Predicate.GtEq gtEq -> "GtEq " + gtEq.col().dot() + " " + renderLiteral(gtEq.v());
            case Predicate.In in ->
                "In " + in.col().dot() + " "
                        + in.values().stream()
                                .map(StatisticsPruningParityIT::renderLiteral)
                                .toList();
            default -> predicate.toString();
        };
    }

    private static String renderLiteral(Value literal) {
        if (literal instanceof Value.BinaryVal binary) {
            return "0x" + HexFormat.of().formatHex(bytesOf(binary));
        }
        return literal.toString();
    }

    /** Null checks, the six comparisons against each literal, and IN over each pair of neighboring literals. */
    private static List<Predicate> predicateGrid(ColumnPath column, List<Value> literals) {
        List<Predicate> grid = new ArrayList<>();
        grid.add(new Predicate.IsNull(column));
        grid.add(new Predicate.IsNotNull(column));
        for (Value literal : literals) {
            grid.add(new Predicate.Eq(column, literal));
            grid.add(new Predicate.NotEq(column, literal));
            grid.add(new Predicate.Lt(column, literal));
            grid.add(new Predicate.LtEq(column, literal));
            grid.add(new Predicate.Gt(column, literal));
            grid.add(new Predicate.GtEq(column, literal));
        }
        for (int i = 0; i + 1 < literals.size(); i++) {
            grid.add(new Predicate.In(column, List.of(literals.get(i), literals.get(i + 1))));
        }
        return grid;
    }

    // --- literal grids ---

    /** The distinct cells of a fixed-length binary column, plus the all-zero and all-one values of the same width. */
    private static List<Value> binaryLiterals(List<Object> cells) {
        Map<String, byte[]> distinct = new LinkedHashMap<>();
        int width = 0;
        for (Object cell : cells) {
            if (cell instanceof byte[] bytes) {
                distinct.put(HexFormat.of().formatHex(bytes), bytes);
                width = bytes.length;
            }
        }
        byte[] allOnes = new byte[width];
        Arrays.fill(allOnes, (byte) 0xFF);
        distinct.put("zeros", new byte[width]);
        distinct.put("ones", allOnes);
        List<Value> literals = new ArrayList<>(distinct.size());
        for (byte[] bytes : distinct.values()) {
            literals.add(new Value.BinaryVal(MemorySegment.ofArray(bytes.clone())));
        }
        return literals;
    }

    /** The distinct cells of an INT32 column in ascending order, plus one value below and one above them all. */
    private static List<Value> integerLiterals(List<Object> cells) {
        TreeSet<Integer> distinct = new TreeSet<>();
        for (Object cell : cells) {
            if (cell instanceof Integer value) {
                distinct.add(value);
            }
        }
        distinct.add(distinct.first() - 1);
        distinct.add(distinct.last() + 1);
        List<Value> literals = new ArrayList<>(distinct.size());
        for (int value : distinct) {
            literals.add(new Value.IntVal(value));
        }
        return literals;
    }

    // --- the independent expectation ---

    private static List<String> expectedRowKeys(
            List<Map<ColumnPath, Object>> rows, ColumnPath column, Predicate predicate) {
        List<Map<ColumnPath, Object>> matching = new ArrayList<>();
        for (Map<ColumnPath, Object> row : rows) {
            if (expectedMatch(predicate, row.get(column))) {
                matching.add(row);
            }
        }
        return keysOf(matching);
    }

    /** A null cell matches only a null check; a value comparison against it is false. */
    private static boolean expectedMatch(Predicate predicate, Object cell) {
        if (cell == null) {
            return predicate instanceof Predicate.IsNull;
        }
        return switch (predicate) {
            case Predicate.IsNull _ -> false;
            case Predicate.IsNotNull _ -> true;
            case Predicate.Eq eq -> equal(cell, eq.v());
            case Predicate.NotEq notEq -> !equal(cell, notEq.v());
            case Predicate.Lt lt -> ordered(cell, lt.v(), sign -> sign < 0);
            case Predicate.LtEq ltEq -> ordered(cell, ltEq.v(), sign -> sign <= 0);
            case Predicate.Gt gt -> ordered(cell, gt.v(), sign -> sign > 0);
            case Predicate.GtEq gtEq -> ordered(cell, gtEq.v(), sign -> sign >= 0);
            case Predicate.In in -> in.values().stream().anyMatch(literal -> equal(cell, literal));
            default -> throw new IllegalArgumentException("not in the grid: " + predicate);
        };
    }

    private static boolean equal(Object cell, Value literal) {
        OptionalInt sign = order(cell, literal);
        return sign.isPresent() && sign.getAsInt() == 0;
    }

    private static boolean ordered(Object cell, Value literal, SignTest test) {
        OptionalInt sign = order(cell, literal);
        return sign.isPresent() && test.holds(sign.getAsInt());
    }

    /** The sign of {@code cell - literal}: unsigned byte order for binary cells, signed order for integers. */
    private static OptionalInt order(Object cell, Value literal) {
        return switch (literal) {
            case Value.BinaryVal binary -> OptionalInt.of(Arrays.compareUnsigned((byte[]) cell, bytesOf(binary)));
            case Value.IntVal integer -> OptionalInt.of(Integer.compare((Integer) cell, integer.value()));
            default -> throw new IllegalArgumentException("no expectation for literal " + literal);
        };
    }

    private static byte[] bytesOf(Value.BinaryVal binary) {
        return binary.value().toArray(JAVA_BYTE);
    }

    @FunctionalInterface
    private interface SignTest {
        boolean holds(int sign);
    }

    // --- reading ---

    private static List<Map<ColumnPath, Object>> readRows(
            ParquetFileReader reader, Predicate predicate, ReadOptions options) {
        List<ColumnPath> leaves = reader.schema().leafColumns();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        try (Stream<ParquetRecord> records = reader.read(predicate, Projection.ALL, options)) {
            records.forEach(rec -> rows.add(cellsOf(rec, leaves)));
        }
        return rows;
    }

    private static List<Map<ColumnPath, Object>> readBatchRows(
            ParquetFileReader reader, Predicate predicate, ReadOptions options) {
        List<ColumnPath> leaves = reader.schema().leafColumns();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        try (Stream<ParquetRecordBatch> batches = reader.readBatches(predicate, Projection.ALL, options)) {
            batches.forEach(batch -> {
                for (int row = 0; row < batch.rowCount(); row++) {
                    rows.add(cellsOf(batch.materialize(row), leaves));
                }
                batch.close();
            });
        }
        return rows;
    }

    /**
     * The cells of one record in column order, which keeps row keys comparable. Binary cells are copied: their segments
     * do not outlive the read.
     */
    private static Map<ColumnPath, Object> cellsOf(ParquetRecord rec, List<ColumnPath> leaves) {
        Map<ColumnPath, Object> cells = new LinkedHashMap<>();
        for (ColumnPath leaf : leaves) {
            Object cell = rec.get(leaf);
            cells.put(leaf, cell instanceof MemorySegment segment ? segment.toArray(JAVA_BYTE) : cell);
        }
        return cells;
    }

    private static List<Object> cellsOf(List<Map<ColumnPath, Object>> rows, ColumnPath column) {
        List<Object> cells = new ArrayList<>(rows.size());
        for (Map<ColumnPath, Object> row : rows) {
            cells.add(row.get(column));
        }
        return cells;
    }

    // --- row identity ---

    private static List<String> keysOf(List<Map<ColumnPath, Object>> rows) {
        List<String> keys = new ArrayList<>(rows.size());
        for (Map<ColumnPath, Object> row : rows) {
            keys.add(keyOf(row));
        }
        return keys;
    }

    /** Renders each cell exactly, with the raw bits of a floating-point cell, in column order. */
    private static String keyOf(Map<ColumnPath, Object> row) {
        StringBuilder key = new StringBuilder();
        for (Map.Entry<ColumnPath, Object> cell : row.entrySet()) {
            key.append(cell.getKey().dot()).append('=');
            key.append(render(cell.getValue())).append(';');
        }
        return key.toString();
    }

    private static String render(Object cell) {
        return switch (cell) {
            case null -> "null";
            case byte[] bytes -> HexFormat.of().formatHex(bytes);
            case Float value -> "f" + Integer.toHexString(Float.floatToRawIntBits(value));
            case Double value -> "d" + Long.toHexString(Double.doubleToRawLongBits(value));
            default -> String.valueOf(cell);
        };
    }
}
