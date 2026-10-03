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
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Pins the cell values of {@code apache/parquet-testing} fixtures to the values documented by the upstream corpus. It
 * covers fixtures unreadable by the parquet-java oracle of {@link ParquetTestingCorpusIT}, and logical annotations left
 * unchecked by that comparison. Cells arrive in parquetry's physical form: binary leaves as bytes, timestamps as their
 * stored integers.
 */
class ParquetTestingFixtureValuesIT {

    private static final String DATA = "parquet-testing/data/";

    @TempDir
    Path tempDir;

    @Test
    void jsonColumnKeepsItsLogicalTypeAndDocuments() {
        Path file = TestCorpus.extractFile(DATA + "json.parquet", tempDir);
        ColumnPath column = ColumnPath.of("json_field");

        assertThat(logicalTypeOf(file, column)).containsInstanceOf(LogicalType.JsonType.class);
        assertThat(readUtf8Cells(file, column))
                .containsExactly("{\"a\":1}", "{\"a\":1,\"b\":null}", "[1,null,3]", null);
    }

    @Test
    void bsonColumnKeepsItsLogicalTypeAndDocuments() {
        Path file = TestCorpus.extractFile(DATA + "bson.parquet", tempDir);
        ColumnPath column = ColumnPath.of("bson_field");

        assertThat(logicalTypeOf(file, column)).containsInstanceOf(LogicalType.BsonType.class);
        assertThat(readHexCells(file, column))
                .containsExactly("0c0000001061000100000000", "0f000000106100010000000a620000", null);
    }

    @Test
    void uuidAnnotationOnInt32ReadsThePhysicalIntegers() {
        Path file = TestCorpus.extractFile(DATA + "int32_with_uuid_logical_type.parquet", tempDir);

        assertThat(readCells(file, ColumnPath.of("int32_uuid"))).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    }

    @Test
    void flba12TimestampCellsHoldTheDocumentedInstants() {
        Path file = TestCorpus.extractFile(DATA + "flba12_timestamp.parquet", tempDir);

        assertThat(readSigned96BitCells(file, ColumnPath.of("timestamp_millis")))
                .containsExactlyElementsOf(documentedEpochSecondsScaledBy(1_000L));
        assertThat(readSigned96BitCells(file, ColumnPath.of("timestamp_micros")))
                .containsExactlyElementsOf(documentedEpochSecondsScaledBy(1_000_000L));
        assertThat(readSigned96BitCells(file, ColumnPath.of("timestamp_nanos")))
                .containsExactlyElementsOf(documentedEpochSecondsScaledBy(1_000_000_000L));
    }

    @Test
    void int96TimestampOrderCellsHoldTheDocumentedTimestamps() {
        Path file = TestCorpus.extractFile(DATA + "int96_timestamp_order.parquet", tempDir);

        // Written out of chronological order: LATE_IN_DAY, NEXT_DAY, EARLY, SAME_DAY_EARLY.
        assertThat(readInt96Cells(file, ColumnPath.of("ts")))
                .containsExactly(
                        new Int96(2440588, 86_399_999_999_999L),
                        new Int96(2440589, 0L),
                        new Int96(2440000, 123L),
                        new Int96(2440588, 1000L));
    }

    /** Epoch seconds of the six rows in {@code flba12_timestamp.md}, in row order. */
    private static List<BigInteger> documentedEpochSecondsScaledBy(long unitsPerSecond) {
        long[] epochSeconds = {0L, 1L, -1L, 9_223_372_036L, 253_402_300_799L, -62_135_596_800L};
        BigInteger scale = BigInteger.valueOf(unitsPerSecond);
        List<BigInteger> scaled = new ArrayList<>(epochSeconds.length);
        for (long seconds : epochSeconds) {
            scaled.add(BigInteger.valueOf(seconds).multiply(scale));
        }
        return scaled;
    }

    private static List<String> readUtf8Cells(Path file, ColumnPath column) {
        List<String> cells = new ArrayList<>();
        for (Object cell : readCells(file, column)) {
            cells.add(cell == null ? null : new String((byte[]) cell, StandardCharsets.UTF_8));
        }
        return cells;
    }

    private static List<String> readHexCells(Path file, ColumnPath column) {
        List<String> cells = new ArrayList<>();
        for (Object cell : readCells(file, column)) {
            cells.add(cell == null ? null : HexFormat.of().formatHex((byte[]) cell));
        }
        return cells;
    }

    /** Decodes FIXED_LEN_BYTE_ARRAY(12) cells as little-endian two's-complement 96-bit integers. */
    private static List<BigInteger> readSigned96BitCells(Path file, ColumnPath column) {
        List<BigInteger> cells = new ArrayList<>();
        for (Object cell : readCells(file, column)) {
            cells.add(signedLittleEndian((byte[]) cell));
        }
        return cells;
    }

    private static BigInteger signedLittleEndian(byte[] littleEndian) {
        byte[] bigEndian = littleEndian.clone();
        reverse(bigEndian);
        return new BigInteger(bigEndian);
    }

    private static void reverse(byte[] bytes) {
        for (int left = 0, right = bytes.length - 1; left < right; left++, right--) {
            byte swap = bytes[left];
            bytes[left] = bytes[right];
            bytes[right] = swap;
        }
    }

    private static List<Int96> readInt96Cells(Path file, ColumnPath column) {
        List<Int96> cells = new ArrayList<>();
        for (Object cell : readCells(file, column)) {
            cells.add(Int96.of((byte[]) cell));
        }
        return cells;
    }

    /** An INT96 timestamp cell: eight little-endian bytes of nanoseconds within the day, then four of Julian day. */
    private record Int96(int julianDay, long nanosOfDay) {

        static Int96 of(byte[] cell) {
            ByteBuffer buffer = ByteBuffer.wrap(cell).order(ByteOrder.LITTLE_ENDIAN);
            long nanosOfDay = buffer.getLong(0);
            int julianDay = buffer.getInt(Long.BYTES);
            return new Int96(julianDay, nanosOfDay);
        }
    }

    private static Optional<LogicalType> logicalTypeOf(Path file, ColumnPath column) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetSchema schema = ParquetFileReader.open(source).schema();
            SchemaNode leaf = schema.find(column).orElseThrow();
            return ((SchemaNode.Primitive) leaf).logicalType();
        }
    }

    /**
     * Reads one column of the file's rows, in file order. Binary cells are copied to {@code byte[]} while the record is
     * live: their segments do not outlive the read.
     */
    private static List<Object> readCells(Path file, ColumnPath column) {
        List<Object> cells = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file);
                Stream<ParquetRecord> records = ParquetFileReader.open(source)
                        .read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            records.forEach(row -> cells.add(copyOf(row.get(column))));
        }
        return Collections.unmodifiableList(cells);
    }

    private static Object copyOf(Object cell) {
        if (cell instanceof MemorySegment segment) {
            return segment.toArray(JAVA_BYTE);
        }
        return cell;
    }
}
