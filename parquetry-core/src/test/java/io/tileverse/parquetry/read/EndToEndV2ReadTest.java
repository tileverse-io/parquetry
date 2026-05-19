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
package io.tileverse.parquetry.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaBuilder;

import io.tileverse.io.ByteBufferPool;

/**
 * End-to-end integration test: write a small Parquet 2.0 file via {@code parquet-avro}, then read it back through the
 * parquetry pipeline (DataPageV2Reader + StreamingColumnReader + RealColumnFetcher + RowGroupReader) and assert the
 * records round-trip exactly.
 *
 * <p>The {@code ParquetDataset.open(...)} facade is wired later; until then, this test bootstraps the read pipeline
 * manually (read footer, build schema, wire ColumnFetcher.real, iterate row groups). The helper is documented as
 * transitional test infrastructure.
 */
class EndToEndV2ReadTest {

    private static final ColumnPath YEAR = ColumnPath.of("year");
    private static final ColumnPath COUNTRY = ColumnPath.of("country");
    private static final ColumnPath VALUE = ColumnPath.of("value");
    private static final ColumnPath OPTIONAL_VALUE = ColumnPath.of("optional_value");

    @ParameterizedTest
    @EnumSource(
            value = ConcurrencyMode.class,
            names = {"SYNC", "PREFETCH_ONLY", "FAN_OUT_ONLY"})
    void readsParquet2SnappyFlatSchemaEndToEnd(ConcurrencyMode mode, @TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("v2-snappy.parquet");
        List<RowDto> expected = generateRows(100);
        writeParquet2File(file, expected, CompressionCodecName.SNAPPY, /*rowGroupBytes*/ 16L * 1024 * 1024);

        ByteBufferPool pool = new ByteBufferPool();
        List<ParquetRecord> actual = readAllRecords(file, pool, mode);

        assertThat(actual).hasSize(expected.size());
        assertRecordsMatch(actual, expected);
        assertThat(outstandingBorrows(pool))
                .as("Every borrowed PooledByteBuffer must be returned end-to-end")
                .isZero();
    }

    @ParameterizedTest
    @EnumSource(
            value = ConcurrencyMode.class,
            names = {"SYNC", "PREFETCH_ONLY", "FAN_OUT_ONLY"})
    void readsParquet2WithMultipleRowGroups(ConcurrencyMode mode, @TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("v2-multi-rg.parquet");
        List<RowDto> expected = generateRows(2_000);
        // Tiny row-group target forces parquet-avro to flush multiple row groups for our 2000 rows.
        writeParquet2File(file, expected, CompressionCodecName.SNAPPY, /*rowGroupBytes*/ 8_192L);

        ByteBufferPool pool = new ByteBufferPool();
        List<ParquetRecord> actual = readAllRecords(file, pool, mode);

        assertThat(actual).hasSize(expected.size());
        assertRecordsMatch(actual, expected);
        assertThat(rowGroupCount(file))
                .as("fixture should span multiple row groups for the multi-RG assertion to be load-bearing")
                .isGreaterThan(1);
        assertThat(outstandingBorrows(pool)).isZero();
    }

    @Test
    void readsParquet2UncompressedRoundTrip(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("v2-uncompressed.parquet");
        List<RowDto> expected = generateRows(50);
        writeParquet2File(file, expected, CompressionCodecName.UNCOMPRESSED, /*rowGroupBytes*/ 16L * 1024 * 1024);

        ByteBufferPool pool = new ByteBufferPool();
        // Fan-out uses one virtual thread per projected column; with the UNCOMPRESSED codec there is no shared
        // decompressor instance to race on, so we can exercise the parallel fetch path safely here.
        List<ParquetRecord> actual = readAllRecords(file, pool, ConcurrencyMode.FAN_OUT_ONLY);

        assertRecordsMatch(actual, expected);
        assertThat(outstandingBorrows(pool)).isZero();
    }

    @Test
    void readsParquet2WithNullableColumn(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("v2-nullable.parquet");
        int rowCount = 20;
        writeParquet2FileWithNullableColumn(file, rowCount);

        ByteBufferPool pool = new ByteBufferPool();
        List<ParquetRecord> actual = readAllRecords(file, pool, ConcurrencyMode.FAN_OUT_ONLY);

        assertThat(actual).hasSize(rowCount);
        for (int i = 0; i < rowCount; i++) {
            ParquetRecord rec = actual.get(i);
            assertThat(rec.getInt(YEAR)).as("year @ row %d", i).isEqualTo(expectedYear(i));
            if (i % 2 == 0) {
                assertThat(rec.isNull(OPTIONAL_VALUE))
                        .as("optional_value @ row %d should be null", i)
                        .isTrue();
            } else {
                assertThat(rec.isNull(OPTIONAL_VALUE))
                        .as("optional_value @ row %d should not be null", i)
                        .isFalse();
                assertThat(rec.getInt(OPTIONAL_VALUE))
                        .as("optional_value @ row %d", i)
                        .isEqualTo(expectedOptionalValue(i));
            }
        }
        assertThat(outstandingBorrows(pool)).isZero();
    }

    // --- fixture generation ---

    private static void writeParquet2File(
            Path out, List<RowDto> rows, CompressionCodecName compression, long rowGroupBytes) throws IOException {
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse("""
                {"type":"record","name":"Row","fields":[
                  {"name":"year","type":"int"},
                  {"name":"country","type":"string"},
                  {"name":"value","type":"double"}
                ]}""");
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(out))
                .withSchema(schema)
                .withCompressionCodec(compression)
                .withWriterVersion(WriterVersion.PARQUET_2_0)
                .withRowGroupSize(rowGroupBytes)
                .build()) {
            for (RowDto r : rows) {
                GenericData.Record avroRecord = new GenericData.Record(schema);
                avroRecord.put("year", r.year());
                avroRecord.put("country", r.country());
                avroRecord.put("value", r.value());
                writer.write(avroRecord);
            }
        }
    }

    private static List<RowDto> generateRows(int count) {
        List<RowDto> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            rows.add(new RowDto(2020 + (i % 5), (i % 2 == 0) ? "AR" : "BR", i * 1.5));
        }
        return rows;
    }

    /**
     * Writes a Parquet V2 file with a schema that includes one required {@code int} column ({@code year}) and one
     * optional {@code int} column ({@code optional_value}). Even-indexed rows leave {@code optional_value} null;
     * odd-indexed rows write {@link #expectedOptionalValue(int)}.
     */
    private static void writeParquet2FileWithNullableColumn(Path out, int rowCount) throws IOException {
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse("""
                {"type":"record","name":"Row","fields":[
                  {"name":"year","type":"int"},
                  {"name":"optional_value","type":["null","int"],"default":null}
                ]}""");
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(out))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .withWriterVersion(WriterVersion.PARQUET_2_0)
                .withRowGroupSize(16L * 1024 * 1024)
                .build()) {
            for (int i = 0; i < rowCount; i++) {
                GenericData.Record avroRecord = new GenericData.Record(schema);
                avroRecord.put("year", expectedYear(i));
                avroRecord.put("optional_value", (i % 2 == 0) ? null : expectedOptionalValue(i));
                writer.write(avroRecord);
            }
        }
    }

    private static int expectedYear(int rowIndex) {
        return 2020 + (rowIndex % 5);
    }

    private static int expectedOptionalValue(int rowIndex) {
        return rowIndex * 10;
    }

    // --- bootstrap helper (test infrastructure) ---

    private static List<ParquetRecord> readAllRecords(Path file, ByteBufferPool pool, ConcurrencyMode mode)
            throws IOException {
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            FileMetaData footer = ParquetFormat.readFooter(reader);
            ParquetSchema schema = SchemaBuilder.build(footer.schema());
            ColumnFetcher fetcher = ColumnFetcher.real(reader, schema, pool);
            ReadOptions options = ReadOptions.builder()
                    .concurrencyMode(mode)
                    .byteBufferPool(pool)
                    .build();

            List<ParquetRecord> collected = new ArrayList<>();
            for (RowGroup rg : footer.rowGroups()) {
                try (RowGroupReader rgReader = new RowGroupReader(reader, schema, schema, rg, options, fetcher);
                        Stream<ParquetRecord> stream = rgReader.read(Materializer.defaultRecord())) {
                    stream.forEach(collected::add);
                }
            }
            return collected;
        }
    }

    private static int rowGroupCount(Path file) throws IOException {
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            return ParquetFormat.readFooter(reader).rowGroups().size();
        }
    }

    // --- assertions ---

    private static void assertRecordsMatch(List<ParquetRecord> actual, List<RowDto> expected) {
        for (int i = 0; i < expected.size(); i++) {
            ParquetRecord rec = actual.get(i);
            RowDto exp = expected.get(i);
            assertThat(rec.getInt(YEAR)).as("year @ row %d", i).isEqualTo(exp.year());
            assertThat(rec.getString(COUNTRY)).as("country @ row %d", i).isEqualTo(exp.country());
            assertThat(rec.getDouble(VALUE)).as("value @ row %d", i).isEqualTo(exp.value());
        }
    }

    private static long outstandingBorrows(ByteBufferPool pool) {
        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        return (stats.created() + stats.reused()) - (stats.returned() + stats.discarded());
    }

    // --- value type ---

    private record RowDto(int year, String country, double value) {}
}
