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
package io.tileverse.parquetry.internal.read;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;

import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Shared test-fixture helpers for Parquet read-path tests.
 *
 * <p>Provides a canonical three-column multi-row-group file writer, a row-group counter, and a {@link ByteRangeSource}
 * factory over a local file.
 */
public final class TestParquetFiles {

    private TestParquetFiles() {}

    private static final String SCHEMA = """
            {"type":"record","name":"Row","fields":[
              {"name":"year","type":"int"},
              {"name":"country","type":"string"},
              {"name":"value","type":"double"}
            ]}""";

    private static final String FIXED_LEN_SCHEMA = """
            {"type":"record","name":"Row","fields":[
              {"name":"id","type":"int"},
              {"name":"uid","type":{"type":"fixed","name":"Uid","size":16}}
            ]}""";

    private static final String NULLABLE_LONG_SCHEMA = """
            {"type":"record","name":"Row","fields":[
              {"name":"id","type":"long"},
              {"name":"stored","type":["null","long"],"default":null}
            ]}""";

    /** The base a non-null {@code stored} cell adds to its row's {@code id} in the nullable-long fixture. */
    public static final long STORED_BASE = 1_000L;

    static final int FIXED_UID_WIDTH = 16;

    /**
     * Writes a Parquet 2.0 / Snappy file with three columns ({@code year}, {@code country}, {@code value}) and a tiny
     * row-group target so the given number of rows spans multiple row groups.
     *
     * @param dir directory in which to write the file
     * @param rows total number of rows to write
     * @return path to the written file
     */
    public static Path writeFlatThreeColumnFileMultiRowGroup(Path dir, int rows) throws IOException {
        Path file = dir.resolve("three-col-multi-rg.parquet");
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(SCHEMA);
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(file))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriterVersion(WriterVersion.PARQUET_2_0)
                .withRowGroupSize(8_192L)
                .build()) {
            for (int i = 0; i < rows; i++) {
                GenericData.Record rec = new GenericData.Record(schema);
                rec.put("year", 2020 + (i % 5));
                rec.put("country", (i % 2 == 0) ? "AR" : "BR");
                rec.put("value", i * 1.5);
                writer.write(rec);
            }
        }
        return file;
    }

    /**
     * Writes a Parquet 2.0 / Snappy file with an {@code id} INT32 column and a {@code uid} FIXED_LEN_BYTE_ARRAY(16)
     * column, spanning multiple row groups. Each {@code uid} encodes its row's {@code id} in the last four bytes (see
     * {@link #fixedUid(int)}), giving the column distinct, ordered values to filter on.
     *
     * @param dir directory in which to write the file
     * @param rows total number of rows to write
     * @return path to the written file
     */
    static Path writeFixedLenColumnFileMultiRowGroup(Path dir, int rows) throws IOException {
        Path file = dir.resolve("fixed-len-multi-rg.parquet");
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(FIXED_LEN_SCHEMA);
        org.apache.avro.Schema uidSchema = schema.getField("uid").schema();
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(file))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriterVersion(WriterVersion.PARQUET_2_0)
                .withRowGroupSize(8_192L)
                .build()) {
            for (int i = 0; i < rows; i++) {
                GenericData.Record rec = new GenericData.Record(schema);
                rec.put("id", i);
                rec.put("uid", new GenericData.Fixed(uidSchema, fixedUid(i)));
                writer.write(rec);
            }
        }
        return file;
    }

    /**
     * Writes a Parquet 2.0 / Snappy file with a required {@code id} INT64 column (the row index) and an optional
     * {@code stored} INT64 column, spanning multiple row groups. Every third row (id divisible by 3) stores
     * {@code STORED_BASE + id}; the other rows leave {@code stored} null. The pattern makes each cell's expected value
     * recoverable from its row's {@code id}, letting coalesce tests verify stored cells win and null cells fall back.
     *
     * @param dir directory in which to write the file
     * @param rows total number of rows to write
     * @return path to the written file
     */
    public static Path writeNullableLongColumnFileMultiRowGroup(Path dir, int rows) throws IOException {
        Path file = dir.resolve("nullable-long-multi-rg.parquet");
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(NULLABLE_LONG_SCHEMA);
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                        new LocalOutputFile(file))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriterVersion(WriterVersion.PARQUET_2_0)
                .withRowGroupSize(8_192L)
                .build()) {
            for (long id = 0; id < rows; id++) {
                GenericData.Record rec = new GenericData.Record(schema);
                rec.put("id", id);
                rec.put("stored", (id % 3 == 0) ? STORED_BASE + id : null);
                writer.write(rec);
            }
        }
        return file;
    }

    /** The 16-byte FIXED_LEN_BYTE_ARRAY value the multi-row-group fixture stores for row {@code id}. */
    static byte[] fixedUid(int id) {
        byte[] bytes = new byte[FIXED_UID_WIDTH];
        bytes[FIXED_UID_WIDTH - 4] = (byte) (id >>> 24);
        bytes[FIXED_UID_WIDTH - 3] = (byte) (id >>> 16);
        bytes[FIXED_UID_WIDTH - 2] = (byte) (id >>> 8);
        bytes[FIXED_UID_WIDTH - 1] = (byte) id;
        return bytes;
    }

    /**
     * Writes a one-column Parquet file whose single physical {@code INT64} column is named {@code columnName}, holding
     * the row index in each row. Used to assert that a read whose caller-named row-position column collides with this
     * physical column is rejected.
     *
     * @param dir directory in which to write the file
     * @param rows total number of rows to write
     * @param columnName the physical column's name
     * @return path to the written file
     */
    public static Path writeSingleLongColumnFile(Path dir, int rows, String columnName) throws IOException {
        Path file = dir.resolve("single-long-column.parquet");
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                columnName, Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(leaf), Optional.empty(), -1);
        ParquetSchema schema = new ParquetSchema(root);
        ColumnPath columnPath = ColumnPath.of(columnName);
        List<Map<ColumnPath, Object>> values = new ArrayList<>(rows);
        for (int row = 0; row < rows; row++) {
            values.add(Map.of(columnPath, (long) row));
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema)) {
            writer.writeBatch(WriteFixtures.batch(schema, values));
        }
        return file;
    }

    /**
     * Returns the number of row groups in the given Parquet file.
     *
     * <p>Opens a fresh source for the sole purpose of reading the footer, then closes it before returning.
     */
    public static int rowGroupCount(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return ParquetFormat.readFooter(source).rowGroups().size();
        }
    }

    /** Opens a {@link ByteRangeSource} over the local {@code file}; closing it closes the underlying channel. */
    public static ByteRangeSource openRangeReader(Path file) {
        return ByteRangeSource.ofFile(file);
    }
}
