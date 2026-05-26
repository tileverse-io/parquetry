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
package io.tileverse.parquetry.data.write.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.data.WriteRow;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Validates that parquetry's V2 output reads cleanly through the parquet-java 1.14 reader. The avro flavour of the
 * reader is used for value-level deep-equal; the lower-level {@link ParquetFileReader} drives footer-level structural
 * assertions (row group count, codec per chunk).
 */
@Tag("conformance")
class ParquetJavaCompatIT {

    @TempDir
    Path tempDir;

    @Test
    void flatPrimitiveRoundTripReadsThroughParquetJava() throws Exception {
        ParquetSchema schema = flatPrimitiveSchema();
        List<Map<ColumnPath, Object>> rows = syntheticRows(100);
        Path file = tempDir.resolve("flat-primitive.parquet");
        writeRows(file, schema, WriteOptions.builder().tempDir(tempDir).build(), rows);

        List<GenericRecord> read = readWithAvro(file);
        assertThat(read).hasSize(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            assertRowMatches(rows.get(i), read.get(i));
        }
    }

    @Test
    void multiRowGroupOutputReadsThroughParquetJava() throws Exception {
        ParquetSchema schema = flatPrimitiveSchema();
        List<Map<ColumnPath, Object>> rows = syntheticRows(50);
        Path file = tempDir.resolve("multi-rg.parquet");
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(10L))
                .build();
        writeRows(file, schema, options, rows);

        ParquetMetadata metadata = readFooterViaParquetJava(file);
        assertThat(metadata.getBlocks()).hasSize(5);

        List<GenericRecord> read = readWithAvro(file);
        assertThat(read).hasSize(rows.size());
    }

    @Test
    void snappyAndZstdPagesAreParsedByParquetJava() throws Exception {
        for (Compression codec : List.of(Compression.snappy(), Compression.zstd(3))) {
            ParquetSchema schema = flatPrimitiveSchema();
            List<Map<ColumnPath, Object>> rows = syntheticRows(64);
            Path file = tempDir.resolve("compressed-" + codec.wireCodec().name() + ".parquet");
            WriteOptions options = WriteOptions.builder()
                    .tempDir(tempDir)
                    .defaultCompression(codec)
                    .build();
            writeRows(file, schema, options, rows);

            List<GenericRecord> read = readWithAvro(file);
            assertThat(read).hasSize(rows.size());
            for (int i = 0; i < rows.size(); i++) {
                assertRowMatches(rows.get(i), read.get(i));
            }
        }
    }

    @Test
    void perColumnCompressionShowsThroughInColumnChunkMetadata() throws Exception {
        ParquetSchema schema = flatPrimitiveSchema();
        List<Map<ColumnPath, Object>> rows = syntheticRows(100);
        Path file = tempDir.resolve("per-column-compression.parquet");
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .defaultCompression(Compression.zstd(3))
                .compression("name", Compression.snappy())
                .build();
        writeRows(file, schema, options, rows);

        ParquetMetadata metadata = readFooterViaParquetJava(file);
        for (BlockMetaData block : metadata.getBlocks()) {
            for (ColumnChunkMetaData chunk : block.getColumns()) {
                CompressionCodecName expected = "name".equals(chunk.getPath().toDotString())
                        ? CompressionCodecName.SNAPPY
                        : CompressionCodecName.ZSTD;
                assertThat(chunk.getCodec())
                        .as("codec for column %s", chunk.getPath().toDotString())
                        .isEqualTo(expected);
            }
        }
    }

    // --- row construction + comparison ---

    private static List<Map<ColumnPath, Object>> syntheticRows(int count) {
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Map<ColumnPath, Object> row = new HashMap<>(5);
            row.put(ColumnPath.of("id"), i);
            row.put(ColumnPath.of("ts"), 1_700_000_000_000L + i);
            row.put(ColumnPath.of("value"), Math.sin(i * 0.001));
            row.put(ColumnPath.of("flag"), i % 2 == 0);
            row.put(
                    ColumnPath.of("name"),
                    MemorySegment.ofArray(("name-" + (i % 32)).getBytes(StandardCharsets.UTF_8)));
            rows.add(row);
        }
        return rows;
    }

    private static void assertRowMatches(Map<ColumnPath, Object> expected, GenericRecord actual) {
        assertThat(((Number) actual.get("id")).intValue()).isEqualTo(expected.get(ColumnPath.of("id")));
        assertThat(((Number) actual.get("ts")).longValue()).isEqualTo(expected.get(ColumnPath.of("ts")));
        assertThat(((Number) actual.get("value")).doubleValue()).isEqualTo(expected.get(ColumnPath.of("value")));
        assertThat(actual.get("flag")).isEqualTo(expected.get(ColumnPath.of("flag")));
        MemorySegment expectedName = (MemorySegment) expected.get(ColumnPath.of("name"));
        byte[] expectedBytes = expectedName.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE);
        assertThat(toBytes(actual.get("name"))).isEqualTo(expectedBytes);
    }

    private static byte[] toBytes(Object value) {
        if (value instanceof Utf8 u) {
            return u.getBytes();
        }
        if (value instanceof ByteBuffer bb) {
            ByteBuffer dup = bb.duplicate();
            byte[] bytes = new byte[dup.remaining()];
            dup.get(bytes);
            return bytes;
        }
        if (value instanceof byte[] b) {
            return b;
        }
        if (value instanceof GenericData.Fixed f) {
            return f.bytes();
        }
        if (value instanceof CharSequence cs) {
            return cs.toString().getBytes(StandardCharsets.UTF_8);
        }
        throw new IllegalStateException("Unhandled name value type: " + value.getClass());
    }

    // --- writer driver ---

    private static void writeRows(
            Path file, ParquetSchema schema, WriteOptions options, List<Map<ColumnPath, Object>> rows)
            throws IOException {
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            for (Map<ColumnPath, Object> row : rows) {
                writer.write(WriteRow.of(row));
            }
        }
    }

    // --- parquet-java drivers ---

    private static List<GenericRecord> readWithAvro(Path file) throws IOException {
        List<GenericRecord> out = new ArrayList<>();
        try (ParquetReader<GenericData.Record> reader = AvroParquetReader.<GenericData.Record>builder(
                        new LocalInputFile(file))
                .build()) {
            GenericData.Record avroRecord;
            while ((avroRecord = reader.read()) != null) {
                out.add(avroRecord);
            }
        }
        return out;
    }

    private static ParquetMetadata readFooterViaParquetJava(Path file) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            return reader.getFooter();
        }
    }

    // --- schema ---

    private static ParquetSchema flatPrimitiveSchema() {
        List<SchemaNode> children = Stream.of(
                        primitive("id", PrimitiveKind.INT32),
                        primitive("ts", PrimitiveKind.INT64),
                        primitive("value", PrimitiveKind.DOUBLE),
                        primitive("flag", PrimitiveKind.BOOLEAN),
                        primitive("name", PrimitiveKind.BYTE_ARRAY))
                .map(f -> (SchemaNode) f)
                .toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive primitive(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.REQUIRED, kind, OptionalInt.empty(), Optional.empty(), -1);
    }
}
