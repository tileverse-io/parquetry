/*
 * Copyright (c) 2026 Multivers.io
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
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.internal.write.WriteFixtures;
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

    @Test
    void dictionaryOverflowChunkReadsThroughParquetJava() throws Exception {
        // The first pages dictionary-encode a few repeated values (RLE_DICTIONARY); later distinct values overflow the
        // dictionary byte budget and fall back to PLAIN. The early dictionary pages index into the chunk dictionary,
        // which the reader can only find when the writer keeps the dictionary page.
        ParquetSchema schema = singleBinaryColumnSchema("label");
        int rowCount = 100;
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            String value = i < 60 ? "dup-" + (i % 4) : "uniq-" + i;
            Map<ColumnPath, Object> row = new HashMap<>(1);
            row.put(ColumnPath.of("label"), MemorySegment.ofArray(value.getBytes(StandardCharsets.UTF_8)));
            rows.add(row);
        }
        Path file = tempDir.resolve("dictionary-overflow.parquet");
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(10)
                .dictionaryByteLimit(40)
                .build();
        writeRows(file, schema, options, rows);

        List<GenericRecord> read = readWithAvro(file);
        assertThat(read).hasSize(rowCount);
        for (int i = 0; i < rowCount; i++) {
            String expected = i < 60 ? "dup-" + (i % 4) : "uniq-" + i;
            assertThat(new String(toBytes(read.get(i).get("label")), StandardCharsets.UTF_8))
                    .as("row %d", i)
                    .isEqualTo(expected);
        }
    }

    @Test
    void allNullColumnsParseThroughParquetJava() throws Exception {
        // An all-null column records a null count but no min/max. The absent min/max must be omitted on the wire; a
        // present zero-byte min/max makes parquet-java's footer parse throw in IntStatistics.setMinMaxFromBytes.
        ParquetSchema schema = nullableIntAndDoubleSchema();
        int rowCount = 500;
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            rows.add(new HashMap<>());
        }
        Path file = tempDir.resolve("all-null.parquet");
        writeRows(file, schema, WriteOptions.builder().tempDir(tempDir).build(), rows);

        ParquetMetadata metadata = readFooterViaParquetJava(file);
        for (BlockMetaData block : metadata.getBlocks()) {
            for (ColumnChunkMetaData chunk : block.getColumns()) {
                String column = chunk.getPath().toDotString();
                assertThat(chunk.getStatistics().getNumNulls())
                        .as("null count for %s", column)
                        .isEqualTo(rowCount);
                assertThat(chunk.getStatistics().hasNonNullValue())
                        .as("no min/max recorded for %s", column)
                        .isFalse();
            }
        }

        List<GenericRecord> read = readWithAvro(file);
        assertThat(read).hasSize(rowCount);
        for (GenericRecord row : read) {
            assertThat(row.get("level")).isNull();
            assertThat(row.get("height")).isNull();
        }
    }

    @Test
    void singleDistinctValueAcrossPagesReadsThroughParquetJava() throws Exception {
        // A chunk dictionary with one distinct value indexes every entry as 0, a zero-bit-width index stream. The
        // values span two data pages, placing a page of non-null indices after an all-null page. The index stream must
        // still emit a run header; an empty one makes parquet-java throw "Reading past RLE/BitPacking stream" when it
        // reads the first value of the second page.
        ParquetSchema schema = nullableIntAndDoubleSchema();
        int rowCount = 40;
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            Map<ColumnPath, Object> row = new HashMap<>(1);
            if (i >= 20 && i % 2 == 0) {
                row.put(ColumnPath.of("level"), 7);
            }
            rows.add(row);
        }
        Path file = tempDir.resolve("single-distinct-two-page.parquet");
        WriteOptions options =
                WriteOptions.builder().tempDir(tempDir).pageValueLimit(20).build();
        writeRows(file, schema, options, rows);

        List<GenericRecord> read = readWithAvro(file);
        assertThat(read).hasSize(rowCount);
        int nonNull = 0;
        for (int i = 0; i < rowCount; i++) {
            Object level = read.get(i).get("level");
            if (i >= 20 && i % 2 == 0) {
                assertThat(((Number) level).intValue()).as("row %d", i).isEqualTo(7);
                nonNull++;
            } else {
                assertThat(level).as("row %d", i).isNull();
            }
        }
        assertThat(nonNull).isEqualTo(10);
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
        // One row per appender batch lets the writer's own row-group sizing policy place boundaries exactly, which the
        // multi-row-group compatibility case depends on.
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender(1);
            for (Map<ColumnPath, Object> row : rows) {
                WriteFixtures.appendRow(appender, schema, row);
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

    private static ParquetSchema nullableIntAndDoubleSchema() {
        List<SchemaNode> children = List.of(
                optionalPrimitive("level", PrimitiveKind.INT32), optionalPrimitive("height", PrimitiveKind.DOUBLE));
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive optionalPrimitive(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static ParquetSchema singleBinaryColumnSchema(String name) {
        SchemaNode.Group root = new SchemaNode.Group(
                "schema",
                Repetition.REQUIRED,
                List.<SchemaNode>of(primitive(name, PrimitiveKind.BYTE_ARRAY)),
                Optional.empty(),
                -1);
        return new ParquetSchema(root);
    }
}
