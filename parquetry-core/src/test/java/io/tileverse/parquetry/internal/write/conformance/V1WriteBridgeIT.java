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
import java.nio.ByteOrder;
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
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Counterpart to {@link ParquetJavaCompatIT} for the V1.1 page-format bridge: validates that the legacy DATA_PAGE
 * output reads through the parquet-java reader, that dictionary encoding survives the bridge, and that the GeoParquet
 * 1.1 KV blob is forced on by the writer when V1.1 is selected.
 */
@Tag("conformance")
class V1WriteBridgeIT {

    @TempDir
    Path tempDir;

    @Test
    void v1FlatPrimitiveOutputReadsThroughParquetJava() throws Exception {
        ParquetSchema schema = flatPrimitiveSchema();
        List<Map<ColumnPath, Object>> rows = syntheticRows(100);
        Path file = tempDir.resolve("v1-flat.parquet");
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .parquetVersion(ParquetVersion.V1_1)
                .build();
        writeRows(file, schema, options, rows);

        List<GenericRecord> read = WriteConformanceSupport.readWithAvro(file);
        assertThat(read).hasSize(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            assertRowMatches(rows.get(i), read.get(i));
        }
    }

    @Test
    void v1DictionaryEncodedOutputReadsThroughParquetJava() throws Exception {
        ParquetSchema schema = lowCardinalitySchema();
        List<Map<ColumnPath, Object>> rows = lowCardinalityRows(500);
        Path file = tempDir.resolve("v1-dict.parquet");
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .parquetVersion(ParquetVersion.V1_1)
                .build();
        writeRows(file, schema, options, rows);

        List<GenericRecord> read = WriteConformanceSupport.readWithAvro(file);
        assertThat(read).hasSize(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            MemorySegment expected = (MemorySegment) rows.get(i).get(ColumnPath.of("category"));
            byte[] expectedBytes = expected.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE);
            assertThat(toBytes(read.get(i).get("category"))).isEqualTo(expectedBytes);
        }
    }

    @Test
    void v1WithGeometryColumnEmitsGeoKvInFooter() throws Exception {
        ParquetSchema schema = geometrySchema();
        Path file = tempDir.resolve("v1-geo.parquet");
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .parquetVersion(ParquetVersion.V1_1)
                .crsEpsg("geometry", 4326)
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(WriteFixtures.batch(
                    schema, List.of(Map.of(ColumnPath.of("geometry"), MemorySegment.ofArray(wkbPoint(1.0, 2.0))))));
        }

        ParquetMetadata metadata = WriteConformanceSupport.readFooterViaParquetJava(file);
        Map<String, String> kv = metadata.getFileMetaData().getKeyValueMetaData();
        assertThat(kv).as("V1.1 must emit the GeoParquet 1.1 'geo' KV blob").containsKey("geo");
        assertThat(kv.get("geo")).contains("\"version\":\"1.1.0\"");
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

    private static List<Map<ColumnPath, Object>> lowCardinalityRows(int count) {
        String[] categories = {"red", "green", "blue", "yellow"};
        List<Map<ColumnPath, Object>> rows = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Map<ColumnPath, Object> row = new HashMap<>(2);
            row.put(ColumnPath.of("id"), i);
            row.put(
                    ColumnPath.of("category"),
                    MemorySegment.ofArray(categories[i % categories.length].getBytes(StandardCharsets.UTF_8)));
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
        throw new IllegalStateException("Unhandled value type: " + value.getClass());
    }

    // --- writer driver ---

    private static void writeRows(
            Path file, ParquetSchema schema, WriteOptions options, List<Map<ColumnPath, Object>> rows)
            throws IOException {
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }
    }

    // --- schemas ---

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

    private static ParquetSchema lowCardinalitySchema() {
        List<SchemaNode> children = Stream.of(
                        primitive("id", PrimitiveKind.INT32), primitive("category", PrimitiveKind.BYTE_ARRAY))
                .map(f -> (SchemaNode) f)
                .toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static ParquetSchema geometrySchema() {
        List<SchemaNode> children = Stream.<SchemaNode>of(primitive("geometry", PrimitiveKind.BYTE_ARRAY))
                .toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive primitive(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.REQUIRED, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static byte[] wkbPoint(double x, double y) {
        ByteBuffer bb = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 1);
        bb.putInt(1);
        bb.putDouble(x);
        bb.putDouble(y);
        return bb.array();
    }
}
