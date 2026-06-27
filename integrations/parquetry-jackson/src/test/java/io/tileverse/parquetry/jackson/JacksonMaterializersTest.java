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
package io.tileverse.parquetry.jackson;

import static io.tileverse.parquetry.jackson.JacksonRecords.primitive;
import static io.tileverse.parquetry.jackson.JacksonRecords.root;
import static io.tileverse.parquetry.jackson.JacksonRecords.stringLeaf;
import static io.tileverse.parquetry.jackson.JacksonRecords.utf8;
import static io.tileverse.parquetry.jackson.JacksonRecords.validBits;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

import tools.jackson.databind.JsonNode;

class JacksonMaterializersTest {

    private static DefaultParquetRecordBatch citiesBatch(ParquetSchema schema) {
        Validity present = validBits(1);
        Map<ColumnPath, ColumnVector> columns = Map.of(
                ColumnPath.of("id"), IntVector.materialized(new int[] {1}, present),
                ColumnPath.of("name"), BinaryVector.materialized(new MemorySegment[] {utf8("Rosario")}, present));
        return new DefaultParquetRecordBatch(schema, columns, 1, Arena.ofConfined());
    }

    @Test
    void jsonNodeMaterializerYieldsTree() {
        SchemaNode.Group rootGroup = root(primitive("id", PrimitiveKind.INT32), stringLeaf("name"));
        ParquetSchema schema = new ParquetSchema(rootGroup);
        Materializer<JsonNode> materializer = JacksonMaterializers.jsonNode();
        try (DefaultParquetRecordBatch batch = citiesBatch(schema)) {
            JsonNode node = materializer.materialize(schema, batch.materialize(0));
            assertThat(node.get("id").intValue()).isEqualTo(1);
            assertThat(node.get("name").stringValue()).isEqualTo("Rosario");
        }
    }

    @Test
    void jsonStringMaterializerYieldsCompactString() {
        SchemaNode.Group rootGroup = root(primitive("id", PrimitiveKind.INT32), stringLeaf("name"));
        ParquetSchema schema = new ParquetSchema(rootGroup);
        Materializer<String> materializer = JacksonMaterializers.jsonString();
        try (DefaultParquetRecordBatch batch = citiesBatch(schema)) {
            String json = materializer.materialize(schema, batch.materialize(0));
            assertThat(json).isEqualTo("{\"id\":1,\"name\":\"Rosario\"}");
        }
    }

    @Test
    void writeNdjsonEmitsOneObjectPerLine() {
        SchemaNode.Group rootGroup = root(primitive("id", PrimitiveKind.INT32), stringLeaf("name"));
        ParquetSchema schema = new ParquetSchema(rootGroup);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DefaultParquetRecordBatch batch = citiesBatch(schema)) {
            ParquetRecord row = batch.materialize(0);
            JsonRecordEncoder.writeNdjson(out, schema, Stream.of(row, row));
        }
        String text = out.toString(StandardCharsets.UTF_8);
        assertThat(text.strip().split("\n")).hasSize(2);
        assertThat(text).startsWith("{\"id\":1,\"name\":\"Rosario\"}");
    }

    @Test
    void writeNdjsonClosesTheRowStreamAndLeavesTheOutputStreamOpen() {
        SchemaNode.Group rootGroup = root(primitive("id", PrimitiveKind.INT32), stringLeaf("name"));
        ParquetSchema schema = new ParquetSchema(rootGroup);
        AtomicBoolean outClosed = new AtomicBoolean(false);
        ByteArrayOutputStream out = new ByteArrayOutputStream() {
            @Override
            public void close() {
                outClosed.set(true);
            }
        };
        AtomicBoolean rowsClosed = new AtomicBoolean(false);
        try (DefaultParquetRecordBatch batch = citiesBatch(schema)) {
            ParquetRecord row = batch.materialize(0);
            Stream<ParquetRecord> rows = Stream.of(row).onClose(() -> rowsClosed.set(true));
            JsonRecordEncoder.writeNdjson(out, schema, rows);
        }
        assertThat(rowsClosed).as("row stream closed by writeNdjson").isTrue();
        assertThat(outClosed).as("caller's OutputStream left open").isFalse();
    }
}
