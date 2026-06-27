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

import static io.tileverse.parquetry.jackson.JacksonRecords.list;
import static io.tileverse.parquetry.jackson.JacksonRecords.map;
import static io.tileverse.parquetry.jackson.JacksonRecords.primitive;
import static io.tileverse.parquetry.jackson.JacksonRecords.root;
import static io.tileverse.parquetry.jackson.JacksonRecords.stringLeaf;
import static io.tileverse.parquetry.jackson.JacksonRecords.struct;
import static io.tileverse.parquetry.jackson.JacksonRecords.utf8;
import static io.tileverse.parquetry.jackson.JacksonRecords.uuidLeaf;
import static io.tileverse.parquetry.jackson.JacksonRecords.validBits;
import static io.tileverse.parquetry.jackson.JacksonRecords.variant;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.StringWriter;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Base64;
import java.util.BitSet;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.BooleanVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.MapVector;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.columnar.VariantVector;
import io.tileverse.parquetry.internal.variant.VariantEncoder;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.UuidConverter;

import tools.jackson.core.JsonGenerator;
import tools.jackson.core.ObjectWriteContext;
import tools.jackson.core.json.JsonFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class JsonRecordEncoderTest {

    private static final JsonFactory FACTORY = new JsonFactory();
    private static final JsonMapper MAPPER = JsonMapper.shared();

    private static JsonNode encode(ParquetSchema schema, ParquetRecord row) {
        StringWriter writer = new StringWriter();
        try (JsonGenerator generator = FACTORY.createGenerator(ObjectWriteContext.empty(), writer)) {
            JsonRecordEncoder.writeObject(generator, schema, row);
        }
        return MAPPER.readTree(writer.toString());
    }

    @Test
    void rendersScalarsAndOmitsNullFields() {
        SchemaNode.Group rootGroup = root(
                primitive("flag", PrimitiveKind.BOOLEAN),
                primitive("count", PrimitiveKind.INT32),
                stringLeaf("name"),
                primitive("blob", PrimitiveKind.BYTE_ARRAY),
                stringLeaf("absent"));
        ParquetSchema schema = new ParquetSchema(rootGroup);

        Validity present = validBits(1);
        Validity missing = Validity.of(new BitSet(1), 1);
        byte[] rawBlob = {1, 2, 3};
        Map<ColumnPath, ColumnVector> columns = Map.of(
                ColumnPath.of("flag"), BooleanVector.materialized(new boolean[] {true}, present),
                ColumnPath.of("count"), IntVector.materialized(new int[] {7}, present),
                ColumnPath.of("name"), BinaryVector.materialized(new MemorySegment[] {utf8("Rosario")}, present),
                ColumnPath.of("blob"),
                        BinaryVector.materialized(new MemorySegment[] {MemorySegment.ofArray(rawBlob)}, present),
                ColumnPath.of("absent"), BinaryVector.materialized(new MemorySegment[] {utf8("")}, missing));

        try (DefaultParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 1, Arena.ofConfined())) {
            JsonNode node = encode(schema, batch.materialize(0));
            assertThat(node.get("flag").booleanValue()).isTrue();
            assertThat(node.get("count").intValue()).isEqualTo(7);
            assertThat(node.get("name").stringValue()).isEqualTo("Rosario");
            assertThat(node.get("blob").stringValue())
                    .isEqualTo(Base64.getEncoder().encodeToString(rawBlob));
            assertThat(node.has("absent")).isFalse();
        }
    }

    @Test
    void rendersUuidColumnAsCanonicalString() {
        UUID uuid = UUID.fromString("0123456f-89ab-cdef-fedc-ba9876543210");
        SchemaNode.Group rootGroup = root(uuidLeaf("id"));
        ParquetSchema schema = new ParquetSchema(rootGroup);

        Validity present = validBits(1);
        Map<ColumnPath, ColumnVector> columns = Map.of(
                ColumnPath.of("id"),
                BinaryVector.materialized(new MemorySegment[] {UuidConverter.toReadOnlySegment(uuid)}, present));

        try (DefaultParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 1, Arena.ofConfined())) {
            JsonNode node = encode(schema, batch.materialize(0));
            assertThat(node.get("id").stringValue()).isEqualTo(uuid.toString());
        }
    }

    @Test
    void rendersNestedStruct() {
        SchemaNode.Group addr = struct("addr", primitive("zip", PrimitiveKind.INT32), stringLeaf("city"));
        SchemaNode.Group rootGroup = root(primitive("id", PrimitiveKind.INT32), addr);
        ParquetSchema schema = new ParquetSchema(rootGroup);

        Validity present = validBits(1);
        StructVector addrVector = new StructVector(
                Map.of(
                        ColumnPath.of("zip"), IntVector.materialized(new int[] {2000}, present),
                        ColumnPath.of("city"),
                                BinaryVector.materialized(new MemorySegment[] {utf8("Rosario")}, present)),
                present,
                1);
        Map<ColumnPath, ColumnVector> columns = Map.of(
                ColumnPath.of("id"), IntVector.materialized(new int[] {1}, present), ColumnPath.of("addr"), addrVector);

        try (DefaultParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 1, Arena.ofConfined())) {
            JsonNode node = encode(schema, batch.materialize(0));
            assertThat(node.get("id").intValue()).isEqualTo(1);
            assertThat(node.get("addr").get("zip").intValue()).isEqualTo(2000);
            assertThat(node.get("addr").get("city").stringValue()).isEqualTo("Rosario");
        }
    }

    @Test
    void rendersListOfNumbersWithNullElement() {
        SchemaNode.Group nums = list("nums", primitive("element", PrimitiveKind.INT32));
        SchemaNode.Group rootGroup = root(nums);
        ParquetSchema schema = new ParquetSchema(rootGroup);

        Validity rowValid = validBits(1);
        BitSet elementValidBits = new BitSet(3);
        elementValidBits.set(0);
        elementValidBits.set(2);
        Validity elementValid = Validity.of(elementValidBits, 3);
        IntVector elements = IntVector.materialized(new int[] {10, 0, 30}, elementValid);
        ListVector listVector = new ListVector(new int[] {0, 3}, elements, rowValid, 1);
        Map<ColumnPath, ColumnVector> columns = Map.of(ColumnPath.of("nums"), listVector);

        try (DefaultParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 1, Arena.ofConfined())) {
            JsonNode node = encode(schema, batch.materialize(0));
            JsonNode array = node.get("nums");
            assertThat(array.isArray()).isTrue();
            assertThat(array.get(0).intValue()).isEqualTo(10);
            assertThat(array.get(1).isNull()).isTrue();
            assertThat(array.get(2).intValue()).isEqualTo(30);
        }
    }

    @Test
    void rendersMapWithStringKeysAsObjectAndIntKeysAsEntries() {
        SchemaNode.Group strMap = map("labels", stringLeaf("key"), primitive("value", PrimitiveKind.INT32));
        SchemaNode.Group intMap =
                map("scores", primitive("key", PrimitiveKind.INT32), primitive("value", PrimitiveKind.DOUBLE));
        SchemaNode.Group rootGroup = root(strMap, intMap);
        ParquetSchema schema = new ParquetSchema(rootGroup);

        Validity rowValid = validBits(1);
        Validity two = validBits(2);
        MapVector labels = new MapVector(
                new int[] {0, 2},
                BinaryVector.materialized(new MemorySegment[] {utf8("a"), utf8("b")}, two),
                IntVector.materialized(new int[] {1, 2}, two),
                rowValid,
                1);
        MapVector scores = new MapVector(
                new int[] {0, 1},
                IntVector.materialized(new int[] {42}, validBits(1)),
                DoubleVector.materialized(new double[] {9.5}, validBits(1)),
                rowValid,
                1);
        Map<ColumnPath, ColumnVector> columns =
                Map.of(ColumnPath.of("labels"), labels, ColumnPath.of("scores"), scores);

        try (DefaultParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 1, Arena.ofConfined())) {
            JsonNode node = encode(schema, batch.materialize(0));
            assertThat(node.get("labels").get("a").intValue()).isEqualTo(1);
            assertThat(node.get("labels").get("b").intValue()).isEqualTo(2);
            JsonNode entries = node.get("scores");
            assertThat(entries.isArray()).isTrue();
            assertThat(entries.get(0).get("key").intValue()).isEqualTo(42);
            assertThat(entries.get(0).get("value").doubleValue()).isEqualTo(9.5);
        }
    }

    @Test
    void rendersVariantObjectInline() {
        SchemaNode.Group v = variant("v");
        SchemaNode.Group rootGroup = root(v);
        ParquetSchema schema = new ParquetSchema(rootGroup);

        VariantEncoder encoder = new VariantEncoder();
        encoder.startObject();
        encoder.field("n").addInt(42);
        encoder.field("label").addString("hi");
        encoder.endObject();
        VariantEncoder.Encoded encoded = encoder.encode();

        Validity present = validBits(1);
        VariantVector variantVector = new VariantVector(
                BinaryVector.materialized(new MemorySegment[] {encoded.metadata()}, present),
                BinaryVector.materialized(new MemorySegment[] {encoded.value()}, present),
                present,
                1);
        Map<ColumnPath, ColumnVector> columns = Map.of(ColumnPath.of("v"), variantVector);

        try (DefaultParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 1, Arena.ofConfined())) {
            JsonNode node = encode(schema, batch.materialize(0));
            assertThat(node.get("v").get("n").intValue()).isEqualTo(42);
            assertThat(node.get("v").get("label").stringValue()).isEqualTo("hi");
        }
    }
}
