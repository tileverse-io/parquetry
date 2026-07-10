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
package io.tileverse.parquetry.jackson;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.UuidConverter;
import io.tileverse.parquetry.variant.Variant;

import tools.jackson.core.JsonGenerator;
import tools.jackson.core.ObjectWriteContext;
import tools.jackson.core.StreamWriteFeature;
import tools.jackson.core.json.JsonFactory;

/**
 * Streams a {@link ParquetRecord} as JSON onto a caller-owned {@link JsonGenerator}, walking the projected schema in
 * lockstep with the row. The single source of JSON-mapping policy: scalars by physical kind (matching the flat CLI
 * renderer), structs as objects, lists as arrays, maps as objects (string keys) or entry arrays, and Variant inline by
 * its own type.
 */
public final class JsonRecordEncoder {

    // AUTO_CLOSE_TARGET is disabled because the stream conveniences write to a caller-owned OutputStream and must not
    // close it (closing stdout would break a CLI that keeps writing after the rows).
    private static final JsonFactory FACTORY =
            JsonFactory.builder().disable(StreamWriteFeature.AUTO_CLOSE_TARGET).build();

    private JsonRecordEncoder() {}

    /** Writes one row as a JSON object onto {@code generator}. */
    public static void writeObject(JsonGenerator generator, ParquetSchema schema, ParquetRecord row) {
        writeObjectBody(generator, schema.root(), row);
    }

    /** Writes one non-null value, resolved against its schema node. Used for delimited/text cells and by consumers. */
    public static void writeValue(JsonGenerator generator, SchemaNode node, Object value) {
        renderValue(generator, node, value);
    }

    /**
     * Writes each row as a JSON object on its own line (newline-delimited JSON) to {@code out}. This closes
     * {@code rows} (it is fully drained) and leaves {@code out} open for the caller to close.
     */
    public static void writeNdjson(OutputStream out, ParquetSchema schema, Stream<ParquetRecord> rows) {
        try (Stream<ParquetRecord> owned = rows;
                JsonGenerator generator = FACTORY.createGenerator(ObjectWriteContext.empty(), out)) {
            owned.forEach(row -> {
                writeObject(generator, schema, row);
                generator.writeRaw('\n');
            });
        }
    }

    /**
     * Writes the rows as a single JSON array to {@code out}. This closes {@code rows} (it is fully drained) and leaves
     * {@code out} open for the caller to close.
     */
    public static void writeArray(OutputStream out, ParquetSchema schema, Stream<ParquetRecord> rows) {
        try (Stream<ParquetRecord> owned = rows;
                JsonGenerator generator = FACTORY.createGenerator(ObjectWriteContext.empty(), out)) {
            generator.writeStartArray();
            owned.forEach(row -> writeObject(generator, schema, row));
            generator.writeEndArray();
        }
    }

    private static void writeObjectBody(JsonGenerator generator, SchemaNode.Group group, ParquetRecord row) {
        generator.writeStartObject();
        for (SchemaNode child : group.children()) {
            Object value = row.get(ColumnPath.of(child.name()));
            if (value == null) {
                continue;
            }
            generator.writeName(child.name());
            renderValue(generator, child, value);
        }
        generator.writeEndObject();
    }

    private static void renderValue(JsonGenerator generator, SchemaNode node, Object value) {
        switch (node) {
            case SchemaNode.Primitive primitive -> renderScalar(generator, primitive, value);
            case SchemaNode.Group group -> renderGroup(generator, group, value);
        }
    }

    private static void renderGroup(JsonGenerator generator, SchemaNode.Group group, Object value) {
        switch (JsonSchemaNodes.classify(group)) {
            case STRUCT -> writeObjectBody(generator, group, (ParquetRecord) value);
            case LIST -> renderList(generator, group, (List<?>) value);
            case MAP -> renderMap(generator, group, (Map<?, ?>) value);
            case VARIANT -> renderVariant(generator, (Variant) value);
        }
    }

    private static void renderList(JsonGenerator generator, SchemaNode.Group listGroup, List<?> values) {
        SchemaNode element = JsonSchemaNodes.elementNode(listGroup);
        generator.writeStartArray();
        for (Object item : values) {
            renderElement(generator, element, item);
        }
        generator.writeEndArray();
    }

    private static void renderMap(JsonGenerator generator, SchemaNode.Group mapGroup, Map<?, ?> entries) {
        SchemaNode keyNode = JsonSchemaNodes.keyNode(mapGroup);
        SchemaNode valueNode = JsonSchemaNodes.valueNode(mapGroup);
        if (hasStringKeys(keyNode)) {
            writeMapAsObject(generator, valueNode, entries);
        } else {
            writeMapAsEntries(generator, keyNode, valueNode, entries);
        }
    }

    private static void writeMapAsObject(JsonGenerator generator, SchemaNode valueNode, Map<?, ?> entries) {
        generator.writeStartObject();
        for (Map.Entry<?, ?> entry : entries.entrySet()) {
            generator.writeName(decodeKey(entry.getKey()));
            renderElement(generator, valueNode, entry.getValue());
        }
        generator.writeEndObject();
    }

    private static void writeMapAsEntries(
            JsonGenerator generator, SchemaNode keyNode, SchemaNode valueNode, Map<?, ?> entries) {
        generator.writeStartArray();
        for (Map.Entry<?, ?> entry : entries.entrySet()) {
            generator.writeStartObject();
            generator.writeName("key");
            renderValue(generator, keyNode, entry.getKey());
            generator.writeName("value");
            renderElement(generator, valueNode, entry.getValue());
            generator.writeEndObject();
        }
        generator.writeEndArray();
    }

    /** Renders one positional value (list element or map value); a null emits explicit JSON null. */
    private static void renderElement(JsonGenerator generator, SchemaNode node, Object value) {
        if (value == null) {
            generator.writeNull();
            return;
        }
        renderValue(generator, node, value);
    }

    private static boolean hasStringKeys(SchemaNode keyNode) {
        return keyNode instanceof SchemaNode.Primitive primitive
                && JsonSchemaNodes.isStringLike(primitive.logicalType());
    }

    private static String decodeKey(Object key) {
        return new String(((MemorySegment) key).toArray(JAVA_BYTE), StandardCharsets.UTF_8);
    }

    private static void renderVariant(JsonGenerator generator, Variant variant) {
        switch (variant.type()) {
            case NULL -> generator.writeNull();
            case BOOLEAN -> generator.writeBoolean(variant.getBoolean());
            case INT8 -> generator.writeNumber(variant.getByte());
            case INT16 -> generator.writeNumber(variant.getShort());
            case INT32 -> generator.writeNumber(variant.getInt());
            case INT64 -> generator.writeNumber(variant.getLong());
            case FLOAT -> generator.writeNumber(variant.getFloat());
            case DOUBLE -> generator.writeNumber(variant.getDouble());
            case DECIMAL4, DECIMAL8, DECIMAL16 -> generator.writeNumber(variant.getBigDecimal());
            case STRING -> generator.writeString(variant.getString());
            case UUID -> generator.writeString(variant.getUuid().toString());
            case BINARY ->
                generator.writeString(
                        Base64.getEncoder().encodeToString(variant.getBinary().toArray(JAVA_BYTE)));
            case DATE -> generator.writeNumber(variant.getDateDays());
            case TIME_NTZ -> generator.writeNumber(variant.getTimeMicros());
            case TIMESTAMP_TZ, TIMESTAMP_NTZ -> generator.writeNumber(variant.getTimestampMicros());
            case TIMESTAMP_NANOS_TZ, TIMESTAMP_NANOS_NTZ -> generator.writeNumber(variant.getTimestampNanos());
            case ARRAY -> renderVariantArray(generator, variant);
            case OBJECT -> renderVariantObject(generator, variant);
        }
    }

    private static void renderVariantArray(JsonGenerator generator, Variant variant) {
        generator.writeStartArray();
        int count = variant.numElements();
        for (int index = 0; index < count; index++) {
            renderVariant(generator, variant.getElement(index));
        }
        generator.writeEndArray();
    }

    private static void renderVariantObject(JsonGenerator generator, Variant variant) {
        generator.writeStartObject();
        for (String field : variant.fieldNames()) {
            generator.writeName(field);
            renderVariant(generator, variant.getField(field));
        }
        generator.writeEndObject();
    }

    private static void renderScalar(JsonGenerator generator, SchemaNode.Primitive primitive, Object value) {
        switch (primitive.kind()) {
            case BOOLEAN -> generator.writeBoolean((Boolean) value);
            case INT32 -> generator.writeNumber((Integer) value);
            case INT64 -> generator.writeNumber((Long) value);
            case FLOAT -> generator.writeNumber((Float) value);
            case DOUBLE -> generator.writeNumber((Double) value);
            default -> generator.writeString(binaryScalarToString(primitive, (MemorySegment) value));
        }
    }

    /**
     * Renders a binary-physical scalar to its display string: a UUID logical type as its canonical form, a string-like
     * logical type (string, enum, JSON) as UTF-8 text, and any other binary as Base64. This is the single source of the
     * binary-scalar display policy shared by the JSON encoder and the flat CLI renderer, keeping their outputs aligned.
     */
    public static String binaryScalarToString(SchemaNode.Primitive primitive, MemorySegment segment) {
        if (primitive.logicalType().orElse(null) instanceof LogicalType.UuidType) {
            return UuidConverter.fromSegment(segment).toString();
        }
        byte[] raw = segment.toArray(JAVA_BYTE);
        if (JsonSchemaNodes.isStringLike(primitive.logicalType())) {
            return new String(raw, StandardCharsets.UTF_8);
        }
        return Base64.getEncoder().encodeToString(raw);
    }
}
