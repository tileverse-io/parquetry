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
package io.tileverse.parquetry.avro;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Materializes a field's JSON default (the raw tree captured by the schema parser) into the runtime value mapping,
 * typed by the field's (reader) schema. Per the Avro spec, bytes and fixed defaults are JSON strings whose code points
 * 0-255 are the byte values, and a union default is validated against and typed by the union's first branch only.
 * Numeric defaults narrow to the field's type; float defaults lose precision per their JSON representation. Extra keys
 * in a record default object are ignored, matching avro-java. A malformed default raises {@link AvroFormatException}.
 */
final class DefaultValues {

    private DefaultValues() {}

    static Object materialize(AvroSchema schema, Object jsonDefault) {
        Object raw =
                switch (schema) {
                    case AvroSchema.Ref ref -> materialize(ref.target(), jsonDefault);
                    case AvroSchema.Union(List<AvroSchema> branches) -> materialize(branches.getFirst(), jsonDefault);
                    case AvroSchema.Primitive primitive -> primitiveDefault(primitive, jsonDefault);
                    case AvroSchema.Fixed fixed -> fixedDefault(fixed, jsonDefault);
                    case AvroSchema.Enum anEnum -> enumDefault(anEnum, jsonDefault);
                    case AvroSchema.Array array -> arrayDefault(array, jsonDefault);
                    case AvroSchema.Map map -> mapDefault(map, jsonDefault);
                    case AvroSchema.Record recordSchema -> recordDefault(recordSchema, jsonDefault);
                };
        if (schema instanceof AvroSchema.Ref || schema instanceof AvroSchema.Union) {
            // the recursive call already applied the target/branch logical conversion
            return raw;
        }
        return schema.logicalType()
                .map(logical -> LogicalValues.convert(logical, raw))
                .orElse(raw);
    }

    private static Object primitiveDefault(AvroSchema.Primitive primitive, Object jsonDefault) {
        return switch (primitive.type()) {
            case NULL -> requireNull(jsonDefault);
            case BOOLEAN -> require(Boolean.class, jsonDefault, "boolean");
            case INT -> intDefault(jsonDefault);
            case LONG -> require(Long.class, jsonDefault, "long");
            case FLOAT -> numberDefault(jsonDefault, "float").floatValue();
            case DOUBLE -> numberDefault(jsonDefault, "double").doubleValue();
            case STRING -> require(String.class, jsonDefault, "string");
            case BYTES -> isoBytes(jsonDefault);
            case RECORD, ARRAY, MAP, UNION, FIXED, ENUM ->
                throw new IllegalStateException("Primitive schema with composite type: " + primitive.type());
        };
    }

    private static Integer intDefault(Object jsonDefault) {
        Long value = require(Long.class, jsonDefault, "int");
        try {
            return Math.toIntExact(value);
        } catch (ArithmeticException outOfRange) {
            throw new AvroFormatException("Default value " + value + " is out of int range");
        }
    }

    private static Number numberDefault(Object jsonDefault, String typeName) {
        if (jsonDefault instanceof Long || jsonDefault instanceof Double) {
            return (Number) jsonDefault;
        }
        throw notOfType(jsonDefault, typeName);
    }

    private static MemorySegment fixedDefault(AvroSchema.Fixed schema, Object jsonDefault) {
        MemorySegment bytes = isoBytes(jsonDefault);
        if (bytes.byteSize() != schema.size()) {
            throw new AvroFormatException("Fixed " + schema.fullName() + " default has length " + bytes.byteSize()
                    + ", expected " + schema.size());
        }
        return bytes;
    }

    private static MemorySegment isoBytes(Object jsonDefault) {
        String text = require(String.class, jsonDefault, "binary string");
        int maxCodePoint = text.codePoints().max().orElse(0);
        if (maxCodePoint > 0xFF) {
            throw new AvroFormatException("Binary default has code point " + maxCodePoint + " above 255");
        }
        byte[] bytes = text.getBytes(StandardCharsets.ISO_8859_1);
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    private static String enumDefault(AvroSchema.Enum schema, Object jsonDefault) {
        String symbol = require(String.class, jsonDefault, "enum symbol");
        if (!schema.symbols().contains(symbol)) {
            throw new AvroFormatException("Default symbol " + symbol + " is not in enum " + schema.fullName());
        }
        return symbol;
    }

    private static List<Object> arrayDefault(AvroSchema.Array schema, Object jsonDefault) {
        List<?> elements = require(List.class, jsonDefault, "array");
        List<Object> materialized = new ArrayList<>(elements.size());
        for (Object element : elements) {
            materialized.add(materialize(schema.element(), element));
        }
        // unmodifiableList over ArrayList: a union-with-null element type makes null elements legal
        return Collections.unmodifiableList(materialized);
    }

    private static Map<String, Object> mapDefault(AvroSchema.Map schema, Object jsonDefault) {
        Map<?, ?> entries = require(Map.class, jsonDefault, "map");
        Map<String, Object> materialized = LinkedHashMap.newLinkedHashMap(entries.size());
        for (Map.Entry<?, ?> entry : entries.entrySet()) {
            String key = require(String.class, entry.getKey(), "map key string");
            materialized.put(key, materialize(schema.values(), entry.getValue()));
        }
        return Collections.unmodifiableMap(materialized);
    }

    private static AvroRecord recordDefault(AvroSchema.Record schema, Object jsonDefault) {
        Map<?, ?> jsonObject = require(Map.class, jsonDefault, "record object");
        Object[] values = new Object[schema.fields().size()];
        for (AvroSchema.Field field : schema.fields()) {
            values[field.position()] = recordFieldDefault(schema, field, jsonObject);
        }
        return new AvroRecord(schema, values);
    }

    private static Object recordFieldDefault(
            AvroSchema.Record recordSchema, AvroSchema.Field field, Map<?, ?> jsonObject) {
        if (jsonObject.containsKey(field.name())) {
            return materialize(field.schema(), jsonObject.get(field.name()));
        }
        if (field.hasDefault()) {
            return materialize(field.schema(), field.defaultValue());
        }
        throw new AvroFormatException("Record default for " + recordSchema.fullName() + " is missing field "
                + field.name()
                + ", which has no default of its own");
    }

    private static <T> T require(Class<T> type, Object jsonDefault, String simpleName) {
        if (type.isInstance(jsonDefault)) {
            return type.cast(jsonDefault);
        }
        throw notOfType(jsonDefault, simpleName);
    }

    private static Object requireNull(Object jsonDefault) {
        if (jsonDefault != null) {
            throw notOfType(jsonDefault, "null");
        }
        return null;
    }

    private static AvroFormatException notOfType(Object jsonDefault, String simpleName) {
        return new AvroFormatException("Default value " + jsonDefault + " is not a " + simpleName);
    }
}
