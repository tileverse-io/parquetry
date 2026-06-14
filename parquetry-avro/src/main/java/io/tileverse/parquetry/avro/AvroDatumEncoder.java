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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Encodes one Avro datum of a given {@link AvroSchema} into an {@link AvroBinaryEncoder}, inverting
 * {@link AvroDatumDecoder}. A record datum may be an {@link AvroRecord} or a {@code Map} by field name; absent fields
 * fall back to their schema default and unknown map keys are rejected. Unions select the first branch whose type
 * accepts the runtime value, in declared order. Thread-confined; reuse within one block encode is fine.
 */
final class AvroDatumEncoder {

    private final LogicalEncoding logicalEncoding = new LogicalEncoding();

    void encode(AvroSchema schema, Object value, AvroBinaryEncoder out) {
        switch (schema) {
            case AvroSchema.Primitive primitive -> encodePrimitive(primitive, value, out);
            case AvroSchema.Record record -> encodeRecord(record, value, out);
            case AvroSchema.Union union -> encodeUnion(union, value, out);
            case AvroSchema.Array array -> encodeArray(array, value, out);
            case AvroSchema.Map map -> encodeMap(map, value, out);
            case AvroSchema.Fixed fixed -> encodeFixed(fixed, value, out);
            case AvroSchema.Enum anEnum -> encodeEnum(anEnum, value, out);
            case AvroSchema.Ref ref -> encode(ref.target(), value, out);
        }
    }

    private void encodePrimitive(AvroSchema.Primitive primitive, Object value, AvroBinaryEncoder out) {
        if (logicalEncoding.encodes(primitive, value, out)) {
            return;
        }
        switch (primitive.type()) {
            case NULL -> requireNull(value);
            case BOOLEAN -> out.writeBoolean(ValueCoercion.asBoolean(value));
            case INT -> out.writeInt(ValueCoercion.asInt(value));
            case LONG -> out.writeLong(ValueCoercion.asLong(value));
            case FLOAT -> out.writeFloat(ValueCoercion.asFloat(value));
            case DOUBLE -> out.writeDouble(ValueCoercion.asDouble(value));
            case STRING -> out.writeString(ValueCoercion.asString(value));
            case BYTES -> out.writeBytes(ValueCoercion.asBytes(value));
            default -> throw new AvroFormatException("Not a primitive: " + primitive.type());
        }
    }

    private void requireNull(Object value) {
        if (value != null) {
            throw new AvroFormatException("Expected null for a null-typed field but got "
                    + value.getClass().getName());
        }
    }

    private void encodeRecord(AvroSchema.Record record, Object value, AvroBinaryEncoder out) {
        switch (value) {
            case AvroRecord avroRecord -> encodeRecordFromPositions(record, avroRecord, out);
            case Map<?, ?> map -> encodeRecordFromMap(record, map, out);
            default ->
                throw new AvroFormatException("A record value must be an AvroRecord or a Map, got " + describe(value));
        }
    }

    private void encodeRecordFromPositions(AvroSchema.Record record, AvroRecord value, AvroBinaryEncoder out) {
        List<AvroSchema.Field> fields = record.fields();
        for (int i = 0; i < fields.size(); i++) {
            encode(fields.get(i).schema(), value.get(i), out);
        }
    }

    private void encodeRecordFromMap(AvroSchema.Record record, Map<?, ?> map, AvroBinaryEncoder out) {
        Set<String> seen = new LinkedHashSet<>();
        for (AvroSchema.Field field : record.fields()) {
            seen.add(field.name());
            encode(field.schema(), valueForField(field, map), out);
        }
        rejectUnknownKeys(map, seen);
    }

    private Object valueForField(AvroSchema.Field field, Map<?, ?> map) {
        if (map.containsKey(field.name())) {
            return map.get(field.name());
        }
        if (field.hasDefault()) {
            return DefaultValues.materialize(field.schema(), field.defaultValue());
        }
        throw new AvroFormatException("Missing field '" + field.name() + "' and no default is declared");
    }

    private void rejectUnknownKeys(Map<?, ?> map, Set<String> known) {
        for (Object key : map.keySet()) {
            if (!known.contains(String.valueOf(key))) {
                throw new AvroFormatException("Unknown record field '" + key + "'");
            }
        }
    }

    private void encodeUnion(AvroSchema.Union union, Object value, AvroBinaryEncoder out) {
        List<AvroSchema> branches = union.branches();
        for (int i = 0; i < branches.size(); i++) {
            if (UnionBranchMatch.accepts(branches.get(i), value)) {
                out.writeInt(i);
                encode(branches.get(i), value, out);
                return;
            }
        }
        throw new AvroFormatException("No union branch accepts " + describe(value) + " among " + branches);
    }

    private void encodeArray(AvroSchema.Array array, Object value, AvroBinaryEncoder out) {
        Collection<?> items = asCollection(value);
        if (!items.isEmpty()) {
            out.writeLong(items.size());
            for (Object item : items) {
                encode(array.element(), item, out);
            }
        }
        out.writeLong(0);
    }

    private void encodeMap(AvroSchema.Map map, Object value, AvroBinaryEncoder out) {
        if (!(value instanceof Map<?, ?> entries)) {
            throw new AvroFormatException("A map value must be a Map, got " + describe(value));
        }
        if (!entries.isEmpty()) {
            out.writeLong(entries.size());
            for (Map.Entry<?, ?> entry : entries.entrySet()) {
                out.writeString(String.valueOf(entry.getKey()));
                encode(map.values(), entry.getValue(), out);
            }
        }
        out.writeLong(0);
    }

    private void encodeEnum(AvroSchema.Enum anEnum, Object value, AvroBinaryEncoder out) {
        String symbol = ValueCoercion.asString(value);
        int index = anEnum.symbols().indexOf(symbol);
        if (index < 0) {
            throw new AvroFormatException("Symbol '" + symbol + "' is not in enum " + anEnum.symbols());
        }
        out.writeInt(index);
    }

    private void encodeFixed(AvroSchema.Fixed fixed, Object value, AvroBinaryEncoder out) {
        if (logicalEncoding.encodes(fixed, value, out)) {
            return;
        }
        byte[] bytes = ValueCoercion.asBytes(value);
        if (bytes.length != fixed.size()) {
            throw new AvroFormatException(
                    "Fixed '" + fixed.name() + "' needs " + fixed.size() + " bytes, got " + bytes.length);
        }
        out.writeFixed(bytes);
    }

    private Collection<?> asCollection(Object value) {
        if (value instanceof Collection<?> collection) {
            return collection;
        }
        if (value != null && value.getClass().isArray()) {
            return arrayToList(value);
        }
        throw new AvroFormatException("An array value must be a Collection or array, got " + describe(value));
    }

    private List<Object> arrayToList(Object value) {
        int length = Array.getLength(value);
        List<Object> items = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            items.add(Array.get(value, i));
        }
        return items;
    }

    private String describe(Object value) {
        return value == null ? "null" : value.getClass().getName();
    }
}
