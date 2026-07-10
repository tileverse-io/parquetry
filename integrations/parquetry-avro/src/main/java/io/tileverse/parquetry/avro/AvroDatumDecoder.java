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
package io.tileverse.parquetry.avro;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Decodes one Avro datum of a given {@link AvroSchema} from an {@link AvroBinaryDecoder}, producing the Java value
 * mapping documented on {@link AvroRecord}. A depth guard bounds structural nesting; recursive schemas over malicious
 * data cannot overflow the stack. Thread-confined; reuse within a single block decode is fine.
 */
final class AvroDatumDecoder {

    static final int MAX_DEPTH = 1000;

    /**
     * Caps a block whose items occupy zero bytes on the wire (an array of nulls, of empty records). The input length
     * cannot bound such a block: a few crafted bytes could declare a count that allocates gigabytes. No plausible
     * writer emits a million zero-byte items in one block; this is a documented decode limit, not an Avro one.
     */
    static final long MAX_ZERO_BYTE_ITEMS_PER_BLOCK = 1_000_000;

    Object decode(AvroSchema schema, AvroBinaryDecoder in) {
        return decode(schema, in, 0);
    }

    private Object decode(AvroSchema schema, AvroBinaryDecoder in, int depth) {
        if (depth > MAX_DEPTH) {
            throw new AvroFormatException("Datum nesting exceeds maximum depth " + MAX_DEPTH);
        }
        return switch (schema) {
            case AvroSchema.Primitive primitive -> decodePrimitive(primitive, in);
            case AvroSchema.Record recordSchema -> decodeRecord(recordSchema, in, depth);
            case AvroSchema.Union union -> decodeUnion(union, in, depth);
            case AvroSchema.Array array -> decodeArray(array, in, depth);
            case AvroSchema.Map map -> decodeMap(map, in, depth);
            case AvroSchema.Fixed fixed -> convertIfLogical(fixed, in.fixed(fixed.size()));
            case AvroSchema.Enum anEnum -> symbol(anEnum, in.readInt());
            case AvroSchema.Ref ref -> decode(ref.target(), in, depth + 1);
        };
    }

    private Object decodePrimitive(AvroSchema.Primitive primitive, AvroBinaryDecoder in) {
        Object raw =
                switch (primitive.type()) {
                    case NULL -> null;
                    case BOOLEAN -> in.readBoolean();
                    case INT -> in.readInt();
                    case LONG -> in.readLong();
                    case FLOAT -> in.readFloat();
                    case DOUBLE -> in.readDouble();
                    case STRING -> in.readString();
                    case BYTES -> in.readBytes();
                    default -> throw new AvroFormatException("Not a primitive: " + primitive.type());
                };
        return convertIfLogical(primitive, raw);
    }

    private Object convertIfLogical(AvroSchema schema, Object raw) {
        return schema.logicalType()
                .map(logical -> LogicalValues.convert(logical, raw))
                .orElse(raw);
    }

    private AvroRecord decodeRecord(AvroSchema.Record recordSchema, AvroBinaryDecoder in, int depth) {
        List<AvroSchema.Field> fields = recordSchema.fields();
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            values[i] = decode(fields.get(i).schema(), in, depth + 1);
        }
        return new AvroRecord(recordSchema, values);
    }

    private Object decodeUnion(AvroSchema.Union union, AvroBinaryDecoder in, int depth) {
        int index = in.readInt();
        List<AvroSchema> branches = union.branches();
        if (index < 0 || index >= branches.size()) {
            throw new AvroFormatException("Union branch index out of range: " + index);
        }
        return decode(branches.get(index), in, depth + 1);
    }

    private List<Object> decodeArray(AvroSchema.Array array, AvroBinaryDecoder in, int depth) {
        boolean zeroByteItems = DatumSkipper.minimumWireSize(array.element()) == 0;
        List<Object> items = new ArrayList<>();
        for (long count = blockCount(in); count != 0; count = blockCount(in)) {
            requirePlausibleItemCount(count, zeroByteItems, in);
            for (long i = 0; i < count; i++) {
                items.add(decode(array.element(), in, depth + 1));
            }
        }
        return items;
    }

    private Map<String, Object> decodeMap(AvroSchema.Map map, AvroBinaryDecoder in, int depth) {
        Map<String, Object> entries = new LinkedHashMap<>();
        for (long count = blockCount(in); count != 0; count = blockCount(in)) {
            // map keys cost at least the length varint; the remaining input always bounds the count
            in.requireBlockCountWithinInput(count);
            for (long i = 0; i < count; i++) {
                String key = in.readString();
                entries.put(key, decode(map.values(), in, depth + 1));
            }
        }
        return entries;
    }

    /**
     * An array/map block count. A negative count means {@code abs(count)} items follow a skippable byte-size hint; the
     * hint is read and discarded and the absolute count is used. Callers bound the count before iterating: by the
     * remaining input when items occupy at least one byte, by {@link #MAX_ZERO_BYTE_ITEMS_PER_BLOCK} otherwise.
     * Package-private: {@link ResolvingDatumDecoder} runs the same block loops with the same guards.
     */
    static long blockCount(AvroBinaryDecoder in) {
        long count = in.readLong();
        if (count < 0) {
            in.readLong();
            if (count == Long.MIN_VALUE) {
                // negating Long.MIN_VALUE yields itself; the still-negative count would slip past every
                // bound and misparse the block's data bytes as the next block count
                throw new AvroFormatException("Block count " + count + " is not representable");
            }
            count = -count;
        }
        return count;
    }

    static void requirePlausibleItemCount(long count, boolean zeroByteItems, AvroBinaryDecoder in) {
        if (!zeroByteItems) {
            in.requireBlockCountWithinInput(count);
        } else if (count > MAX_ZERO_BYTE_ITEMS_PER_BLOCK) {
            throw new AvroFormatException("Block count " + count + " of zero-byte items exceeds the maximum "
                    + MAX_ZERO_BYTE_ITEMS_PER_BLOCK + " per block");
        }
    }

    private String symbol(AvroSchema.Enum anEnum, int index) {
        List<String> symbols = anEnum.symbols();
        if (index < 0 || index >= symbols.size()) {
            throw new AvroFormatException("Enum index out of range: " + index);
        }
        return symbols.get(index);
    }
}
