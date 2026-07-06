/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
 * Decodes one writer-encoded Avro datum into a reader-shaped value by walking a {@link Resolved} plan from
 * {@link SchemaResolver}. Mirrors {@link AvroDatumDecoder}: the same depth guard bounds structural nesting and the same
 * block-count guards bound array/map allocation. Thread-confined; reuse within a single block decode is fine.
 */
final class ResolvingDatumDecoder {

    private final DatumSkipper skipper = new DatumSkipper();

    Object decode(Resolved plan, AvroBinaryDecoder in) {
        return decode(plan, in, 0);
    }

    private Object decode(Resolved plan, AvroBinaryDecoder in, int depth) {
        if (depth > AvroDatumDecoder.MAX_DEPTH) {
            throw new AvroFormatException("Datum nesting exceeds maximum depth " + AvroDatumDecoder.MAX_DEPTH);
        }
        return switch (plan) {
            case Resolved.Direct(AvroSchema writer, AvroSchema reader) -> decodeDirect(writer, reader, in);
            case Resolved.Promote(AvroSchema writer, AvroSchema reader) -> decodePromoted(writer, reader, in);
            case Resolved.RecordPlan recordPlan -> decodeRecord(recordPlan, in, depth);
            case Resolved.ArrayPlan arrayPlan -> decodeArray(arrayPlan, in, depth);
            case Resolved.MapPlan mapPlan -> decodeMap(mapPlan, in, depth);
            case Resolved.WriterUnionPlan unionPlan -> decodeWriterUnion(unionPlan, in, depth);
            case Resolved.EnumPlan enumPlan -> decodeEnum(enumPlan, in);
            case Resolved.Failure(String message) -> throw new AvroFormatException(message);
            case Resolved.PlanRef ref -> decode(ref.target(), in, depth + 1);
        };
    }

    /** The raw bytes follow the writer schema; the logical-type interpretation belongs to the reader schema. */
    private Object decodeDirect(AvroSchema writer, AvroSchema reader, AvroBinaryDecoder in) {
        Object raw =
                switch (writer.type()) {
                    case NULL -> null;
                    case BOOLEAN -> in.readBoolean();
                    case INT -> in.readInt();
                    case LONG -> in.readLong();
                    case FLOAT -> in.readFloat();
                    case DOUBLE -> in.readDouble();
                    case STRING -> in.readString();
                    case BYTES -> in.readBytes();
                    case FIXED -> in.fixed(((AvroSchema.Fixed) writer).size());
                    case RECORD, ARRAY, MAP, UNION, ENUM ->
                        throw new IllegalStateException("Composite type planned as Direct: " + writer.type());
                };
        return convertIfLogical(reader, raw);
    }

    /**
     * String and bytes share one wire form (length prefix plus UTF-8 bytes); the promotion only changes the Java box,
     * by reading through the other type's accessor.
     */
    private Object decodePromoted(AvroSchema writer, AvroSchema reader, AvroBinaryDecoder in) {
        Object promoted =
                switch (writer.type()) {
                    case INT -> widenInt(in.readInt(), reader.type());
                    case LONG -> widenLong(in.readLong(), reader.type());
                    case FLOAT -> (double) in.readFloat();
                    case STRING -> in.readBytes();
                    case BYTES -> in.readString();
                    default -> throw new IllegalStateException("No promotion from writer type " + writer.type());
                };
        return convertIfLogical(reader, promoted);
    }

    private static Object widenInt(int value, AvroSchema.Type readerType) {
        return switch (readerType) {
            case LONG -> (long) value;
            case FLOAT -> (float) value;
            case DOUBLE -> (double) value;
            default -> throw new IllegalStateException("No promotion from int to " + readerType);
        };
    }

    private static Object widenLong(long value, AvroSchema.Type readerType) {
        return switch (readerType) {
            case FLOAT -> (float) value;
            case DOUBLE -> (double) value;
            default -> throw new IllegalStateException("No promotion from long to " + readerType);
        };
    }

    private AvroRecord decodeRecord(Resolved.RecordPlan plan, AvroBinaryDecoder in, int depth) {
        Object[] values = new Object[plan.reader().fields().size()];
        for (Resolved.FieldStep step : plan.steps()) {
            if (step.readerPosition() < 0) {
                skipper.skip(step.writerSchema(), in);
            } else {
                values[step.readerPosition()] = decode(step.value(), in, depth + 1);
            }
        }
        for (Resolved.Defaulted defaulted : plan.defaulted()) {
            values[defaulted.readerPosition()] = defaulted.value();
        }
        return new AvroRecord(plan.reader(), values);
    }

    private List<Object> decodeArray(Resolved.ArrayPlan plan, AvroBinaryDecoder in, int depth) {
        List<Object> items = new ArrayList<>();
        for (long count = AvroDatumDecoder.blockCount(in); count != 0; count = AvroDatumDecoder.blockCount(in)) {
            AvroDatumDecoder.requirePlausibleItemCount(count, plan.zeroByteElements(), in);
            for (long i = 0; i < count; i++) {
                items.add(decode(plan.element(), in, depth + 1));
            }
        }
        return items;
    }

    private Map<String, Object> decodeMap(Resolved.MapPlan plan, AvroBinaryDecoder in, int depth) {
        Map<String, Object> entries = new LinkedHashMap<>();
        for (long count = AvroDatumDecoder.blockCount(in); count != 0; count = AvroDatumDecoder.blockCount(in)) {
            // map keys cost at least the length varint; the remaining input always bounds the count
            in.requireBlockCountWithinInput(count);
            for (long i = 0; i < count; i++) {
                String key = in.readString();
                entries.put(key, decode(plan.values(), in, depth + 1));
            }
        }
        return entries;
    }

    private Object decodeWriterUnion(Resolved.WriterUnionPlan plan, AvroBinaryDecoder in, int depth) {
        int index = in.readInt();
        List<Resolved> branches = plan.branches();
        if (index < 0 || index >= branches.size()) {
            throw new AvroFormatException("Union branch index out of range: " + index);
        }
        return decode(branches.get(index), in, depth + 1);
    }

    private String decodeEnum(Resolved.EnumPlan plan, AvroBinaryDecoder in) {
        int writerIndex = in.readInt();
        int[] mapping = plan.symbolMapping();
        if (writerIndex < 0 || writerIndex >= mapping.length) {
            throw new AvroFormatException("Enum index out of range: " + writerIndex);
        }
        int readerIndex = mapping[writerIndex];
        if (readerIndex >= 0) {
            return plan.reader().symbols().get(readerIndex);
        }
        String fallback = plan.reader().defaultSymbol();
        if (fallback == null) {
            throw new AvroFormatException("Writer enum symbol at index " + writerIndex
                    + " has no mapping in reader enum " + plan.reader().fullName() + " and no default");
        }
        return fallback;
    }

    private Object convertIfLogical(AvroSchema reader, Object raw) {
        return reader.logicalType()
                .map(logical -> LogicalValues.convert(logical, raw))
                .orElse(raw);
    }
}
