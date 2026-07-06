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

import java.util.List;

/**
 * Skips one datum of a given {@link AvroSchema} without materializing values; used by schema resolution to advance past
 * writer fields the reader drops. Blocked arrays and maps that come with a byte-size hint are skipped in one jump. The
 * same depth guard as {@link AvroDatumDecoder} bounds structural nesting.
 */
final class DatumSkipper {

    private static final int MAX_DEPTH = AvroDatumDecoder.MAX_DEPTH;

    private static final int MINIMUM_SIZE_DEPTH_CAP = 100;

    /**
     * The minimum bytes one datum of {@code schema} occupies on the wire. Zero means every datum of the schema is
     * invisible in the input (null, fixed(0), records of those); block-count guards switch strategy on that. A direct
     * record cycle with no union escape cannot encode any finite datum; past a small depth cap the answer is 1, which
     * keeps the remaining-input guard active.
     */
    static int minimumWireSize(AvroSchema schema) {
        return minimumWireSize(schema, 0);
    }

    private static int minimumWireSize(AvroSchema schema, int depth) {
        if (depth > MINIMUM_SIZE_DEPTH_CAP) {
            return 1;
        }
        return switch (schema) {
            case AvroSchema.Primitive primitive -> minimumPrimitiveWireSize(primitive);
            case AvroSchema.Record recordSchema -> {
                long sum = 0;
                for (AvroSchema.Field field : recordSchema.fields()) {
                    sum += minimumWireSize(field.schema(), depth + 1);
                }
                yield (int) Math.min(sum, Integer.MAX_VALUE);
            }
            // the branch index varint; the cheapest branch may be null and add nothing
            case AvroSchema.Union _ -> 1;
            // the terminating zero block count
            case AvroSchema.Array _ -> 1;
            case AvroSchema.Map _ -> 1;
            case AvroSchema.Fixed fixed -> fixed.size();
            case AvroSchema.Enum _ -> 1;
            case AvroSchema.Ref ref -> minimumWireSize(ref.target(), depth + 1);
        };
    }

    private static int minimumPrimitiveWireSize(AvroSchema.Primitive primitive) {
        return switch (primitive.type()) {
            case NULL -> 0;
            case BOOLEAN -> 1;
            case INT, LONG -> 1;
            case FLOAT -> 4;
            case DOUBLE -> 8;
            case STRING, BYTES -> 1;
            default -> throw new AvroFormatException("Not a primitive: " + primitive.type());
        };
    }

    void skip(AvroSchema schema, AvroBinaryDecoder in) {
        skip(schema, in, 0);
    }

    private void skip(AvroSchema schema, AvroBinaryDecoder in, int depth) {
        if (depth > MAX_DEPTH) {
            throw new AvroFormatException("Datum nesting exceeds maximum depth " + MAX_DEPTH);
        }
        switch (schema) {
            case AvroSchema.Primitive primitive -> skipPrimitive(primitive, in);
            case AvroSchema.Record recordSchema -> {
                for (AvroSchema.Field field : recordSchema.fields()) {
                    skip(field.schema(), in, depth + 1);
                }
            }
            case AvroSchema.Union(List<AvroSchema> branches) -> {
                int index = in.readInt();
                if (index < 0 || index >= branches.size()) {
                    throw new AvroFormatException("Union branch index out of range: " + index);
                }
                skip(branches.get(index), in, depth + 1);
            }
            case AvroSchema.Array(AvroSchema element) -> skipBlocks(element, in, depth);
            case AvroSchema.Map(AvroSchema values) -> skipMapBlocks(values, in, depth);
            case AvroSchema.Fixed fixed -> in.skip(fixed.size());
            case AvroSchema.Enum _ -> in.readInt();
            case AvroSchema.Ref ref -> skip(ref.target(), in, depth + 1);
        }
    }

    private void skipPrimitive(AvroSchema.Primitive primitive, AvroBinaryDecoder in) {
        switch (primitive.type()) {
            case NULL -> {
                /* zero bytes on the wire */
            }
            case BOOLEAN -> in.skip(1);
            case INT, LONG -> in.readLong();
            case FLOAT -> in.skip(4);
            case DOUBLE -> in.skip(8);
            case STRING, BYTES -> in.skip(nonNegativeLength(in));
            default -> throw new AvroFormatException("Not a primitive: " + primitive.type());
        }
    }

    /** Negative block counts come with a byte-size hint, allowing the whole block to be skipped in one jump. */
    private void skipBlocks(AvroSchema element, AvroBinaryDecoder in, int depth) {
        // a zero-minimum element is invisible in the input: any count of them is already skipped
        boolean zeroByteItems = minimumWireSize(element) == 0;
        for (long count = in.readLong(); count != 0; count = in.readLong()) {
            if (count < 0) {
                in.skip(nonNegativeLength(in));
            } else if (!zeroByteItems) {
                in.requireBlockCountWithinInput(count);
                for (long i = 0; i < count; i++) {
                    skip(element, in, depth + 1);
                }
            }
        }
    }

    private void skipMapBlocks(AvroSchema values, AvroBinaryDecoder in, int depth) {
        for (long count = in.readLong(); count != 0; count = in.readLong()) {
            if (count < 0) {
                in.skip(nonNegativeLength(in));
            } else {
                // map keys cost at least the length varint; the remaining input always bounds the count
                in.requireBlockCountWithinInput(count);
                for (long i = 0; i < count; i++) {
                    in.skip(nonNegativeLength(in)); // key string
                    skip(values, in, depth + 1);
                }
            }
        }
    }

    private long nonNegativeLength(AvroBinaryDecoder in) {
        long length = in.readLong();
        if (length < 0) {
            throw new AvroFormatException("Negative length: " + length);
        }
        return length;
    }
}
