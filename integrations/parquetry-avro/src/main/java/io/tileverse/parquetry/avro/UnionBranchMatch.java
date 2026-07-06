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

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

/**
 * Decides whether a union branch accepts a runtime value. Matching is by Java type and is deliberately conservative for
 * numbers so a value lands in the narrowest declared branch; the encoder consults branches in declared order, which
 * resolves any remaining ambiguity (for example a record branch and a map branch that both accept a {@code Map}).
 */
final class UnionBranchMatch {

    private UnionBranchMatch() {}

    static boolean accepts(AvroSchema schema, Object value) {
        return switch (schema) {
            case AvroSchema.Ref ref -> accepts(ref.target(), value);
            case AvroSchema.Union ignored -> false;
            case AvroSchema.Primitive primitive -> acceptsPrimitive(primitive, value);
            case AvroSchema.Record ignored -> value instanceof AvroRecord || value instanceof Map<?, ?>;
            case AvroSchema.Array ignored -> value instanceof Collection<?> || isArray(value);
            case AvroSchema.Map ignored -> value instanceof Map<?, ?>;
            case AvroSchema.Enum ignored -> value instanceof CharSequence;
            case AvroSchema.Fixed fixed -> acceptsFixed(fixed, value);
        };
    }

    private static boolean acceptsPrimitive(AvroSchema.Primitive primitive, Object value) {
        if (primitive.logicalType().isPresent() && acceptsLogical(value)) {
            return true;
        }
        return switch (primitive.type()) {
            case NULL -> value == null;
            case BOOLEAN -> value instanceof Boolean;
            case INT -> value instanceof Integer || value instanceof Short || value instanceof Byte;
            case LONG ->
                value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte;
            case FLOAT -> value instanceof Float;
            case DOUBLE -> value instanceof Double || value instanceof Float;
            case STRING -> value instanceof CharSequence;
            case BYTES -> value instanceof byte[] || value instanceof MemorySegment || value instanceof ByteBuffer;
            default -> false;
        };
    }

    private static boolean acceptsFixed(AvroSchema.Fixed fixed, Object value) {
        if (fixed.logicalType().isPresent() && acceptsLogical(value)) {
            return true;
        }
        return value instanceof byte[] || value instanceof MemorySegment;
    }

    private static boolean acceptsLogical(Object value) {
        return value instanceof BigDecimal
                || value instanceof UUID
                || value instanceof LocalDate
                || value instanceof LocalTime
                || value instanceof Instant
                || value instanceof LocalDateTime
                || value instanceof AvroDuration;
    }

    private static boolean isArray(Object value) {
        return value != null && value.getClass().isArray();
    }
}
