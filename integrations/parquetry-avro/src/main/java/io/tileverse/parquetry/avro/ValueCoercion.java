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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;

/**
 * Coerces a caller-supplied Java value to the primitive shape an Avro schema demands. Coercion widens numeric types and
 * accepts the common byte and text representations, but never reinterprets across kinds: a String is not a number and a
 * number is not text. Out-of-kind or out-of-range input raises {@link AvroFormatException}.
 */
final class ValueCoercion {

    private ValueCoercion() {}

    static boolean asBoolean(Object value) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        throw notA("boolean", value);
    }

    static int asInt(Object value) {
        long widened = asLong(value);
        if (widened < Integer.MIN_VALUE || widened > Integer.MAX_VALUE) {
            throw new AvroFormatException("Value " + widened + " is outside int range");
        }
        return (int) widened;
    }

    static long asLong(Object value) {
        if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            return ((Number) value).longValue();
        }
        throw notA("integer", value);
    }

    static float asFloat(Object value) {
        if (value instanceof Number number) {
            return number.floatValue();
        }
        throw notA("float", value);
    }

    static double asDouble(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        throw notA("double", value);
    }

    static String asString(Object value) {
        if (value instanceof CharSequence text) {
            return text.toString();
        }
        throw notA("string", value);
    }

    static byte[] asBytes(Object value) {
        return switch (value) {
            case byte[] bytes -> bytes;
            case MemorySegment segment -> segment.toArray(ValueLayout.JAVA_BYTE);
            case ByteBuffer buffer -> remaining(buffer);
            default -> throw notA("bytes", value);
        };
    }

    private static byte[] remaining(ByteBuffer buffer) {
        ByteBuffer view = buffer.duplicate();
        byte[] bytes = new byte[view.remaining()];
        view.get(bytes);
        return bytes;
    }

    private static AvroFormatException notA(String expected, Object value) {
        String actual = value == null ? "null" : value.getClass().getName();
        return new AvroFormatException("Cannot encode " + actual + " as Avro " + expected);
    }
}
