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
package io.tileverse.parquetry.internal.write;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.tileverse.parquetry.internal.write.page.Encoder;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Per-kind growable scratch buffer for the non-null values of a single data page. The carrier array returned by
 * {@link #payloadValues(int)} matches the column's {@link PrimitiveKind} so the per-page {@link Encoder} can consume it
 * directly. INT96 reuses the binary buffer: its values are the 12-byte little-endian packings produced by the writer.
 */
abstract class ValueBuffer {

    static ValueBuffer forKind(PrimitiveKind kind) {
        return switch (kind) {
            case BOOLEAN -> new BooleanBuffer();
            case INT32 -> new IntBuffer();
            case INT64 -> new LongBuffer();
            case FLOAT -> new FloatBuffer();
            case DOUBLE -> new DoubleBuffer();
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> new BinaryBuffer();
        };
    }

    /** Returns the buffered values as the encoder's carrier array, trimmed to {@code nonNullCount}. */
    abstract Object payloadValues(int nonNullCount);

    /** Drops all buffered values, keeping capacity for the next page. */
    abstract void clear();

    void addInt(int value) {
        throw new UnsupportedOperationException();
    }

    void addLong(long value) {
        throw new UnsupportedOperationException();
    }

    void addFloat(float value) {
        throw new UnsupportedOperationException();
    }

    void addDouble(double value) {
        throw new UnsupportedOperationException();
    }

    void addBoolean(boolean value) {
        throw new UnsupportedOperationException();
    }

    void addBinary(byte[] value) {
        throw new UnsupportedOperationException();
    }

    private static final class IntBuffer extends ValueBuffer {

        private int[] values = new int[1024];
        private int size;

        @Override
        void addInt(int value) {
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = value;
        }

        @Override
        Object payloadValues(int nonNullCount) {
            return Arrays.copyOf(values, nonNullCount);
        }

        @Override
        void clear() {
            size = 0;
        }
    }

    private static final class LongBuffer extends ValueBuffer {

        private long[] values = new long[1024];
        private int size;

        @Override
        void addLong(long value) {
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = value;
        }

        @Override
        Object payloadValues(int nonNullCount) {
            return Arrays.copyOf(values, nonNullCount);
        }

        @Override
        void clear() {
            size = 0;
        }
    }

    private static final class FloatBuffer extends ValueBuffer {

        private float[] values = new float[1024];
        private int size;

        @Override
        void addFloat(float value) {
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = value;
        }

        @Override
        Object payloadValues(int nonNullCount) {
            return Arrays.copyOf(values, nonNullCount);
        }

        @Override
        void clear() {
            size = 0;
        }
    }

    private static final class DoubleBuffer extends ValueBuffer {

        private double[] values = new double[1024];
        private int size;

        @Override
        void addDouble(double value) {
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = value;
        }

        @Override
        Object payloadValues(int nonNullCount) {
            return Arrays.copyOf(values, nonNullCount);
        }

        @Override
        void clear() {
            size = 0;
        }
    }

    private static final class BooleanBuffer extends ValueBuffer {

        private boolean[] values = new boolean[1024];
        private int size;

        @Override
        void addBoolean(boolean value) {
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            values[size++] = value;
        }

        @Override
        Object payloadValues(int nonNullCount) {
            return Arrays.copyOf(values, nonNullCount);
        }

        @Override
        void clear() {
            size = 0;
        }
    }

    /** Backs BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, and INT96; the latter stores its packed 12-byte representation. */
    private static final class BinaryBuffer extends ValueBuffer {

        private final List<byte[]> values = new ArrayList<>(1024);

        @Override
        void addBinary(byte[] value) {
            values.add(value);
        }

        @Override
        Object payloadValues(int nonNullCount) {
            byte[][] carrier = new byte[nonNullCount][];
            for (int i = 0; i < nonNullCount; i++) {
                carrier[i] = values.get(i);
            }
            return carrier;
        }

        @Override
        void clear() {
            values.clear();
        }
    }
}
