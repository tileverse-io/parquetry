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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;

import io.tileverse.parquetry.internal.write.page.Encoder;
import io.tileverse.parquetry.internal.write.page.PackedBinaryPayload;
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

    void addBinary(MemorySegment src, long off, long len) {
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

    /**
     * Backs BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, and INT96; the latter stores its packed 12-byte representation. Every
     * value is copied into one contiguous {@code backing} array with a parallel (offset, length) index, minting no
     * per-value array.
     */
    private static final class BinaryBuffer extends ValueBuffer {

        private byte[] backing = new byte[4096];
        private int[] offsets = new int[1024];
        private int[] lengths = new int[1024];
        private int size;
        private int byteEnd;

        @Override
        void addBinary(MemorySegment src, long off, long len) {
            int length = Math.toIntExact(len);
            int start = reserveBytes(length);
            MemorySegment.copy(src, ValueLayout.JAVA_BYTE, off, backing, start, length);
            recordValue(start, length);
        }

        @Override
        void addBinary(byte[] value) {
            int start = reserveBytes(value.length);
            System.arraycopy(value, 0, backing, start, value.length);
            recordValue(start, value.length);
        }

        private int reserveBytes(int length) {
            long needed = (long) byteEnd + length;
            if (needed > backing.length) {
                long grown = Math.max((long) backing.length * 2, needed);
                backing = Arrays.copyOf(backing, Math.toIntExact(grown));
            }
            return byteEnd;
        }

        private void recordValue(int start, int length) {
            if (size == offsets.length) {
                offsets = Arrays.copyOf(offsets, offsets.length * 2);
                lengths = Arrays.copyOf(lengths, lengths.length * 2);
            }
            offsets[size] = start;
            lengths[size] = length;
            size++;
            byteEnd = start + length;
        }

        // The payload aliases the live backing and index arrays rather than copying them. This is sound because the
        // caller encodes the page synchronously before clear() reuses the buffer for the next page.
        @Override
        Object payloadValues(int nonNullCount) {
            return new PackedBinaryPayload(backing, offsets, lengths, nonNullCount);
        }

        @Override
        void clear() {
            size = 0;
            byteEnd = 0;
        }
    }
}
