/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.page.dict;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.BitSet;
import java.util.List;

import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Dictionary page payload: the unique values referenced by every data page in a column chunk.
 *
 * <p>One variant per Parquet primitive kind. Fixed-width primitive variants hold a read-only primitive buffer
 * ({@link IntBuffer}, {@link LongBuffer}, etc.) so dictionary lookups are unboxed and the dictionary owns a single
 * contiguous heap allocation. The boolean variant uses a {@link BitSet}. Binary variants (BYTE_ARRAY,
 * FIXED_LEN_BYTE_ARRAY, INT96) hold a {@code List} of owned heap {@link ByteBuffer}s.
 */
public sealed interface Dictionary<T>
        permits Dictionary.BooleanDict,
                Dictionary.IntDict,
                Dictionary.LongDict,
                Dictionary.FloatDict,
                Dictionary.DoubleDict,
                Dictionary.BinaryDict,
                Dictionary.FixedLenBinaryDict,
                Dictionary.Int96Dict {

    PrimitiveKind kind();

    int size();

    /** Get the value at {@code index}. */
    T get(int index);

    record BooleanDict(BitSet values, int size) implements Dictionary<Boolean> {
        @Override
        public PrimitiveKind kind() {
            return PrimitiveKind.BOOLEAN;
        }

        @Override
        public Boolean get(int index) {
            return values.get(index);
        }
    }

    record IntDict(IntBuffer values) implements Dictionary<Integer> {

        @Override
        public PrimitiveKind kind() {
            return PrimitiveKind.INT32;
        }

        @Override
        public int size() {
            return values.capacity();
        }

        @Override
        public Integer get(int index) {
            return values.get(index);
        }
    }

    record LongDict(LongBuffer values) implements Dictionary<Long> {

        @Override
        public PrimitiveKind kind() {
            return PrimitiveKind.INT64;
        }

        @Override
        public int size() {
            return values.capacity();
        }

        @Override
        public Long get(int index) {
            return values.get(index);
        }
    }

    record FloatDict(FloatBuffer values) implements Dictionary<Float> {

        @Override
        public PrimitiveKind kind() {
            return PrimitiveKind.FLOAT;
        }

        @Override
        public int size() {
            return values.capacity();
        }

        @Override
        public Float get(int index) {
            return values.get(index);
        }
    }

    record DoubleDict(DoubleBuffer values) implements Dictionary<Double> {

        @Override
        public PrimitiveKind kind() {
            return PrimitiveKind.DOUBLE;
        }

        @Override
        public int size() {
            return values.capacity();
        }

        @Override
        public Double get(int index) {
            return values.get(index);
        }
    }

    record BinaryDict(List<ByteBuffer> values) implements Dictionary<ByteBuffer> {
        public BinaryDict {
            values = values.stream().map(ByteBuffer::asReadOnlyBuffer).toList();
        }

        @Override
        public PrimitiveKind kind() {
            return PrimitiveKind.BYTE_ARRAY;
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public ByteBuffer get(int index) {
            return values.get(index);
        }
    }

    record FixedLenBinaryDict(List<ByteBuffer> values, int typeLength) implements Dictionary<ByteBuffer> {
        public FixedLenBinaryDict {
            values = values.stream().map(ByteBuffer::asReadOnlyBuffer).toList();
        }

        @Override
        public PrimitiveKind kind() {
            return PrimitiveKind.FIXED_LEN_BYTE_ARRAY;
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public ByteBuffer get(int index) {
            return values.get(index);
        }
    }

    record Int96Dict(List<ByteBuffer> values) implements Dictionary<ByteBuffer> {
        public Int96Dict {
            values = values.stream().map(ByteBuffer::asReadOnlyBuffer).toList();
        }

        @Override
        public PrimitiveKind kind() {
            return PrimitiveKind.INT96;
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public ByteBuffer get(int index) {
            return values.get(index);
        }
    }
}
