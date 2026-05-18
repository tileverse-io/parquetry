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
import java.util.List;

import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Dictionary page payload: list of unique values for a column chunk.
 *
 * <p>One variant per Parquet primitive kind. The {@code values()} list is immutable and indexed in the order values
 * were written; data pages reference values by their index.
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

    record BooleanDict(List<Boolean> values) implements Dictionary<Boolean> {
        public BooleanDict {
            values = List.copyOf(values);
        }

        @Override
        public PrimitiveKind kind() {
            return PrimitiveKind.BOOLEAN;
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public Boolean get(int index) {
            return values.get(index);
        }
    }

    record IntDict(List<Integer> values) implements Dictionary<Integer> {
        public IntDict {
            values = List.copyOf(values);
        }

        @Override
        public PrimitiveKind kind() {
            return PrimitiveKind.INT32;
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public Integer get(int index) {
            return values.get(index);
        }
    }

    record LongDict(List<Long> values) implements Dictionary<Long> {
        public LongDict {
            values = List.copyOf(values);
        }

        @Override
        public PrimitiveKind kind() {
            return PrimitiveKind.INT64;
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public Long get(int index) {
            return values.get(index);
        }
    }

    record FloatDict(List<Float> values) implements Dictionary<Float> {
        public FloatDict {
            values = List.copyOf(values);
        }

        @Override
        public PrimitiveKind kind() {
            return PrimitiveKind.FLOAT;
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public Float get(int index) {
            return values.get(index);
        }
    }

    record DoubleDict(List<Double> values) implements Dictionary<Double> {
        public DoubleDict {
            values = List.copyOf(values);
        }

        @Override
        public PrimitiveKind kind() {
            return PrimitiveKind.DOUBLE;
        }

        @Override
        public int size() {
            return values.size();
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
