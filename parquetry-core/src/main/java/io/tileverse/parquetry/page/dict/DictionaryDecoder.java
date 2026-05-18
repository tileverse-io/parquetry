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
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

import io.tileverse.parquetry.page.plain.PlainBinaryDecoder;
import io.tileverse.parquetry.page.plain.PlainBooleanDecoder;
import io.tileverse.parquetry.page.plain.PlainDoubleDecoder;
import io.tileverse.parquetry.page.plain.PlainFixedLenBinaryDecoder;
import io.tileverse.parquetry.page.plain.PlainFloatDecoder;
import io.tileverse.parquetry.page.plain.PlainInt32Decoder;
import io.tileverse.parquetry.page.plain.PlainInt64Decoder;
import io.tileverse.parquetry.page.plain.PlainInt96Decoder;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Reads a dictionary page into a {@link Dictionary}.
 *
 * <p>Dictionary pages carry all unique values for a column chunk in PLAIN encoding. This class dispatches to the
 * appropriate PLAIN decoder based on the column's primitive kind, reads {@code valueCount} values, and wraps them in
 * the matching {@link Dictionary} variant.
 */
public final class DictionaryDecoder {

    private DictionaryDecoder() {}

    /**
     * Decode a dictionary page into a {@link Dictionary}.
     *
     * @param page the dictionary page bytes (decompressed)
     * @param kind primitive kind for the column
     * @param valueCount number of unique values in the dictionary (from the page header)
     * @param typeLength only used for {@link PrimitiveKind#FIXED_LEN_BYTE_ARRAY}
     */
    // The wildcard return is intentional: the concrete element type is chosen at runtime by PrimitiveKind.
    @SuppressWarnings("java:S1452")
    public static Dictionary<?> read(ByteBuffer page, PrimitiveKind kind, int valueCount, OptionalInt typeLength) {
        return switch (kind) {
            case BOOLEAN -> readBooleans(page, valueCount);
            case INT32 -> readInts(page, valueCount);
            case INT64 -> readLongs(page, valueCount);
            case INT96 -> readInt96s(page, valueCount);
            case FLOAT -> readFloats(page, valueCount);
            case DOUBLE -> readDoubles(page, valueCount);
            case BYTE_ARRAY -> readBinary(page, valueCount);
            case FIXED_LEN_BYTE_ARRAY ->
                readFixedLenBinary(
                        page,
                        valueCount,
                        typeLength.orElseThrow(() -> new IllegalArgumentException(
                                "typeLength required for FIXED_LEN_BYTE_ARRAY dictionary")));
        };
    }

    private static Dictionary.BooleanDict readBooleans(ByteBuffer page, int n) {
        PlainBooleanDecoder decoder = new PlainBooleanDecoder();
        decoder.load(page, n);
        List<Boolean> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            values.add(decoder.next());
        }
        return new Dictionary.BooleanDict(values);
    }

    private static Dictionary.IntDict readInts(ByteBuffer page, int n) {
        PlainInt32Decoder decoder = new PlainInt32Decoder();
        decoder.load(page, n);
        List<Integer> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            values.add(decoder.next());
        }
        return new Dictionary.IntDict(values);
    }

    private static Dictionary.LongDict readLongs(ByteBuffer page, int n) {
        PlainInt64Decoder decoder = new PlainInt64Decoder();
        decoder.load(page, n);
        List<Long> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            values.add(decoder.next());
        }
        return new Dictionary.LongDict(values);
    }

    private static Dictionary.FloatDict readFloats(ByteBuffer page, int n) {
        PlainFloatDecoder decoder = new PlainFloatDecoder();
        decoder.load(page, n);
        List<Float> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            values.add(decoder.next());
        }
        return new Dictionary.FloatDict(values);
    }

    private static Dictionary.DoubleDict readDoubles(ByteBuffer page, int n) {
        PlainDoubleDecoder decoder = new PlainDoubleDecoder();
        decoder.load(page, n);
        List<Double> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            values.add(decoder.next());
        }
        return new Dictionary.DoubleDict(values);
    }

    private static Dictionary.Int96Dict readInt96s(ByteBuffer page, int n) {
        PlainInt96Decoder decoder = new PlainInt96Decoder();
        decoder.load(page, n);
        List<ByteBuffer> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            values.add(decoder.next());
        }
        return new Dictionary.Int96Dict(values);
    }

    private static Dictionary.BinaryDict readBinary(ByteBuffer page, int n) {
        PlainBinaryDecoder decoder = new PlainBinaryDecoder();
        decoder.load(page, n);
        List<ByteBuffer> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            values.add(decoder.next());
        }
        return new Dictionary.BinaryDict(values);
    }

    private static Dictionary.FixedLenBinaryDict readFixedLenBinary(ByteBuffer page, int n, int length) {
        PlainFixedLenBinaryDecoder decoder = new PlainFixedLenBinaryDecoder(length);
        decoder.load(page, n);
        List<ByteBuffer> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            values.add(decoder.next());
        }
        return new Dictionary.FixedLenBinaryDict(values, length);
    }
}
