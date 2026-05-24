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
package io.tileverse.parquetry.data.read.page;

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.FLOAT;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.OptionalInt;

import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Reads a dictionary page into a {@link Dictionary}.
 *
 * <p>Dictionary pages carry all unique values for a column chunk in PLAIN encoding. For fixed-width primitives the
 * decoder bulk-copies the page bytes into an owned primitive buffer (no boxing, single allocation); for variable-length
 * binary it copies each value into an owned heap {@link MemorySegment}. Either way, the resulting {@link Dictionary}
 * outlives the source page bytes, so its values must own their storage to survive the dictionary-page scratch buffer
 * being returned to its pool.
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
    public static Dictionary<?> read(MemorySegment page, PrimitiveKind kind, int valueCount, OptionalInt typeLength) {
        return switch (kind) {
            case BOOLEAN -> readBooleans(page, valueCount);
            case INT32 -> readInts(page, valueCount);
            case INT64 -> readLongs(page, valueCount);
            case INT96 -> readInt96s(page, valueCount);
            case FLOAT -> readFloats(page, valueCount);
            case DOUBLE -> readDoubles(page, valueCount);
            case BYTE_ARRAY -> readBinary(page, valueCount);
            case FIXED_LEN_BYTE_ARRAY -> {
                int valueLength = typeLength.orElseThrow(
                        () -> new MalformedFileException("typeLength required for FIXED_LEN_BYTE_ARRAY dictionary"));
                yield readFixedLenBinary(page, valueCount, valueLength);
            }
        };
    }

    private static Dictionary.BooleanDict readBooleans(MemorySegment page, int n) {
        int requiredBytes = (n + 7) >>> 3;
        if (page.byteSize() < requiredBytes) {
            throw new MalformedFileException("BOOLEAN dictionary page has " + page.byteSize() + " byte(s) but "
                    + requiredBytes + " are required for " + n + " value(s)");
        }
        return new Dictionary.BooleanDict(BitSet.valueOf(page.toArray(JAVA_BYTE)), n);
    }

    private static Dictionary.IntDict readInts(MemorySegment page, int n) {
        int[] values = new int[n];
        MemorySegment.copy(page, INT32, 0L, values, 0, n);
        return new Dictionary.IntDict(IntBuffer.wrap(values).asReadOnlyBuffer());
    }

    private static Dictionary.LongDict readLongs(MemorySegment page, int n) {
        long[] values = new long[n];
        MemorySegment.copy(page, INT64, 0L, values, 0, n);
        return new Dictionary.LongDict(LongBuffer.wrap(values).asReadOnlyBuffer());
    }

    private static Dictionary.FloatDict readFloats(MemorySegment page, int n) {
        float[] values = new float[n];
        MemorySegment.copy(page, FLOAT, 0L, values, 0, n);
        return new Dictionary.FloatDict(FloatBuffer.wrap(values).asReadOnlyBuffer());
    }

    private static Dictionary.DoubleDict readDoubles(MemorySegment page, int n) {
        double[] values = new double[n];
        MemorySegment.copy(page, DOUBLE, 0L, values, 0, n);
        return new Dictionary.DoubleDict(DoubleBuffer.wrap(values).asReadOnlyBuffer());
    }

    private static Dictionary.Int96Dict readInt96s(MemorySegment page, int n) {
        PlainInt96Decoder decoder = new PlainInt96Decoder();
        decoder.load(page, n);
        List<MemorySegment> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            values.add(copyToOwnedSegment(decoder.next()));
        }
        return new Dictionary.Int96Dict(values);
    }

    private static Dictionary.BinaryDict readBinary(MemorySegment page, int n) {
        PlainBinaryDecoder decoder = new PlainBinaryDecoder();
        decoder.load(page, n);
        List<MemorySegment> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            values.add(copyToOwnedSegment(decoder.next()));
        }
        return new Dictionary.BinaryDict(values);
    }

    private static Dictionary.FixedLenBinaryDict readFixedLenBinary(MemorySegment page, int n, int length) {
        PlainFixedLenBinaryDecoder decoder = new PlainFixedLenBinaryDecoder(length);
        decoder.load(page, n);
        List<MemorySegment> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            values.add(copyToOwnedSegment(decoder.next()));
        }
        return new Dictionary.FixedLenBinaryDict(values, length);
    }

    /**
     * Copies the bytes of {@code source} into a freshly-allocated heap {@link MemorySegment} and returns it read-only.
     * The {@code PlainBinaryDecoder} family returns zero-copy views of the source page; dictionaries outlive that page
     * (the dictionary-page scratch buffer is returned to the pool right after decode), so dictionary values must own
     * their bytes to avoid dangling references into recycled pool memory.
     */
    private static MemorySegment copyToOwnedSegment(MemorySegment source) {
        byte[] copy = source.toArray(JAVA_BYTE);
        return MemorySegment.ofArray(copy).asReadOnly();
    }
}
