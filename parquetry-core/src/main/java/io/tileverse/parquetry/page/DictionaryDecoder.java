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
package io.tileverse.parquetry.page;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.OptionalInt;

import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Reads a dictionary page into a {@link Dictionary}.
 *
 * <p>Dictionary pages carry all unique values for a column chunk in PLAIN encoding. For fixed-width primitives the
 * decoder bulk-copies the page bytes into an owned primitive buffer (no boxing, single allocation); for variable-length
 * binary it copies each value into an owned heap {@link ByteBuffer}. Either way, the resulting {@link Dictionary}
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
    public static Dictionary<?> read(ByteBuffer page, PrimitiveKind kind, int valueCount, OptionalInt typeLength) {
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
                        () -> new IllegalArgumentException("typeLength required for FIXED_LEN_BYTE_ARRAY dictionary"));
                yield readFixedLenBinary(page, valueCount, valueLength);
            }
        };
    }

    private static Dictionary.BooleanDict readBooleans(ByteBuffer page, int n) {
        int requiredBytes = (n + 7) >>> 3;
        if (page.remaining() < requiredBytes) {
            throw new IllegalArgumentException("BOOLEAN dictionary page has " + page.remaining() + " byte(s) but "
                    + requiredBytes + " are required for " + n + " value(s)");
        }
        return new Dictionary.BooleanDict(BitSet.valueOf(page), n);
    }

    private static Dictionary.IntDict readInts(ByteBuffer page, int n) {
        IntBuffer view = page.duplicate().order(LITTLE_ENDIAN).asIntBuffer().limit(n);
        IntBuffer owned = IntBuffer.allocate(n).put(view).flip().asReadOnlyBuffer();
        return new Dictionary.IntDict(owned);
    }

    private static Dictionary.LongDict readLongs(ByteBuffer page, int n) {
        LongBuffer view = page.duplicate().order(LITTLE_ENDIAN).asLongBuffer().limit(n);
        LongBuffer owned = LongBuffer.allocate(n).put(view).flip().asReadOnlyBuffer();
        return new Dictionary.LongDict(owned);
    }

    private static Dictionary.FloatDict readFloats(ByteBuffer page, int n) {
        FloatBuffer view = page.duplicate().order(LITTLE_ENDIAN).asFloatBuffer().limit(n);
        FloatBuffer owned = FloatBuffer.allocate(n).put(view).flip().asReadOnlyBuffer();
        return new Dictionary.FloatDict(owned);
    }

    private static Dictionary.DoubleDict readDoubles(ByteBuffer page, int n) {
        DoubleBuffer view =
                page.duplicate().order(LITTLE_ENDIAN).asDoubleBuffer().limit(n);
        DoubleBuffer owned = DoubleBuffer.allocate(n).put(view).flip().asReadOnlyBuffer();
        return new Dictionary.DoubleDict(owned);
    }

    private static Dictionary.Int96Dict readInt96s(ByteBuffer page, int n) {
        PlainInt96Decoder decoder = new PlainInt96Decoder();
        decoder.load(page, n);
        List<MemorySegment> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            values.add(copyToOwnedSegment(decoder.next()));
        }
        return new Dictionary.Int96Dict(values);
    }

    private static Dictionary.BinaryDict readBinary(ByteBuffer page, int n) {
        PlainBinaryDecoder decoder = new PlainBinaryDecoder();
        decoder.load(page, n);
        List<MemorySegment> values = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            values.add(copyToOwnedSegment(decoder.next()));
        }
        return new Dictionary.BinaryDict(values);
    }

    private static Dictionary.FixedLenBinaryDict readFixedLenBinary(ByteBuffer page, int n, int length) {
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
