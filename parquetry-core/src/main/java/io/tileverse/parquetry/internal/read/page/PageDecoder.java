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
package io.tileverse.parquetry.internal.read.page;

import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.format.ParquetLayouts;

/**
 * Lazily materializes column values from one decompressed data-page payload.
 *
 * <p>Two access modes coexist: per-value {@link #next()} (used by the legacy row-API path) and primitive-specialized
 * bulk {@code decode...} methods (used by the batch driver). Concrete decoders override the bulk method matching their
 * value kind. The default implementations delegate to {@link #next()} so an unconverted decoder is correct, just not
 * bulk-optimized.
 *
 * <p>{@link #skip(int)} advances the cursor without producing values. Thread-confined; one decoder per column reader
 * per page.
 *
 * <p>{@code valueCount} is the page header's {@code numValues} - the logical row count <em>including nulls</em>. PLAIN
 * encoding stores only the non-null values; an all-null OPTIONAL page therefore validly has {@code valueCount = N} and
 * zero value bytes; the column reader will not call {@link #next()} for those rows. Implementations must therefore not
 * cap the input buffer at {@code valueCount} (e.g. {@code asIntBuffer().limit(valueCount)}); the buffer's natural
 * capacity already reflects what was written. Over-consumption past that capacity should fail loudly.
 */
public interface PageDecoder<T> {

    void load(MemorySegment page, int valueCount);

    T next();

    void skip(int n);

    /** Bulk decode {@code n} ints. */
    default void decodeInts(int n, int[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = (Integer) next();
        }
    }

    /** Bulk decode {@code n} longs. */
    default void decodeLongs(int n, long[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = (Long) next();
        }
    }

    /** Bulk decode {@code n} floats. */
    default void decodeFloats(int n, float[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = (Float) next();
        }
    }

    /** Bulk decode {@code n} doubles. */
    default void decodeDoubles(int n, double[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = (Double) next();
        }
    }

    /**
     * Bulk decode {@code n} ints into {@code dst} starting at element index {@code dstIndex}, in the unaligned
     * little-endian {@link ParquetLayouts#INT32} layout (pooled destination segments make no alignment promise).
     */
    default void decodeInts(int n, MemorySegment dst, long dstIndex) {
        for (int i = 0; i < n; i++) {
            dst.setAtIndex(ParquetLayouts.INT32, dstIndex + i, (Integer) next());
        }
    }

    /**
     * Bulk decode {@code n} longs into {@code dst} starting at element index {@code dstIndex}
     * ({@link ParquetLayouts#INT64}).
     */
    default void decodeLongs(int n, MemorySegment dst, long dstIndex) {
        for (int i = 0; i < n; i++) {
            dst.setAtIndex(ParquetLayouts.INT64, dstIndex + i, (Long) next());
        }
    }

    /**
     * Bulk decode {@code n} floats into {@code dst} starting at element index {@code dstIndex}
     * ({@link ParquetLayouts#FLOAT}).
     */
    default void decodeFloats(int n, MemorySegment dst, long dstIndex) {
        for (int i = 0; i < n; i++) {
            dst.setAtIndex(ParquetLayouts.FLOAT, dstIndex + i, (Float) next());
        }
    }

    /**
     * Bulk decode {@code n} doubles into {@code dst} starting at element index {@code dstIndex}
     * ({@link ParquetLayouts#DOUBLE}).
     */
    default void decodeDoubles(int n, MemorySegment dst, long dstIndex) {
        for (int i = 0; i < n; i++) {
            dst.setAtIndex(ParquetLayouts.DOUBLE, dstIndex + i, (Double) next());
        }
    }

    /** Bulk decode {@code n} booleans (one element per row, not bit-packed). */
    default void decodeBooleans(int n, boolean[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = (Boolean) next();
        }
    }

    /** Bulk decode {@code n} binary values as read-only {@link MemorySegment} views. */
    default void decodeBinary(int n, MemorySegment[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = (MemorySegment) next();
        }
    }

    /**
     * Records, for {@code n} dense (non-null) values, each value's absolute byte offset within the loaded page value
     * buffer (into {@code positions}) and its byte length (into {@code lengths}), starting at {@code offset}, without
     * copying any value bytes. A caller copies the bytes later straight from the page. Only the contiguous binary
     * decoders (PLAIN, DELTA_LENGTH_BYTE_ARRAY) implement this.
     */
    default void decodeBinaryLayout(int n, int[] positions, int[] lengths, int offset) {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " does not support direct binary layout decoding");
    }
}
