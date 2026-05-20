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

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/**
 * Lazily materializes column values from one decompressed data-page payload.
 *
 * <p>Two access modes coexist: per-value {@link #next()} (used by the legacy row-API path until the row-on-batch
 * reimplementation lands) and primitive-specialized bulk {@code decode...} methods (used by the batch driver). Concrete
 * decoders override the bulk method matching their value kind. The default implementations delegate to {@link #next()}
 * so an unconverted decoder is correct, just not bulk-optimized.
 *
 * <p>{@link #skip(int)} advances the cursor without producing values. Thread-confined; one decoder per column reader
 * per page.
 *
 * <p>{@code valueCount} is the page header's {@code numValues} - the logical row count <em>including nulls</em>. PLAIN
 * encoding stores only the non-null values, so an all-null OPTIONAL page validly has {@code valueCount = N} and zero
 * value bytes; the column reader will not call {@link #next()} for those rows. Implementations must therefore not cap
 * the input buffer at {@code valueCount} (e.g. {@code asIntBuffer().limit(valueCount)}); the buffer's natural capacity
 * already reflects what was written. Over-consumption past that capacity should fail loudly.
 */
public interface PageDecoder<T> {

    void load(ByteBuffer page, int valueCount);

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
}
