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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.format.ParquetLayouts;

/**
 * Data-page decoder for dictionary-encoded columns (PLAIN_DICTIONARY / RLE_DICTIONARY).
 *
 * <p>Each value is an integer index into the column's {@link Dictionary}. Indexes are encoded with the RLE-Bit-Packed
 * hybrid; the bit width is stored as the first byte of the page payload (NOT in the page header). After reading the bit
 * width, the rest of the payload is delegated to a {@link LevelDecoder} configured at that bit width.
 *
 * @param <T> the dictionary value type
 */
public final class RleDictionaryPageDecoder<T> implements PageDecoder<T> {

    private final Dictionary<T> dictionary;
    private LevelDecoder indexDecoder;

    public RleDictionaryPageDecoder(Dictionary<T> dictionary) {
        this.dictionary = dictionary;
    }

    /**
     * Load a data page. Reads the bit-width byte first, then hands the remaining bytes to a new {@link LevelDecoder}
     * for index decoding.
     */
    @Override
    public void load(MemorySegment page, int valueCount) {
        int bitWidth = page.get(JAVA_BYTE, 0L) & 0xff;
        indexDecoder = new LevelDecoder(bitWidth);
        indexDecoder.load(page.asSlice(1L));
    }

    @Override
    public T next() {
        int index = indexDecoder.nextValue();
        return dictionary.get(index);
    }

    @Override
    public void skip(int n) {
        indexDecoder.skip(n);
    }

    /**
     * Decodes {@code n} raw dictionary indexes into {@code dst} starting at {@code offset}, without dictionary lookup.
     */
    public void decodeIndices(int n, int[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = indexDecoder.nextValue();
        }
    }

    /**
     * Decodes {@code n} raw dictionary indexes into {@code dst} as little-endian 32-bit ints, written unaligned and
     * without dictionary lookup. {@code dst} must cover at least {@code 4L * n} bytes.
     */
    public void decodeIndicesInto(int n, MemorySegment dst) {
        for (int i = 0; i < n; i++) {
            dst.setAtIndex(ParquetLayouts.INT32, i, indexDecoder.nextValue());
        }
    }

    @Override
    public void decodeInts(int n, int[] dst, int offset) {
        Dictionary.IntDict ints = (Dictionary.IntDict) dictionary;
        for (int i = 0; i < n; i++) {
            dst[offset + i] = ints.getInt(indexDecoder.nextValue());
        }
    }

    @Override
    public void decodeInts(int n, MemorySegment dst, long dstIndex) {
        Dictionary.IntDict ints = (Dictionary.IntDict) dictionary;
        for (int i = 0; i < n; i++) {
            dst.setAtIndex(ParquetLayouts.INT32, dstIndex + i, ints.getInt(indexDecoder.nextValue()));
        }
    }

    @Override
    public void decodeLongs(int n, long[] dst, int offset) {
        Dictionary.LongDict longs = (Dictionary.LongDict) dictionary;
        for (int i = 0; i < n; i++) {
            dst[offset + i] = longs.getLong(indexDecoder.nextValue());
        }
    }

    @Override
    public void decodeLongs(int n, MemorySegment dst, long dstIndex) {
        Dictionary.LongDict longs = (Dictionary.LongDict) dictionary;
        for (int i = 0; i < n; i++) {
            dst.setAtIndex(ParquetLayouts.INT64, dstIndex + i, longs.getLong(indexDecoder.nextValue()));
        }
    }

    @Override
    public void decodeFloats(int n, float[] dst, int offset) {
        Dictionary.FloatDict floats = (Dictionary.FloatDict) dictionary;
        for (int i = 0; i < n; i++) {
            dst[offset + i] = floats.getFloat(indexDecoder.nextValue());
        }
    }

    @Override
    public void decodeFloats(int n, MemorySegment dst, long dstIndex) {
        Dictionary.FloatDict floats = (Dictionary.FloatDict) dictionary;
        for (int i = 0; i < n; i++) {
            dst.setAtIndex(ParquetLayouts.FLOAT, dstIndex + i, floats.getFloat(indexDecoder.nextValue()));
        }
    }

    @Override
    public void decodeDoubles(int n, double[] dst, int offset) {
        Dictionary.DoubleDict doubles = (Dictionary.DoubleDict) dictionary;
        for (int i = 0; i < n; i++) {
            dst[offset + i] = doubles.getDouble(indexDecoder.nextValue());
        }
    }

    @Override
    public void decodeDoubles(int n, MemorySegment dst, long dstIndex) {
        Dictionary.DoubleDict doubles = (Dictionary.DoubleDict) dictionary;
        for (int i = 0; i < n; i++) {
            dst.setAtIndex(ParquetLayouts.DOUBLE, dstIndex + i, doubles.getDouble(indexDecoder.nextValue()));
        }
    }

    @Override
    public void decodeBooleans(int n, boolean[] dst, int offset) {
        Dictionary.BooleanDict booleans = (Dictionary.BooleanDict) dictionary;
        for (int i = 0; i < n; i++) {
            dst[offset + i] = booleans.getBoolean(indexDecoder.nextValue());
        }
    }

    @Override
    public void decodeBinary(int n, MemorySegment[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = (MemorySegment) next();
        }
    }
}
