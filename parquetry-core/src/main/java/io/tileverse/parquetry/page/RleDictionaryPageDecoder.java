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
 * Data-page decoder for dictionary-encoded columns (PLAIN_DICTIONARY / RLE_DICTIONARY).
 *
 * <p>Each value is an integer index into the column's {@link Dictionary}. Indexes are encoded with the RLE-Bit-Packed
 * hybrid; the bit width is carried as the first byte of the page payload (NOT in the page header). After reading the
 * bit width, the rest of the payload is delegated to a {@link LevelDecoder} configured at that bit width.
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
    public void load(ByteBuffer page, int valueCount) {
        int bitWidth = page.get() & 0xff;
        indexDecoder = new LevelDecoder(bitWidth);
        indexDecoder.load(page.slice());
        // Advance position to end so callers see the full page as consumed.
        page.position(page.limit());
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

    @Override
    public void decodeInts(int n, int[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = (Integer) next();
        }
    }

    @Override
    public void decodeLongs(int n, long[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = (Long) next();
        }
    }

    @Override
    public void decodeFloats(int n, float[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = (Float) next();
        }
    }

    @Override
    public void decodeDoubles(int n, double[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = (Double) next();
        }
    }

    @Override
    public void decodeBooleans(int n, boolean[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = (Boolean) next();
        }
    }

    @Override
    public void decodeBinary(int n, MemorySegment[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = (MemorySegment) next();
        }
    }
}
