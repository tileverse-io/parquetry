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
package io.tileverse.parquetry.batch;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.BitSet;

import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.page.PageDecoder;
import io.tileverse.parquetry.page.PlainInt64Decoder;

public final class LongVector implements ColumnVector {

    private final int size;
    private final BitSet validity;
    private MemorySegment rawPage;
    private Encoding encoding;
    private long[] values;

    private LongVector(MemorySegment rawPage, Encoding encoding, int size, BitSet validity) {
        this.rawPage = rawPage;
        this.encoding = encoding;
        this.size = size;
        this.validity = validity;
    }

    /** Builds a raw (lazy) LongVector backed by {@code page}. */
    public static LongVector raw(MemorySegment page, Encoding encoding, int size, BitSet validity) {
        return new LongVector(page, encoding, size, validity);
    }

    /** Builds an already-materialized LongVector. Used when tests or upstream code already has the long[]. */
    public static LongVector materialized(long[] values, BitSet validity) {
        LongVector vec = new LongVector(null, null, values.length, validity);
        vec.values = values;
        return vec;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public BitSet validity() {
        return validity;
    }

    @Override
    public boolean isMaterialized() {
        return values != null;
    }

    @Override
    public void materialize() {
        if (values != null) {
            return;
        }
        long[] dst = new long[size];
        PageDecoder<?> decoder = decoderFor(encoding);
        decoder.load(asByteBuffer(rawPage), size);
        decoder.decodeLongs(size, dst, 0);
        values = dst;
        rawPage = null;
        encoding = null;
    }

    @Override
    public void materializeSurvivors(BitSet survivors) {
        materialize();
    }

    /** Returns the value at row {@code row}; triggers full materialization if needed. */
    public long get(int row) {
        if (values == null) {
            materialize();
        }
        return values[row];
    }

    /** Bulk accessor; triggers materialization if needed. */
    public long[] asArray() {
        if (values == null) {
            materialize();
        }
        return values;
    }

    private static PageDecoder<?> decoderFor(Encoding encoding) {
        return switch (encoding) {
            case PLAIN -> new PlainInt64Decoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY ->
                throw new UnsupportedOperationException(
                        "Dictionary-encoded LongVector requires the chunk's Dictionary; wired by BatchColumnReader.");
            default ->
                throw new UnsupportedOperationException(
                        "LongVector materialize not yet wired for encoding " + encoding);
        };
    }

    private static ByteBuffer asByteBuffer(MemorySegment segment) {
        return segment.asByteBuffer().order(LITTLE_ENDIAN);
    }
}
