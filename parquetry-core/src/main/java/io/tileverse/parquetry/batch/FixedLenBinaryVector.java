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
import io.tileverse.parquetry.page.DeltaByteArrayDecoder;
import io.tileverse.parquetry.page.Dictionary;
import io.tileverse.parquetry.page.PageDecoder;
import io.tileverse.parquetry.page.PlainFixedLenBinaryDecoder;
import io.tileverse.parquetry.page.RleDictionaryPageDecoder;

/**
 * Column vector for FIXED_LEN_BYTE_ARRAY values.
 *
 * <p>Each element of the materialized {@code MemorySegment[]} is a read-only view into the page bytes of exactly
 * {@link #byteWidth()} bytes. Lifetime is bound to the batch's Arena.
 */
public final class FixedLenBinaryVector implements ColumnVector {

    private final int size;
    private final int byteWidth;
    private final BitSet validity;
    private MemorySegment rawPage;
    private Encoding encoding;
    private MemorySegment[] values;
    private Dictionary<?> dictionary;

    private FixedLenBinaryVector(MemorySegment rawPage, Encoding encoding, int size, int byteWidth, BitSet validity) {
        this.rawPage = rawPage;
        this.encoding = encoding;
        this.size = size;
        this.byteWidth = byteWidth;
        this.validity = validity;
    }

    /** Builds a raw (lazy) FixedLenBinaryVector backed by the given page bytes. */
    public static FixedLenBinaryVector raw(
            MemorySegment page, Encoding encoding, int size, int byteWidth, BitSet validity) {
        return new FixedLenBinaryVector(page, encoding, size, byteWidth, validity);
    }

    /**
     * Builds a raw (lazy) FixedLenBinaryVector backed by the given page bytes with an optional dictionary for
     * dictionary-encoded columns. {@code dictionary} may be {@code null} for non-dictionary encodings.
     */
    public static FixedLenBinaryVector raw(
            MemorySegment page, Encoding encoding, int size, int byteWidth, BitSet validity, Dictionary<?> dictionary) {
        FixedLenBinaryVector vec = new FixedLenBinaryVector(page, encoding, size, byteWidth, validity);
        vec.dictionary = dictionary;
        return vec;
    }

    /** Builds an already-materialized FixedLenBinaryVector. */
    public static FixedLenBinaryVector materialized(MemorySegment[] values, int byteWidth, BitSet validity) {
        FixedLenBinaryVector vec = new FixedLenBinaryVector(null, null, values.length, byteWidth, validity);
        vec.values = values;
        return vec;
    }

    @Override
    public int size() {
        return size;
    }

    /** Fixed width in bytes per value, as declared in the column schema's {@code typeLength}. */
    public int byteWidth() {
        return byteWidth;
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
        int nonNullCount = validity.cardinality();
        MemorySegment[] dst = new MemorySegment[size];
        if (nonNullCount > 0) {
            PageDecoder<?> decoder = decoderFor(encoding);
            decoder.load(asByteBuffer(rawPage), nonNullCount);
            if (nonNullCount == size) {
                decoder.decodeBinary(size, dst, 0);
            } else {
                MemorySegment[] dense = new MemorySegment[nonNullCount];
                decoder.decodeBinary(nonNullCount, dense, 0);
                spread(dense, dst);
            }
        }
        values = dst;
        rawPage = null;
        encoding = null;
        dictionary = null;
    }

    private void spread(MemorySegment[] dense, MemorySegment[] dst) {
        int denseIndex = 0;
        for (int i = validity.nextSetBit(0); i >= 0; i = validity.nextSetBit(i + 1)) {
            dst[i] = dense[denseIndex++];
        }
    }

    @Override
    public void materializeSurvivors(BitSet survivors) {
        materialize();
    }

    /** Returns the segment at row {@code row}; triggers full materialization if needed. */
    public MemorySegment get(int row) {
        if (values == null) {
            materialize();
        }
        return values[row];
    }

    /** Bulk accessor; triggers materialization if needed. */
    public MemorySegment[] asArray() {
        if (values == null) {
            materialize();
        }
        return values;
    }

    private PageDecoder<?> decoderFor(Encoding encoding) {
        return switch (encoding) {
            case PLAIN -> new PlainFixedLenBinaryDecoder(byteWidth);
            case DELTA_BYTE_ARRAY -> new DeltaByteArrayDecoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> {
                if (dictionary == null) {
                    throw new IllegalStateException(
                            "Dictionary-encoded FixedLenBinaryVector but no dictionary supplied");
                }
                yield new RleDictionaryPageDecoder<>(dictionary);
            }
            default ->
                throw new UnsupportedOperationException(
                        "FixedLenBinaryVector materialize not yet wired for encoding " + encoding);
        };
    }

    private static ByteBuffer asByteBuffer(MemorySegment segment) {
        return segment.asByteBuffer().order(LITTLE_ENDIAN);
    }
}
