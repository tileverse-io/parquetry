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
import io.tileverse.parquetry.page.ByteStreamSplitDoubleDecoder;
import io.tileverse.parquetry.page.Dictionary;
import io.tileverse.parquetry.page.PageDecoder;
import io.tileverse.parquetry.page.PlainDoubleDecoder;
import io.tileverse.parquetry.page.RleDictionaryPageDecoder;

public final class DoubleVector implements ColumnVector {

    private final int size;
    private final BitSet validity;
    private MemorySegment rawPage;
    private Encoding encoding;
    private double[] values;
    private Dictionary<?> dictionary;

    private DoubleVector(MemorySegment rawPage, Encoding encoding, int size, BitSet validity) {
        this.rawPage = rawPage;
        this.encoding = encoding;
        this.size = size;
        this.validity = validity;
    }

    /** Builds a raw (lazy) DoubleVector backed by {@code page}. */
    public static DoubleVector raw(MemorySegment page, Encoding encoding, int size, BitSet validity) {
        return new DoubleVector(page, encoding, size, validity);
    }

    /**
     * Builds a raw (lazy) DoubleVector backed by {@code page} with an optional dictionary for dictionary-encoded
     * columns. {@code dictionary} may be {@code null} for non-dictionary encodings.
     */
    public static DoubleVector raw(
            MemorySegment page, Encoding encoding, int size, BitSet validity, Dictionary<?> dictionary) {
        DoubleVector vec = new DoubleVector(page, encoding, size, validity);
        vec.dictionary = dictionary;
        return vec;
    }

    /** Builds an already-materialized DoubleVector. Used when tests or upstream code already has the double[]. */
    public static DoubleVector materialized(double[] values, BitSet validity) {
        DoubleVector vec = new DoubleVector(null, null, values.length, validity);
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
        double[] dst = new double[size];
        PageDecoder<?> decoder = decoderFor(encoding);
        decoder.load(asByteBuffer(rawPage), size);
        decoder.decodeDoubles(size, dst, 0);
        values = dst;
        rawPage = null;
        encoding = null;
        dictionary = null;
    }

    @Override
    public void materializeSurvivors(BitSet survivors) {
        materialize();
    }

    /** Returns the value at row {@code row}; triggers full materialization if needed. */
    public double get(int row) {
        if (values == null) {
            materialize();
        }
        return values[row];
    }

    /** Bulk accessor; triggers materialization if needed. */
    public double[] asArray() {
        if (values == null) {
            materialize();
        }
        return values;
    }

    private PageDecoder<?> decoderFor(Encoding encoding) {
        return switch (encoding) {
            case PLAIN -> new PlainDoubleDecoder();
            case BYTE_STREAM_SPLIT -> new ByteStreamSplitDoubleDecoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> {
                if (dictionary == null) {
                    throw new IllegalStateException("Dictionary-encoded DoubleVector but no dictionary supplied");
                }
                yield new RleDictionaryPageDecoder<>(dictionary);
            }
            default ->
                throw new UnsupportedOperationException(
                        "DoubleVector materialize not yet wired for encoding " + encoding);
        };
    }

    private static ByteBuffer asByteBuffer(MemorySegment segment) {
        return segment.asByteBuffer().order(LITTLE_ENDIAN);
    }
}
