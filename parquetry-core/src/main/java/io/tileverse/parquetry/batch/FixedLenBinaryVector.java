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
import io.tileverse.parquetry.page.PageDecoder;
import io.tileverse.parquetry.page.PlainFixedLenBinaryDecoder;

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
        MemorySegment[] dst = new MemorySegment[size];
        PageDecoder<?> decoder = decoderFor(encoding);
        decoder.load(asByteBuffer(rawPage), size);
        decoder.decodeBinary(size, dst, 0);
        values = dst;
        rawPage = null;
        encoding = null;
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
            case RLE_DICTIONARY, PLAIN_DICTIONARY ->
                throw new UnsupportedOperationException(
                        "Dictionary-encoded FixedLenBinaryVector requires the chunk's Dictionary; wired by BatchColumnReader.");
            default ->
                throw new UnsupportedOperationException(
                        "FixedLenBinaryVector materialize not yet wired for encoding " + encoding);
        };
    }

    private static ByteBuffer asByteBuffer(MemorySegment segment) {
        return segment.asByteBuffer().order(LITTLE_ENDIAN);
    }
}
