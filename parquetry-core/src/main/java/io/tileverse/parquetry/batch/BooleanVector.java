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
import io.tileverse.parquetry.page.PlainBooleanDecoder;
import io.tileverse.parquetry.page.RleBooleanDecoder;

public final class BooleanVector implements ColumnVector {

    private final int size;
    private final BitSet validity;
    private MemorySegment rawPage;
    private Encoding encoding;
    private boolean[] values;

    private BooleanVector(MemorySegment rawPage, Encoding encoding, int size, BitSet validity) {
        this.rawPage = rawPage;
        this.encoding = encoding;
        this.size = size;
        this.validity = validity;
    }

    /** Builds a raw (lazy) BooleanVector backed by {@code page}. */
    public static BooleanVector raw(MemorySegment page, Encoding encoding, int size, BitSet validity) {
        return new BooleanVector(page, encoding, size, validity);
    }

    /** Builds an already-materialized BooleanVector. Used when tests or upstream code already has the boolean[]. */
    public static BooleanVector materialized(boolean[] values, BitSet validity) {
        BooleanVector vec = new BooleanVector(null, null, values.length, validity);
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
        boolean[] dst = new boolean[size];
        PageDecoder<?> decoder = decoderFor(encoding);
        decoder.load(asByteBuffer(rawPage), size);
        decoder.decodeBooleans(size, dst, 0);
        values = dst;
        rawPage = null;
        encoding = null;
    }

    @Override
    public void materializeSurvivors(BitSet survivors) {
        materialize();
    }

    /** Returns the value at row {@code row}; triggers full materialization if needed. */
    public boolean get(int row) {
        if (values == null) {
            materialize();
        }
        return values[row];
    }

    /** Bulk accessor; triggers materialization if needed. */
    public boolean[] asArray() {
        if (values == null) {
            materialize();
        }
        return values;
    }

    private static PageDecoder<?> decoderFor(Encoding encoding) {
        return switch (encoding) {
            case PLAIN -> new PlainBooleanDecoder();
            case RLE -> new RleBooleanDecoder();
            default ->
                throw new UnsupportedOperationException(
                        "BooleanVector materialize not yet wired for encoding " + encoding);
        };
    }

    private static ByteBuffer asByteBuffer(MemorySegment segment) {
        return segment.asByteBuffer().order(LITTLE_ENDIAN);
    }
}
