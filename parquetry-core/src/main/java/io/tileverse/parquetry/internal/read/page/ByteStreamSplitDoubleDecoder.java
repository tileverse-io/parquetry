/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
 * BYTE_STREAM_SPLIT page decoder for DOUBLE (8-byte values).
 *
 * <p>Per parquet-format Encodings.md: the page is laid out as 8 byte streams of N bytes each (where N is the value
 * count). Value {@code i} is reconstructed by taking byte {@code s} from stream {@code s} at index {@code i}, for s in
 * [0, 8).
 *
 * <p>The layout is:
 *
 * <pre>
 *   [byte0 of val0, byte0 of val1, ..., byte0 of valN-1,
 *    byte1 of val0, byte1 of val1, ..., byte1 of valN-1,
 *    ...
 *    byte7 of val0, byte7 of val1, ..., byte7 of valN-1]
 * </pre>
 *
 * <p>Bytes are assembled little-endian into the IEEE 754 bit pattern.
 */
public final class ByteStreamSplitDoubleDecoder implements PageDecoder<Double> {

    private static final int BYTES_PER_VALUE = 8;
    private byte[] streams;
    private int valueCount;
    private int cursor;

    @Override
    public void load(MemorySegment page, int valueCount) {
        this.valueCount = valueCount;
        int total = valueCount * BYTES_PER_VALUE;
        this.streams = new byte[total];
        MemorySegment.copy(page, JAVA_BYTE, 0L, streams, 0, total);
        this.cursor = 0;
    }

    @Override
    public Double next() {
        return Double.longBitsToDouble(nextBits());
    }

    @Override
    public void decodeDoubles(int n, double[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = Double.longBitsToDouble(nextBits());
        }
    }

    @Override
    public void decodeDoubles(int n, MemorySegment dst, long dstIndex) {
        for (int i = 0; i < n; i++) {
            dst.setAtIndex(ParquetLayouts.DOUBLE, dstIndex + i, Double.longBitsToDouble(nextBits()));
        }
    }

    private long nextBits() {
        long bits = 0L;
        for (int s = 0; s < BYTES_PER_VALUE; s++) {
            long b = streams[s * valueCount + cursor] & 0xffL;
            bits |= b << (s * 8);
        }
        cursor++;
        return bits;
    }

    @Override
    public void skip(int n) {
        cursor += n;
    }
}
