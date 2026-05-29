/*
 * Copyright (c) 2026 Multivers.io
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
package io.tileverse.parquetry.data.read.page;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;

/**
 * BYTE_STREAM_SPLIT page decoder for FLOAT (4-byte values).
 *
 * <p>Per parquet-format Encodings.md: the page is laid out as 4 byte streams of N bytes each (where N is the value
 * count). Value {@code i} is reconstructed by taking byte {@code s} from stream {@code s} at index {@code i}, for s in
 * [0, 4).
 *
 * <p>The layout is:
 *
 * <pre>
 *   [byte0 of val0, byte0 of val1, ..., byte0 of valN-1,
 *    byte1 of val0, byte1 of val1, ..., byte1 of valN-1,
 *    byte2 of val0, byte2 of val1, ..., byte2 of valN-1,
 *    byte3 of val0, byte3 of val1, ..., byte3 of valN-1]
 * </pre>
 *
 * <p>Bytes are assembled little-endian into the IEEE 754 bit pattern.
 */
public final class ByteStreamSplitFloatDecoder implements PageDecoder<Float> {

    private static final int BYTES_PER_VALUE = 4;
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
    public Float next() {
        int bits = 0;
        for (int s = 0; s < BYTES_PER_VALUE; s++) {
            int b = streams[s * valueCount + cursor] & 0xff;
            bits |= b << (s * 8);
        }
        cursor++;
        return Float.intBitsToFloat(bits);
    }

    @Override
    public void decodeFloats(int n, float[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = next();
        }
    }

    @Override
    public void skip(int n) {
        cursor += n;
    }
}
