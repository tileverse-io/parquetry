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

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;

import java.lang.foreign.MemorySegment;

/** PLAIN decoder for DOUBLE: eight bytes, little-endian IEEE 754 per value. */
public final class PlainDoubleDecoder implements PageDecoder<Double> {

    private static final int BYTES_PER_VALUE = Double.BYTES;

    private MemorySegment segment;
    private long offset;

    @Override
    public void load(MemorySegment page, int valueCount) {
        this.segment = page;
        this.offset = 0L;
    }

    @Override
    public Double next() {
        double value = segment.get(DOUBLE, offset);
        offset += BYTES_PER_VALUE;
        return value;
    }

    @Override
    public void decodeDoubles(int n, double[] dst, int dstOffset) {
        MemorySegment.copy(segment, DOUBLE, offset, dst, dstOffset, n);
        offset += (long) n * BYTES_PER_VALUE;
    }

    @Override
    public void decodeDoubles(int n, MemorySegment dst, long dstIndex) {
        long byteCount = (long) n * BYTES_PER_VALUE;
        MemorySegment.copy(segment, offset, dst, dstIndex * BYTES_PER_VALUE, byteCount);
        offset += byteCount;
    }

    @Override
    public void skip(int n) {
        offset += (long) n * BYTES_PER_VALUE;
    }
}
