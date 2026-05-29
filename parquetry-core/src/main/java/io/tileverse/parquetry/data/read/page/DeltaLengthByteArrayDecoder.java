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

import java.lang.foreign.MemorySegment;

/**
 * DELTA_LENGTH_BYTE_ARRAY page decoder.
 *
 * <p>Per parquet-format Encodings.md: lengths are encoded with DELTA_BINARY_PACKED int32, followed by the concatenated
 * payload bytes. Each {@link #next()} yields a read-only {@link MemorySegment} view of the payload.
 *
 * <p>The length stream is decoded first; the length decoder reports how many bytes it consumed, so the payload starts
 * right after it.
 */
public final class DeltaLengthByteArrayDecoder implements PageDecoder<MemorySegment> {

    private int[] lengths;
    private int cursor;
    private MemorySegment payload;
    private long payloadOffset;

    @Override
    public void load(MemorySegment page, int valueCount) {
        DeltaBinaryPackedDecoder lengthDecoder = new DeltaBinaryPackedDecoder();
        lengthDecoder.load(page);

        int totalLengths = lengthDecoder.totalValueCount();
        this.lengths = new int[totalLengths];
        for (int i = 0; i < totalLengths; i++) {
            this.lengths[i] = (int) lengthDecoder.next();
        }

        this.cursor = 0;
        this.payload = page.asSlice(lengthDecoder.position());
        this.payloadOffset = 0L;
    }

    @Override
    public MemorySegment next() {
        int len = lengths[cursor++];
        MemorySegment slice = payload.asSlice(payloadOffset, len).asReadOnly();
        payloadOffset += len;
        return slice;
    }

    @Override
    public void decodeBinary(int n, MemorySegment[] dst, int offset) {
        for (int i = 0; i < n; i++) {
            dst[offset + i] = next();
        }
    }

    @Override
    public void skip(int n) {
        for (int i = 0; i < n; i++) {
            int len = lengths[cursor++];
            payloadOffset += len;
        }
    }
}
