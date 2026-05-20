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
 * DELTA_LENGTH_BYTE_ARRAY page decoder.
 *
 * <p>Per parquet-format Encodings.md: lengths are encoded with DELTA_BINARY_PACKED int32, followed by the concatenated
 * payload bytes. Each {@link #next()} yields a read-only {@link MemorySegment} view of the payload.
 *
 * <p>The length stream is decoded first (consuming bytes from the page buffer via DELTA_BINARY_PACKED's lazy block
 * reads). After all lengths are read, the buffer's position sits at the start of the concatenated payload, ready to
 * slice.
 */
final class DeltaLengthByteArrayDecoder implements PageDecoder<MemorySegment> {

    private int[] lengths;
    private int cursor;
    private ByteBuffer payload;

    @Override
    public void load(ByteBuffer page, int valueCount) {
        // Use a slice so the caller's buffer position is not disturbed.
        // DeltaBinaryPackedDecoder mutates view.position() as it reads,
        // so after decoding all lengths, view.position() is at the payload start.
        ByteBuffer view = page.slice();
        DeltaBinaryPackedDecoder lengthDecoder = new DeltaBinaryPackedDecoder();
        lengthDecoder.load(view);

        int totalLengths = lengthDecoder.totalValueCount();
        this.lengths = new int[totalLengths];
        for (int i = 0; i < totalLengths; i++) {
            this.lengths[i] = (int) lengthDecoder.next();
        }

        this.cursor = 0;
        // view.position() has been advanced past all the lengths data by the decoder reads.
        this.payload = view;
    }

    @Override
    public MemorySegment next() {
        int len = lengths[cursor++];
        ByteBuffer slice = payload.slice().limit(len);
        payload.position(payload.position() + len);
        return MemorySegment.ofBuffer(slice).asReadOnly();
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
            payload.position(payload.position() + len);
        }
    }
}
