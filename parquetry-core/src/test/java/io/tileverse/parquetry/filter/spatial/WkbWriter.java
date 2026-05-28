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
package io.tileverse.parquetry.filter.spatial;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Builds a WKB payload byte-by-byte for the handful of tests JTS cannot produce: ISO Z/M/ZM type codes
 * ({@code +1000/+2000/+3000}), which JTS's WKBWriter does not emit -- it uses EWKB high-bit flags instead. The everyday
 * 2D path uses {@code io.tileverse.parquetry.testsupport.Wkb} (JTS-backed) so we read what an independent reference
 * encoder writes.
 */
final class WkbWriter {

    private final ByteBuffer buffer;

    WkbWriter(ByteOrder order, int wkbType) {
        this.buffer = ByteBuffer.allocate(64).order(order);
        byte orderByte = (byte) (order == ByteOrder.LITTLE_ENDIAN ? 0x01 : 0x00);
        buffer.put(orderByte);
        buffer.putInt(wkbType);
    }

    void writeDouble(double value) {
        buffer.putDouble(value);
    }

    MemorySegment toSegment() {
        byte[] copy = new byte[buffer.position()];
        buffer.flip();
        buffer.get(copy);
        return MemorySegment.ofArray(copy).asReadOnly();
    }
}
