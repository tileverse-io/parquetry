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
package io.tileverse.parquetry.avro;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * A forward positional cursor over a {@link ByteRangeSource} for OCF container framing: the magic, the metadata map,
 * sync markers, and per-block headers. Reads varints byte-by-byte (headers are tiny) and bulk-reads known-length
 * payloads. Thread-confined; create one per read pass.
 */
final class ByteRangeCursor {

    private final ByteRangeSource source;
    private final MemorySegment oneByte = MemorySegment.ofArray(new byte[1]);
    private long position;

    ByteRangeCursor(ByteRangeSource source) {
        this(source, 0);
    }

    /** A cursor starting at {@code initialOffset}; the source is positional, no seek is involved. */
    ByteRangeCursor(ByteRangeSource source, long initialOffset) {
        this.source = source;
        this.position = initialOffset;
    }

    long position() {
        return position;
    }

    boolean hasRemaining() {
        return position < source.size();
    }

    int readRawByte() {
        int read = source.read(position, oneByte);
        if (read < 0) {
            throw new AvroFormatException("Unexpected end of file at offset " + position);
        }
        position++;
        return oneByte.get(ValueLayout.JAVA_BYTE, 0) & 0xff;
    }

    long readLong() {
        return Varints.decodeZigZag(readUnsignedVarLong());
    }

    MemorySegment readSegment(int length) {
        MemorySegment target = MemorySegment.ofArray(new byte[length]);
        source.readFully(position, target);
        position += length;
        return target;
    }

    /** Reads exactly {@code target.byteSize()} bytes at the current position into {@code target}, then advances. */
    void readInto(MemorySegment target) {
        source.readFully(position, target);
        position += target.byteSize();
    }

    void skip(long count) {
        long target = position + count;
        if (count < 0 || target > source.size()) {
            throw new AvroFormatException("Skip past end of file to offset " + target);
        }
        position = target;
    }

    private long readUnsignedVarLong() {
        long result = 0L;
        int shift = 0;
        while (true) {
            int b = readRawByte();
            result |= ((long) (b & 0x7f)) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
            shift += 7;
            if (shift > 63) {
                throw new AvroFormatException("Varint too long");
            }
        }
    }
}
