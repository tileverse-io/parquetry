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
package io.tileverse.parquetry.avro;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * A forward, cursor-based decoder of Avro binary datums over an in-memory {@link MemorySegment} (a decompressed OCF
 * block). Thread-confined; create one per block. All multi-byte numerics are little-endian per the Avro spec.
 */
final class AvroBinaryDecoder {

    private static final ValueLayout.OfDouble LE_DOUBLE =
            ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfFloat LE_FLOAT =
            ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    private final MemorySegment segment;
    private long position;

    AvroBinaryDecoder(MemorySegment segment) {
        this.segment = segment;
    }

    boolean hasRemaining() {
        return position < segment.byteSize();
    }

    long readLong() {
        return Varints.decodeZigZag(readUnsignedVarLong());
    }

    int readInt() {
        long value = readLong();
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new AvroFormatException("Varint value out of int range: " + value);
        }
        return (int) value;
    }

    boolean readBoolean() {
        return readRawByte() != 0;
    }

    double readDouble() {
        requireRemaining(Double.BYTES);
        double value = segment.get(LE_DOUBLE, position);
        position += Double.BYTES;
        return value;
    }

    float readFloat() {
        requireRemaining(Float.BYTES);
        float value = segment.get(LE_FLOAT, position);
        position += Float.BYTES;
        return value;
    }

    String readString() {
        int length = lengthPrefix();
        byte[] bytes = bulk(length);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /** A length-prefixed byte string, returned as a read-only heap segment that no other code shares. */
    MemorySegment readBytes() {
        return fixed(lengthPrefix());
    }

    /** Exactly {@code size} raw bytes (Avro {@code fixed}), returned as a read-only heap segment. */
    MemorySegment fixed(int size) {
        return MemorySegment.ofArray(bulk(size)).asReadOnly();
    }

    /** Advances past {@code count} bytes without decoding them. */
    void skip(long count) {
        if (count < 0) {
            throw new AvroFormatException("Negative skip length: " + count);
        }
        if (count > remaining()) {
            throw new AvroFormatException("Skip of " + count + " bytes past end of Avro block at offset " + position);
        }
        position += count;
    }

    /** Bytes left between the current position and the end of input. */
    long remaining() {
        return segment.byteSize() - position;
    }

    /**
     * Rejects an array/map block item count larger than the remaining input. Such a count is structurally impossible
     * when every item occupies at least one byte; callers apply this only to blocks whose items have a nonzero minimum
     * wire size (map keys always do).
     */
    void requireBlockCountWithinInput(long count) {
        if (count > remaining()) {
            throw new AvroFormatException(
                    "Block item count " + count + " exceeds remaining input bytes " + remaining());
        }
    }

    private int lengthPrefix() {
        long length = readLong();
        if (length < 0 || length > Integer.MAX_VALUE) {
            throw new AvroFormatException("Invalid length prefix: " + length);
        }
        return (int) length;
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

    private int readRawByte() {
        if (position >= segment.byteSize()) {
            throw new AvroFormatException("Unexpected end of Avro block at offset " + position);
        }
        int b = segment.get(ValueLayout.JAVA_BYTE, position) & 0xff;
        position++;
        return b;
    }

    private byte[] bulk(int length) {
        if (length < 0) {
            throw new AvroFormatException("Negative read length: " + length);
        }
        requireRemaining(length);
        MemorySegment slice = segment.asSlice(position, length);
        byte[] bytes = slice.toArray(ValueLayout.JAVA_BYTE);
        position += length;
        return bytes;
    }

    private void requireRemaining(int n) {
        if (position + n > segment.byteSize()) {
            throw new AvroFormatException("Read of " + n + " bytes past end of Avro block at offset " + position);
        }
    }
}
