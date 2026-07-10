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

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

/**
 * A forward encoder of Avro binary datums into a growable buffer, the exact inverse of {@link AvroBinaryDecoder}: longs
 * and ints are zigzag varints, float and double are little-endian, strings and bytes are length-prefixed, fixed is raw.
 * Thread-confined; create one per block being assembled.
 */
final class AvroBinaryEncoder {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    void writeBoolean(boolean value) {
        out.write(value ? 1 : 0);
    }

    void writeInt(int value) {
        writeLong(value);
    }

    void writeLong(long value) {
        long zigzag = Varints.encodeZigZag(value);
        while ((zigzag & ~0x7FL) != 0) {
            out.write((int) ((zigzag & 0x7F) | 0x80));
            zigzag >>>= 7;
        }
        out.write((int) zigzag);
    }

    void writeFloat(float value) {
        writeLittleEndian(Float.floatToRawIntBits(value), Float.BYTES);
    }

    void writeDouble(double value) {
        writeLittleEndian(Double.doubleToRawLongBits(value), Double.BYTES);
    }

    void writeString(String value) {
        writeLengthPrefixed(value.getBytes(StandardCharsets.UTF_8));
    }

    void writeBytes(MemorySegment value) {
        writeLengthPrefixed(value.toArray(ValueLayout.JAVA_BYTE));
    }

    void writeBytes(byte[] value) {
        writeLengthPrefixed(value);
    }

    /** Raw bytes with no length prefix (Avro {@code fixed}, and the OCF magic and sync marker). */
    void writeFixed(byte[] value) {
        out.writeBytes(value);
    }

    int size() {
        return out.size();
    }

    byte[] toByteArray() {
        return out.toByteArray();
    }

    void reset() {
        out.reset();
    }

    private void writeLengthPrefixed(byte[] value) {
        writeLong(value.length);
        out.writeBytes(value);
    }

    private void writeLittleEndian(long bits, int byteCount) {
        for (int i = 0; i < byteCount; i++) {
            out.write((int) ((bits >>> (8 * i)) & 0xFF));
        }
    }
}
