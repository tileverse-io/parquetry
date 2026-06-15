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

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/** Test-only encoder for the Avro binary subset, mirroring AvroBinaryDecoder. */
final class AvroTestEncoder {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    AvroTestEncoder longValue(long value) {
        OcfTestWriter.varLong(out, value);
        return this;
    }

    AvroTestEncoder intValue(int value) {
        OcfTestWriter.varLong(out, value);
        return this;
    }

    AvroTestEncoder string(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        OcfTestWriter.varLong(out, bytes.length);
        out.writeBytes(bytes);
        return this;
    }

    AvroTestEncoder doubleValue(double value) {
        long bits = Double.doubleToLongBits(value);
        for (int i = 0; i < 8; i++) {
            out.write((int) ((bits >>> (8 * i)) & 0xff));
        }
        return this;
    }

    AvroTestEncoder floatValue(float value) {
        int bits = Float.floatToIntBits(value);
        for (int i = 0; i < 4; i++) {
            out.write((bits >>> (8 * i)) & 0xff);
        }
        return this;
    }

    AvroTestEncoder booleanValue(boolean value) {
        out.write(value ? 1 : 0);
        return this;
    }

    /** A length-prefixed byte string (Avro {@code bytes}). */
    AvroTestEncoder bytes(byte[] value) {
        OcfTestWriter.varLong(out, value.length);
        out.writeBytes(value);
        return this;
    }

    /** Appends bytes verbatim: fixed values, pre-encoded datums, padding. */
    AvroTestEncoder raw(byte[] value) {
        out.writeBytes(value);
        return this;
    }

    byte[] toBytes() {
        return out.toByteArray();
    }
}
