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
package io.tileverse.parquetry.format.codec;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;

class CompactProtocolReaderPropertyTest {

    @Property
    void zigzagI32_roundTrip(@ForAll int original) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        writeZigzagVarLong(bytes, original);
        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(bytes.toByteArray()));
        assertThat(r.readI32()).isEqualTo(original);
    }

    @Property
    void zigzagI64_roundTrip(@ForAll long original) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        writeZigzagVarLong(bytes, original);
        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(bytes.toByteArray()));
        assertThat(r.readI64()).isEqualTo(original);
    }

    @Property
    void doubleRoundTrip(@ForAll double original) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        long bits = Double.doubleToRawLongBits(original);
        for (int i = 0; i < 8; i++) {
            bytes.write((int) ((bits >>> (i * 8)) & 0xff));
        }
        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(bytes.toByteArray()));
        double decoded = r.readDouble();
        if (Double.isNaN(original)) {
            assertThat(decoded).isNaN();
        } else {
            assertThat(decoded).isEqualTo(original);
        }
    }

    private static void writeZigzagVarLong(ByteArrayOutputStream out, long value) {
        long zz = (value << 1) ^ (value >> 63);
        while ((zz & ~0x7fL) != 0L) {
            out.write((int) ((zz & 0x7f) | 0x80));
            zz >>>= 7;
        }
        out.write((int) zz);
    }
}
