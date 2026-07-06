/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies the round-trip behavior of zigzag varint and little-endian double encoding against an explicit set of
 * boundary vectors (signed extremes, sign-flip neighbors, IEEE 754 special values).
 */
class CompactProtocolReaderRoundTripTest {

    @ParameterizedTest
    @ValueSource(
            ints = {
                0,
                1,
                -1,
                2,
                -2,
                63,
                -64,
                64,
                -65,
                127,
                -128,
                16_383,
                -16_384,
                Integer.MAX_VALUE - 1,
                Integer.MAX_VALUE,
                Integer.MIN_VALUE + 1,
                Integer.MIN_VALUE
            })
    void zigzagI32RoundTrip(int original) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        writeZigzagVarLong(bytes, original);
        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(bytes.toByteArray()));
        assertThat(r.readI32()).isEqualTo(original);
    }

    @ParameterizedTest
    @ValueSource(
            longs = {
                0L,
                1L,
                -1L,
                2L,
                -2L,
                63L,
                -64L,
                Integer.MAX_VALUE,
                Integer.MIN_VALUE,
                (long) Integer.MAX_VALUE + 1L,
                (long) Integer.MIN_VALUE - 1L,
                Long.MAX_VALUE - 1L,
                Long.MAX_VALUE,
                Long.MIN_VALUE + 1L,
                Long.MIN_VALUE
            })
    void zigzagI64RoundTrip(long original) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        writeZigzagVarLong(bytes, original);
        CompactProtocolReader r = new CompactProtocolReader(new ByteArrayInputStream(bytes.toByteArray()));
        assertThat(r.readI64()).isEqualTo(original);
    }

    @ParameterizedTest
    @ValueSource(
            doubles = {
                0.0,
                -0.0,
                1.0,
                -1.0,
                Math.PI,
                -Math.PI,
                Double.MIN_VALUE,
                Double.MIN_NORMAL,
                Double.MAX_VALUE,
                -Double.MAX_VALUE,
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY,
                Double.NaN
            })
    void doubleRoundTrip(double original) throws Exception {
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
