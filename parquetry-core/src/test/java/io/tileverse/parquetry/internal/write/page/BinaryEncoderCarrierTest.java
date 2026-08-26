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
package io.tileverse.parquetry.internal.write.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

/**
 * Pins the three PLAIN binary encoders to the exact bytes they must emit when the page's values arrive through a
 * {@link BinaryPayload} instead of a bare {@code byte[][]}. An {@link ArrayBinaryPayload} wrapping the same values must
 * produce the identical byte stream the encoders wrote before the carrier existed, which keeps the rewiring
 * byte-for-byte transparent.
 */
class BinaryEncoderCarrierTest {

    @Test
    void plainBinaryEmitsLengthPrefixedValuesFromCarrier() throws Exception {
        byte[][] values = {bytes(""), bytes("a"), bytes("hello"), bytes("world!")};

        byte[] encoded = encode(new PlainBinaryEncoder(), values);

        assertThat(encoded).isEqualTo(lengthPrefixedOracle(values));
    }

    @Test
    void plainFixedLenEmitsContiguousValuesFromCarrier() throws Exception {
        int width = 4;
        byte[][] values = {{1, 2, 3, 4}, {10, 20, 30, 40}, {-1, -2, -3, -4}};

        byte[] encoded = encode(new PlainFixedLenBinaryEncoder(width), values);

        assertThat(encoded).isEqualTo(contiguousOracle(values));
    }

    @Test
    void plainInt96EmitsContiguousTwelveByteValuesFromCarrier() throws Exception {
        byte[][] values = {repeat12(0x00), repeat12(0x55), repeat12(0xaa)};

        byte[] encoded = encode(new PlainInt96Encoder(), values);

        assertThat(encoded).isEqualTo(contiguousOracle(values));
    }

    private static byte[] encode(Encoder<BinaryPayload> encoder, byte[][] values) throws Exception {
        GrowableByteSink out = new GrowableByteSink(64);
        encoder.encode(new ArrayBinaryPayload(values, values.length), values.length, out);
        return out.toByteArray();
    }

    private static byte[] lengthPrefixedOracle(byte[][] values) {
        int total = 0;
        for (byte[] value : values) {
            total += Integer.BYTES + value.length;
        }
        ByteBuffer oracle = ByteBuffer.allocate(total).order(ByteOrder.LITTLE_ENDIAN);
        for (byte[] value : values) {
            oracle.putInt(value.length);
            oracle.put(value);
        }
        return oracle.array();
    }

    private static byte[] contiguousOracle(byte[][] values) {
        int total = 0;
        for (byte[] value : values) {
            total += value.length;
        }
        ByteBuffer oracle = ByteBuffer.allocate(total);
        for (byte[] value : values) {
            oracle.put(value);
        }
        return oracle.array();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] repeat12(int byteValue) {
        byte[] value = new byte[12];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) byteValue;
        }
        return value;
    }
}
