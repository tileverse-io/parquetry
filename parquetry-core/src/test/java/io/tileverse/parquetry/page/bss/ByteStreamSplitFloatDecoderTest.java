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
package io.tileverse.parquetry.page.bss;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.page.PageDecoder;

class ByteStreamSplitFloatDecoderTest {

    @Test
    void roundTripThreeFloats() {
        float[] values = {1.5f, -2.25f, 3.14159f};
        byte[] encoded = encode(values);

        PageDecoder<Float> decoder = new ByteStreamSplitFloatDecoder();
        decoder.load(ByteBuffer.wrap(encoded), values.length);
        for (int i = 0; i < values.length; i++) {
            assertThat(decoder.next()).as("value " + i).isEqualTo(values[i]);
        }
    }

    @Test
    void skipAdvances() {
        float[] values = {1f, 2f, 3f, 4f, 5f};
        byte[] encoded = encode(values);

        PageDecoder<Float> decoder = new ByteStreamSplitFloatDecoder();
        decoder.load(ByteBuffer.wrap(encoded), values.length);
        decoder.skip(3);
        assertThat(decoder.next()).isEqualTo(4f);
        assertThat(decoder.next()).isEqualTo(5f);
    }

    static byte[] encode(float[] values) {
        int n = values.length;
        byte[] out = new byte[n * 4];
        for (int i = 0; i < n; i++) {
            int bits = Float.floatToRawIntBits(values[i]);
            for (int s = 0; s < 4; s++) {
                out[s * n + i] = (byte) ((bits >>> (s * 8)) & 0xff);
            }
        }
        return out;
    }
}
