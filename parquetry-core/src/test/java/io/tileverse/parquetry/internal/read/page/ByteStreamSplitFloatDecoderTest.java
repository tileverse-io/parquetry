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
package io.tileverse.parquetry.internal.read.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.ParquetLayouts;

class ByteStreamSplitFloatDecoderTest {

    @Test
    void roundTripThreeFloats() {
        float[] values = {1.5f, -2.25f, 3.14159f};
        byte[] encoded = encode(values);

        PageDecoder<Float> decoder = new ByteStreamSplitFloatDecoder();
        decoder.load(MemorySegment.ofArray(encoded), values.length);
        for (int i = 0; i < values.length; i++) {
            assertThat(decoder.next()).as("value " + i).isEqualTo(values[i]);
        }
    }

    @Test
    void skipAdvances() {
        float[] values = {1f, 2f, 3f, 4f, 5f};
        byte[] encoded = encode(values);

        PageDecoder<Float> decoder = new ByteStreamSplitFloatDecoder();
        decoder.load(MemorySegment.ofArray(encoded), values.length);
        decoder.skip(3);
        assertThat(decoder.next()).isEqualTo(4f);
        assertThat(decoder.next()).isEqualTo(5f);
    }

    @Test
    void bulkDecodeFloatsFillsArray() {
        float[] values = {1.5f, -2.25f, 3.14159f, 0f};
        byte[] encoded = encode(values);

        PageDecoder<Float> decoder = new ByteStreamSplitFloatDecoder();
        decoder.load(MemorySegment.ofArray(encoded), values.length);

        float[] dst = new float[values.length];
        decoder.decodeFloats(values.length, dst, 0);

        assertThat(dst).containsExactly(values);
    }

    @Test
    void bulkDecodeFloatsIntoSegment() {
        float[] values = {1.5f, -2.25f, 3.14159f, 0f};
        byte[] encoded = encode(values);

        PageDecoder<Float> decoder = new ByteStreamSplitFloatDecoder();
        decoder.load(MemorySegment.ofArray(encoded), values.length);

        MemorySegment dst = MemorySegment.ofArray(new byte[values.length * Float.BYTES]);
        dst.setAtIndex(ParquetLayouts.FLOAT, 0, -1f); // sentinel - must be left alone
        decoder.decodeFloats(3, dst, 1);

        assertThat(dst.getAtIndex(ParquetLayouts.FLOAT, 0)).isEqualTo(-1f);
        assertThat(dst.getAtIndex(ParquetLayouts.FLOAT, 1)).isEqualTo(1.5f);
        assertThat(dst.getAtIndex(ParquetLayouts.FLOAT, 2)).isEqualTo(-2.25f);
        assertThat(dst.getAtIndex(ParquetLayouts.FLOAT, 3)).isEqualTo(3.14159f);
        assertThat(decoder.next()).isEqualTo(0f);
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
