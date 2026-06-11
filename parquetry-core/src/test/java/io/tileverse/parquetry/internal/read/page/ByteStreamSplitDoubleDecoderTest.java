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
package io.tileverse.parquetry.internal.read.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.ParquetLayouts;

class ByteStreamSplitDoubleDecoderTest {

    @Test
    void roundTripThreeDoubles() {
        double[] values = {1.5, -2.25, 3.141592653589793};
        byte[] encoded = encode(values);

        PageDecoder<Double> decoder = new ByteStreamSplitDoubleDecoder();
        decoder.load(MemorySegment.ofArray(encoded), values.length);
        for (int i = 0; i < values.length; i++) {
            assertThat(decoder.next()).as("value " + i).isEqualTo(values[i]);
        }
    }

    @Test
    void skipAdvances() {
        double[] values = {1.0, 2.0, 3.0, 4.0, 5.0};
        byte[] encoded = encode(values);

        PageDecoder<Double> decoder = new ByteStreamSplitDoubleDecoder();
        decoder.load(MemorySegment.ofArray(encoded), values.length);
        decoder.skip(3);
        assertThat(decoder.next()).isEqualTo(4.0);
        assertThat(decoder.next()).isEqualTo(5.0);
    }

    @Test
    void bulkDecodeDoublesFillsArray() {
        double[] values = {1.5, -2.25, 3.141592653589793, 0.0};
        byte[] encoded = encode(values);

        PageDecoder<Double> decoder = new ByteStreamSplitDoubleDecoder();
        decoder.load(MemorySegment.ofArray(encoded), values.length);

        double[] dst = new double[values.length];
        decoder.decodeDoubles(values.length, dst, 0);

        assertThat(dst).containsExactly(values);
    }

    @Test
    void bulkDecodeDoublesIntoSegment() {
        double[] values = {1.5, -2.25, 3.141592653589793, 0.0};
        byte[] encoded = encode(values);

        PageDecoder<Double> decoder = new ByteStreamSplitDoubleDecoder();
        decoder.load(MemorySegment.ofArray(encoded), values.length);

        MemorySegment dst = MemorySegment.ofArray(new byte[values.length * Double.BYTES]);
        dst.setAtIndex(ParquetLayouts.DOUBLE, 0, -1.0); // sentinel - must be left alone
        decoder.decodeDoubles(3, dst, 1);

        assertThat(dst.getAtIndex(ParquetLayouts.DOUBLE, 0)).isEqualTo(-1.0);
        assertThat(dst.getAtIndex(ParquetLayouts.DOUBLE, 1)).isEqualTo(1.5);
        assertThat(dst.getAtIndex(ParquetLayouts.DOUBLE, 2)).isEqualTo(-2.25);
        assertThat(dst.getAtIndex(ParquetLayouts.DOUBLE, 3)).isEqualTo(3.141592653589793);
        assertThat(decoder.next()).isEqualTo(0.0);
    }

    static byte[] encode(double[] values) {
        int n = values.length;
        byte[] out = new byte[n * 8];
        for (int i = 0; i < n; i++) {
            long bits = Double.doubleToRawLongBits(values[i]);
            for (int s = 0; s < 8; s++) {
                out[s * n + i] = (byte) ((bits >>> (s * 8)) & 0xffL);
            }
        }
        return out;
    }
}
