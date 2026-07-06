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
package io.tileverse.parquetry.internal.write.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.internal.read.page.PlainFloatDecoder;
import io.tileverse.parquetry.testsupport.ByteArrayWritableChannel;

class PlainFloatEncoderTest {

    @Test
    void encodingMarkerIsPlain() {
        assertThat(new PlainFloatEncoder().parquetEncoding()).isEqualTo(Encoding.PLAIN);
    }

    @Test
    void emptyInputWritesNoBytes() throws Exception {
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        int written = new PlainFloatEncoder().encode(new float[0], 0, out);
        assertThat(written).isZero();
    }

    static Stream<Arguments> roundTripCases() {
        return Stream.of(
                Arguments.of("zero", new float[] {0.0f}),
                Arguments.of("negativeZero", new float[] {-0.0f}),
                Arguments.of("specialValues", new float[] {
                    Float.NaN,
                    Float.POSITIVE_INFINITY,
                    Float.NEGATIVE_INFINITY,
                    Float.MAX_VALUE,
                    Float.MIN_VALUE,
                    Float.MIN_NORMAL
                }),
                Arguments.of("everyday", new float[] {1.0f, -1.0f, 3.14f, -2.71f, 1e30f, -1e30f}),
                Arguments.of("allSame", new float[] {0.5f, 0.5f, 0.5f}));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roundTripCases")
    void roundTripViaDecoder(String label, float[] values) throws Exception {
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        new PlainFloatEncoder().encode(values, values.length, out);

        PlainFloatDecoder decoder = new PlainFloatDecoder();
        decoder.load(MemorySegment.ofArray(out.toByteArray()), values.length);

        float[] decoded = new float[values.length];
        decoder.decodeFloats(values.length, decoded, 0);
        // Use bit-pattern equality so NaN and signed zero round-trip cleanly.
        for (int i = 0; i < values.length; i++) {
            assertThat(Float.floatToRawIntBits(decoded[i]))
                    .as("%s [%d]", label, i)
                    .isEqualTo(Float.floatToRawIntBits(values[i]));
        }
    }
}
