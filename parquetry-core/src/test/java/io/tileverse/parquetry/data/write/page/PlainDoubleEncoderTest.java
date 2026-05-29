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
package io.tileverse.parquetry.data.write.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.read.page.PlainDoubleDecoder;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.testsupport.ByteArrayWritableChannel;

class PlainDoubleEncoderTest {

    @Test
    void encodingMarkerIsPlain() {
        assertThat(new PlainDoubleEncoder().parquetEncoding()).isEqualTo(Encoding.PLAIN);
    }

    @Test
    void emptyInputWritesNoBytes() throws Exception {
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        int written = new PlainDoubleEncoder().encode(new double[0], 0, out);
        assertThat(written).isZero();
    }

    static Stream<Arguments> roundTripCases() {
        return Stream.of(
                Arguments.of("zero", new double[] {0.0}),
                Arguments.of("negativeZero", new double[] {-0.0}),
                Arguments.of("specialValues", new double[] {
                    Double.NaN,
                    Double.POSITIVE_INFINITY,
                    Double.NEGATIVE_INFINITY,
                    Double.MAX_VALUE,
                    Double.MIN_VALUE,
                    Double.MIN_NORMAL
                }),
                Arguments.of("everyday", new double[] {1.0, -1.0, Math.PI, -Math.E, 1e300, -1e300}),
                Arguments.of("allSame", new double[] {0.5, 0.5, 0.5}));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roundTripCases")
    void roundTripViaDecoder(String label, double[] values) throws Exception {
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        new PlainDoubleEncoder().encode(values, values.length, out);

        PlainDoubleDecoder decoder = new PlainDoubleDecoder();
        decoder.load(MemorySegment.ofArray(out.toByteArray()), values.length);

        double[] decoded = new double[values.length];
        decoder.decodeDoubles(values.length, decoded, 0);
        for (int i = 0; i < values.length; i++) {
            assertThat(Double.doubleToRawLongBits(decoded[i]))
                    .as("%s [%d]", label, i)
                    .isEqualTo(Double.doubleToRawLongBits(values[i]));
        }
    }
}
