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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.internal.read.page.PlainFixedLenBinaryDecoder;

class PlainFixedLenBinaryEncoderTest {

    @Test
    void encodingMarkerIsPlain() {
        assertThat(new PlainFixedLenBinaryEncoder(4).parquetEncoding()).isEqualTo(Encoding.PLAIN);
    }

    @Test
    void rejectsNegativeLength() {
        assertThatThrownBy(() -> new PlainFixedLenBinaryEncoder(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-negative");
    }

    @Test
    void rejectsMismatchedLength() {
        PlainFixedLenBinaryEncoder encoder = new PlainFixedLenBinaryEncoder(3);
        GrowableByteSink out = new GrowableByteSink(64);
        byte[][] values = {{1, 2, 3, 4}};
        assertThatThrownBy(() -> encoder.encode(values, 1, out))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("length 4");
    }

    static Stream<Arguments> roundTripCases() {
        return Stream.of(
                Arguments.of("singleValueWidth4", 4, new byte[][] {{1, 2, 3, 4}}),
                Arguments.of("widthZero", 0, new byte[][] {new byte[0], new byte[0]}),
                Arguments.of("width12LikeUuid", 12, new byte[][] {repeat(12, 0x55), repeat(12, 0xaa)}),
                Arguments.of("wideSetOfWidth16", 16, new byte[][] {
                    repeat(16, 0), repeat(16, 1), repeat(16, 0xff), repeat(16, 0x5a)
                }));
    }

    private static byte[] repeat(int n, int byteValue) {
        byte[] result = new byte[n];
        for (int i = 0; i < n; i++) {
            result[i] = (byte) byteValue;
        }
        return result;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roundTripCases")
    void roundTripViaDecoder(String label, int width, byte[][] values) throws Exception {
        GrowableByteSink out = new GrowableByteSink(64);
        new PlainFixedLenBinaryEncoder(width).encode(values, values.length, out);

        PlainFixedLenBinaryDecoder decoder = new PlainFixedLenBinaryDecoder(width);
        decoder.load(MemorySegment.ofArray(out.toByteArray()), values.length);

        for (int i = 0; i < values.length; i++) {
            MemorySegment slice = decoder.next();
            assertThat(slice.toArray(JAVA_BYTE)).as("%s [%d]", label, i).isEqualTo(values[i]);
        }
    }
}
