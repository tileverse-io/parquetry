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
package io.tileverse.parquetry.data.write.page;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.read.page.PlainInt96Decoder;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.testsupport.ByteArrayWritableChannel;

class PlainInt96EncoderTest {

    @Test
    void encodingMarkerIsPlain() {
        assertThat(new PlainInt96Encoder().parquetEncoding()).isEqualTo(Encoding.PLAIN);
    }

    @Test
    void rejectsNon12ByteValue() {
        PlainInt96Encoder encoder = new PlainInt96Encoder();
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        byte[][] bad = {new byte[11]};
        assertThatThrownBy(() -> encoder.encode(bad, 1, out))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be 12");
    }

    static Stream<Arguments> roundTripCases() {
        return Stream.of(
                Arguments.of("empty", new byte[0][]),
                Arguments.of("singleZero", new byte[][] {new byte[12]}),
                Arguments.of("singleSentinel", new byte[][] {repeat12(0xaa)}),
                Arguments.of("twoDistinct", new byte[][] {
                    repeat12(0x55), repeat12(0xaa),
                }),
                Arguments.of("manyValues", new byte[][] {
                    repeat12(0x00), repeat12(0x01), repeat12(0xff), repeat12(0x80),
                }));
    }

    private static byte[] repeat12(int byteValue) {
        byte[] value = new byte[12];
        for (int i = 0; i < value.length; i++) {
            value[i] = (byte) byteValue;
        }
        return value;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roundTripCases")
    void roundTripViaDecoder(String label, byte[][] values) throws Exception {
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        new PlainInt96Encoder().encode(values, values.length, out);

        PlainInt96Decoder decoder = new PlainInt96Decoder();
        decoder.load(MemorySegment.ofArray(out.toByteArray()), values.length);

        for (int i = 0; i < values.length; i++) {
            MemorySegment slice = decoder.next();
            assertThat(slice.toArray(JAVA_BYTE)).as("%s [%d]", label, i).isEqualTo(values[i]);
        }
    }
}
