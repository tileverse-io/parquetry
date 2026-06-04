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
package io.tileverse.parquetry.internal.write.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.internal.read.page.PlainInt32Decoder;
import io.tileverse.parquetry.testsupport.ByteArrayWritableChannel;

class PlainInt32EncoderTest {

    @Test
    void encodingMarkerIsPlain() {
        assertThat(new PlainInt32Encoder().parquetEncoding()).isEqualTo(Encoding.PLAIN);
        assertThat(new PlainInt32Encoder().parquetEncodingV1()).isEqualTo(Encoding.PLAIN);
    }

    @Test
    void emptyInputWritesNoBytes() throws Exception {
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        int written = new PlainInt32Encoder().encode(new int[0], 0, out);
        assertThat(written).isZero();
        assertThat(out.size()).isZero();
    }

    @Test
    void bytesPerValueIsFour() throws Exception {
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        int written = new PlainInt32Encoder().encode(new int[] {1, 2, 3}, 3, out);
        assertThat(written).isEqualTo(12);
        assertThat(out.size()).isEqualTo(12);
    }

    static Stream<Arguments> roundTripCases() {
        return Stream.of(
                Arguments.of("singleZero", new int[] {0}),
                Arguments.of("singleNegativeOne", new int[] {-1}),
                Arguments.of("minMax", new int[] {Integer.MIN_VALUE, Integer.MAX_VALUE}),
                Arguments.of("varintBoundaries", new int[] {127, 128, -128, -129, 16383, 16384, -16384, -16385}),
                Arguments.of("ascending", new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
                Arguments.of("allSame", new int[] {42, 42, 42, 42}),
                Arguments.of("largeBatch", largeBatch()));
    }

    private static int[] largeBatch() {
        int[] values = new int[1024];
        for (int i = 0; i < values.length; i++) {
            values[i] = (i * 7) - 512;
        }
        return values;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roundTripCases")
    void roundTripViaDecoder(String label, int[] values) throws Exception {
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        new PlainInt32Encoder().encode(values, values.length, out);

        PlainInt32Decoder decoder = new PlainInt32Decoder();
        decoder.load(MemorySegment.ofArray(out.toByteArray()), values.length);

        int[] decoded = new int[values.length];
        decoder.decodeInts(values.length, decoded, 0);
        assertThat(decoded).as(label).containsExactly(values);
    }
}
