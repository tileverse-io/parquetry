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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.read.page.DeltaBinaryPackedInt32Decoder;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.testsupport.ByteArrayWritableChannel;

class DeltaBinaryPackedInt32EncoderTest {

    @Test
    void encodingMarkerIsDeltaBinaryPacked() {
        assertThat(new DeltaBinaryPackedInt32Encoder().parquetEncoding()).isEqualTo(Encoding.DELTA_BINARY_PACKED);
    }

    static Stream<Arguments> roundTripCases() {
        return Stream.of(
                Arguments.of("empty", new int[0]),
                Arguments.of("single", new int[] {42}),
                Arguments.of("exactlyBlockSize", arithmetic(128, 0, 7)),
                Arguments.of("blockSizePlusOne", arithmetic(129, 100, -3)),
                Arguments.of("subBlock", arithmetic(7, 1000, 5)),
                Arguments.of("multiBlock", arithmetic(513, -100_000, 17)),
                Arguments.of("allSame", filled(64, 42)),
                Arguments.of("zigzag", new int[] {0, -1, 2, -3, 4, -5, 6, -7}),
                Arguments.of(
                        "boundaryValues",
                        new int[] {Integer.MIN_VALUE, Integer.MAX_VALUE, 0, -1, 1, Integer.MIN_VALUE + 1}));
    }

    private static int[] arithmetic(int n, int start, int step) {
        int[] values = new int[n];
        long current = start;
        for (int i = 0; i < n; i++) {
            values[i] = (int) current;
            current += step;
        }
        return values;
    }

    private static int[] filled(int n, int value) {
        int[] values = new int[n];
        for (int i = 0; i < n; i++) {
            values[i] = value;
        }
        return values;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roundTripCases")
    void roundTripViaDecoder(String label, int[] values) throws Exception {
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        new DeltaBinaryPackedInt32Encoder().encode(values, values.length, out);

        DeltaBinaryPackedInt32Decoder decoder = new DeltaBinaryPackedInt32Decoder();
        decoder.load(MemorySegment.ofArray(out.toByteArray()), values.length);

        int[] decoded = new int[values.length];
        decoder.decodeInts(values.length, decoded, 0);
        assertThat(decoded).as(label).containsExactly(values);
    }
}
