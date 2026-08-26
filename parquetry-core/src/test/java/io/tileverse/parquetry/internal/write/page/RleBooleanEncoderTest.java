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

import java.lang.foreign.MemorySegment;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.internal.read.page.RleBooleanDecoder;

class RleBooleanEncoderTest {

    @Test
    void encodingMarkerIsRle() {
        assertThat(new RleBooleanEncoder().parquetEncoding()).isEqualTo(Encoding.RLE);
    }

    static Stream<Arguments> roundTripCases() {
        return Stream.of(
                Arguments.of("empty", new boolean[0]),
                Arguments.of("singleTrue", new boolean[] {true}),
                Arguments.of("singleFalse", new boolean[] {false}),
                Arguments.of("allTrueLongRun", filled(64, true)),
                Arguments.of("allFalseLongRun", filled(64, false)),
                Arguments.of("alternating8", alternating(8)),
                Arguments.of("alternating9", alternating(9)),
                Arguments.of("mixedRunsAndBitPacked", mixed()),
                Arguments.of("crossesGroupBoundaries", alternating(100)));
    }

    private static boolean[] alternating(int n) {
        boolean[] values = new boolean[n];
        for (int i = 0; i < n; i++) {
            values[i] = (i & 1) == 1;
        }
        return values;
    }

    private static boolean[] filled(int n, boolean v) {
        boolean[] values = new boolean[n];
        for (int i = 0; i < n; i++) {
            values[i] = v;
        }
        return values;
    }

    private static boolean[] mixed() {
        boolean[] values = new boolean[20];
        // 8 true, 8 false (long enough to trigger RLE), 4 alternating
        for (int i = 0; i < 8; i++) {
            values[i] = true;
        }
        for (int i = 16; i < 20; i++) {
            values[i] = (i & 1) == 1;
        }
        return values;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roundTripCases")
    void roundTripViaDecoder(String label, boolean[] values) throws Exception {
        GrowableByteSink out = new GrowableByteSink(64);
        new RleBooleanEncoder().encode(values, values.length, out);

        RleBooleanDecoder decoder = new RleBooleanDecoder();
        decoder.load(MemorySegment.ofArray(out.toByteArray()), values.length);

        boolean[] decoded = new boolean[values.length];
        decoder.decodeBooleans(values.length, decoded, 0);
        assertThat(decoded).as(label).containsExactly(values);
    }
}
