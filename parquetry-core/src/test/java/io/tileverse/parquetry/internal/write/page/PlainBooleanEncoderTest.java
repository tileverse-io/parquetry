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
import io.tileverse.parquetry.internal.read.page.PlainBooleanDecoder;
import io.tileverse.parquetry.testsupport.ByteArrayWritableChannel;

class PlainBooleanEncoderTest {

    @Test
    void encodingMarkerIsPlain() {
        assertThat(new PlainBooleanEncoder().parquetEncoding()).isEqualTo(Encoding.PLAIN);
    }

    @Test
    void emptyInputWritesNoBytes() throws Exception {
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        int written = new PlainBooleanEncoder().encode(new boolean[0], 0, out);
        assertThat(written).isZero();
    }

    @Test
    void packsLsbFirst() throws Exception {
        boolean[] values = {true, false, true, false, false, false, false, false};
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        new PlainBooleanEncoder().encode(values, values.length, out);
        // bit0 = 1, bit2 = 1 -> 0b0000_0101 = 0x05
        assertThat(out.toByteArray()).containsExactly(0x05);
    }

    static Stream<Arguments> roundTripCases() {
        return Stream.of(
                Arguments.of("singleTrue", new boolean[] {true}),
                Arguments.of("singleFalse", new boolean[] {false}),
                Arguments.of("exactly8", alternating(8)),
                Arguments.of("oneLessThan8", alternating(7)),
                Arguments.of("oneMoreThan8", alternating(9)),
                Arguments.of("allTrue16", filled(16, true)),
                Arguments.of("allFalse16", filled(16, false)),
                Arguments.of("crossesManyBytes", alternating(100)));
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("roundTripCases")
    void roundTripViaDecoder(String label, boolean[] values) throws Exception {
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        new PlainBooleanEncoder().encode(values, values.length, out);

        PlainBooleanDecoder decoder = new PlainBooleanDecoder();
        decoder.load(MemorySegment.ofArray(out.toByteArray()), values.length);

        boolean[] decoded = new boolean[values.length];
        decoder.decodeBooleans(values.length, decoded, 0);
        assertThat(decoded).as(label).containsExactly(values);
    }
}
