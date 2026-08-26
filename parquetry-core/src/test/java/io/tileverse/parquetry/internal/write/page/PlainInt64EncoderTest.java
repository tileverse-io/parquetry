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
import io.tileverse.parquetry.internal.read.page.PlainInt64Decoder;

class PlainInt64EncoderTest {

    @Test
    void encodingMarkerIsPlain() {
        assertThat(new PlainInt64Encoder().parquetEncoding()).isEqualTo(Encoding.PLAIN);
    }

    @Test
    void emptyInputWritesNoBytes() throws Exception {
        GrowableByteSink out = new GrowableByteSink(64);
        int written = new PlainInt64Encoder().encode(new long[0], 0, out);
        assertThat(written).isZero();
    }

    static Stream<Arguments> roundTripCases() {
        return Stream.of(
                Arguments.of("singleZero", new long[] {0L}),
                Arguments.of("singleNegativeOne", new long[] {-1L}),
                Arguments.of("minMax", new long[] {Long.MIN_VALUE, Long.MAX_VALUE}),
                Arguments.of(
                        "varintBoundaries",
                        new long[] {127L, 128L, -128L, -129L, 16383L, 16384L, Integer.MAX_VALUE + 1L}),
                Arguments.of("ascending", new long[] {0L, 1L, 2L, 3L, 4L, 5L}),
                Arguments.of("allSame", new long[] {42L, 42L, 42L, 42L}));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roundTripCases")
    void roundTripViaDecoder(String label, long[] values) throws Exception {
        GrowableByteSink out = new GrowableByteSink(64);
        new PlainInt64Encoder().encode(values, values.length, out);

        PlainInt64Decoder decoder = new PlainInt64Decoder();
        decoder.load(MemorySegment.ofArray(out.toByteArray()), values.length);

        long[] decoded = new long[values.length];
        decoder.decodeLongs(values.length, decoded, 0);
        assertThat(decoded).as(label).containsExactly(values);
    }
}
