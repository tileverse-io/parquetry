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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.internal.read.page.DeltaLengthByteArrayDecoder;
import io.tileverse.parquetry.testsupport.ByteArrayWritableChannel;

class DeltaLengthByteArrayEncoderTest {

    @Test
    void encodingMarkerIsDeltaLengthByteArray() {
        assertThat(new DeltaLengthByteArrayEncoder().parquetEncoding()).isEqualTo(Encoding.DELTA_LENGTH_BYTE_ARRAY);
    }

    static Stream<Arguments> roundTripCases() {
        return Stream.of(
                Arguments.of("empty", new byte[0][]),
                Arguments.of("single", new byte[][] {bytes("hello")}),
                Arguments.of(
                        "fewMixedSizes",
                        new byte[][] {bytes(""), bytes("a"), bytes("hi"), bytes("hello"), bytes("parquetry")}),
                Arguments.of("allSame", new byte[][] {bytes("dup"), bytes("dup"), bytes("dup"), bytes("dup")}),
                Arguments.of("over128Values", repeating(150, "value")),
                Arguments.of("varyingLengths", varyingLengths()));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[][] repeating(int n, String value) {
        byte[][] result = new byte[n][];
        for (int i = 0; i < n; i++) {
            result[i] = bytes(value + i);
        }
        return result;
    }

    private static byte[][] varyingLengths() {
        byte[][] result = new byte[10][];
        for (int i = 0; i < result.length; i++) {
            byte[] value = new byte[i];
            for (int j = 0; j < i; j++) {
                value[j] = (byte) ('a' + j);
            }
            result[i] = value;
        }
        return result;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roundTripCases")
    void roundTripViaDecoder(String label, byte[][] values) throws Exception {
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        new DeltaLengthByteArrayEncoder().encode(values, values.length, out);

        DeltaLengthByteArrayDecoder decoder = new DeltaLengthByteArrayDecoder();
        decoder.load(MemorySegment.ofArray(out.toByteArray()), values.length);

        for (int i = 0; i < values.length; i++) {
            MemorySegment slice = decoder.next();
            assertThat(slice.toArray(JAVA_BYTE)).as("%s [%d]", label, i).isEqualTo(values[i]);
        }
    }
}
