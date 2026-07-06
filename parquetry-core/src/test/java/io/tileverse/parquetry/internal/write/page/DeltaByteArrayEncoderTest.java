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
import io.tileverse.parquetry.internal.read.page.DeltaByteArrayDecoder;
import io.tileverse.parquetry.testsupport.ByteArrayWritableChannel;

class DeltaByteArrayEncoderTest {

    @Test
    void encodingMarkerIsDeltaByteArray() {
        assertThat(new DeltaByteArrayEncoder().parquetEncoding()).isEqualTo(Encoding.DELTA_BYTE_ARRAY);
    }

    static Stream<Arguments> roundTripCases() {
        return Stream.of(
                Arguments.of("empty", new byte[0][]),
                Arguments.of("singleValueHasZeroPrefix", new byte[][] {bytes("hello")}),
                Arguments.of("prefixShared", new byte[][] {bytes("car"), bytes("carpet"), bytes("carry")}),
                Arguments.of(
                        "deeplyNestedSuffix",
                        new byte[][] {bytes("a"), bytes("aa"), bytes("aaa"), bytes("aaaa"), bytes("aaaaa")}),
                Arguments.of(
                        "noPrefixOverlap",
                        new byte[][] {bytes("alpha"), bytes("bravo"), bytes("charlie"), bytes("delta"), bytes("echo")}),
                Arguments.of("allEmpty", new byte[][] {bytes(""), bytes(""), bytes("")}),
                Arguments.of("over128Values", sequential(150)));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[][] sequential(int n) {
        byte[][] result = new byte[n][];
        for (int i = 0; i < n; i++) {
            // Reuse the previous prefix so we exercise the prefix-shared path under block-boundary conditions.
            result[i] = bytes("prefix_" + String.format("%04d", i));
        }
        return result;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roundTripCases")
    void roundTripViaDecoder(String label, byte[][] values) throws Exception {
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        new DeltaByteArrayEncoder().encode(values, values.length, out);

        DeltaByteArrayDecoder decoder = new DeltaByteArrayDecoder();
        decoder.load(MemorySegment.ofArray(out.toByteArray()), values.length);

        for (int i = 0; i < values.length; i++) {
            MemorySegment slice = decoder.next();
            assertThat(slice.toArray(JAVA_BYTE)).as("%s [%d]", label, i).isEqualTo(values[i]);
        }
    }
}
