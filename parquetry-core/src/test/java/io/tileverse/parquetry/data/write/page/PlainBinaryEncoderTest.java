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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.read.page.PlainBinaryDecoder;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.testsupport.ByteArrayWritableChannel;

class PlainBinaryEncoderTest {

    @Test
    void encodingMarkerIsPlain() {
        assertThat(new PlainBinaryEncoder().parquetEncoding()).isEqualTo(Encoding.PLAIN);
    }

    @Test
    void emptyInputWritesNoBytes() throws Exception {
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        int written = new PlainBinaryEncoder().encode(new byte[0][], 0, out);
        assertThat(written).isZero();
    }

    static Stream<Arguments> roundTripCases() {
        return Stream.of(
                Arguments.of("singleEmpty", new byte[][] {bytes("")}),
                Arguments.of("singleSmall", new byte[][] {bytes("hi")}),
                Arguments.of("mixedSizes", new byte[][] {bytes(""), bytes("a"), bytes("hello"), bytes("world!")}),
                Arguments.of("allSame", new byte[][] {bytes("same"), bytes("same"), bytes("same")}),
                Arguments.of("largeValue", new byte[][] {largeValue(1024)}));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] largeValue(int length) {
        byte[] value = new byte[length];
        for (int i = 0; i < length; i++) {
            value[i] = (byte) (i & 0xff);
        }
        return value;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("roundTripCases")
    void roundTripViaDecoder(String label, byte[][] values) throws Exception {
        ByteArrayWritableChannel out = new ByteArrayWritableChannel();
        new PlainBinaryEncoder().encode(values, values.length, out);

        PlainBinaryDecoder decoder = new PlainBinaryDecoder();
        decoder.load(MemorySegment.ofArray(out.toByteArray()), values.length);

        for (int i = 0; i < values.length; i++) {
            MemorySegment slice = decoder.next();
            byte[] decoded = slice.toArray(JAVA_BYTE);
            assertThat(decoded).as("%s [%d]", label, i).isEqualTo(values[i]);
        }
    }
}
