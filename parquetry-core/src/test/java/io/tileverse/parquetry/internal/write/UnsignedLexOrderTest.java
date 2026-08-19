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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class UnsignedLexOrderTest {

    static Stream<Arguments> orderings() {
        return Stream.of(
                Arguments.of("equal", bytes("abc"), bytes("abc"), 0),
                Arguments.of("empty vs empty", new byte[0], new byte[0], 0),
                Arguments.of("empty orders first", new byte[0], bytes("a"), -1),
                Arguments.of("shorter prefix orders first", bytes("a"), bytes("ab"), -1),
                Arguments.of("longer orders last", bytes("abc"), bytes("ab"), 1),
                Arguments.of("unsigned high byte orders last", new byte[] {0x7f}, new byte[] {(byte) 0x80}, -1),
                Arguments.of("differing middle byte", bytes("aZc"), bytes("abc"), -1));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("orderings")
    void comparesUnsignedLexicographically(String name, byte[] a, byte[] b, int expectedSign) {
        int forward = UnsignedLexOrder.compare(MemorySegment.ofArray(a), MemorySegment.ofArray(b));
        int backward = UnsignedLexOrder.compare(MemorySegment.ofArray(b), MemorySegment.ofArray(a));
        assertThat(Integer.signum(forward)).isEqualTo(expectedSign);
        assertThat(Integer.signum(backward)).isEqualTo(-expectedSign);
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }
}
