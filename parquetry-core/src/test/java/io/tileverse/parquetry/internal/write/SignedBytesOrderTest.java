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
import java.math.BigInteger;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SignedBytesOrderTest {

    private static final int FIXED_LENGTH = 12;
    private static final int PADDED_LENGTH = 11;

    private static final List<Long> NUMBERS = List.of(
            Long.MIN_VALUE,
            -65_537L,
            -32_769L,
            -32_768L,
            -129L,
            -128L,
            -1L,
            0L,
            1L,
            127L,
            128L,
            255L,
            256L,
            32_767L,
            32_768L,
            65_536L,
            Long.MAX_VALUE);

    @Test
    void minimalEncodingsCompareAsTheirNumbers() {
        for (long a : NUMBERS) {
            for (long b : NUMBERS) {
                int compared = SignedBytesOrder.compare(minimal(a), minimal(b));

                assertThat(Integer.signum(compared)).as("%d against %d", a, b).isEqualTo(Long.compare(a, b));
            }
        }
    }

    @Test
    void encodingsOfOneFixedLengthCompareAsTheirNumbers() {
        for (long a : NUMBERS) {
            for (long b : NUMBERS) {
                int compared = SignedBytesOrder.compare(padded(a, FIXED_LENGTH), padded(b, FIXED_LENGTH));

                assertThat(Integer.signum(compared)).as("%d against %d", a, b).isEqualTo(Long.compare(a, b));
            }
        }
    }

    @Test
    void aPaddedEncodingEqualsItsMinimalOne() {
        for (long number : NUMBERS) {
            assertThat(SignedBytesOrder.compare(minimal(number), padded(number, PADDED_LENGTH)))
                    .as("%d", number)
                    .isZero();
            assertThat(SignedBytesOrder.compare(padded(number, PADDED_LENGTH), minimal(number)))
                    .as("%d", number)
                    .isZero();
        }
    }

    /** Pairs of numbers, each with the count of sign bytes added on the left of its minimal encoding. */
    static Stream<Arguments> mixedLengthPairs() {
        return Stream.of(
                Arguments.of("1", 0, "256", 0),
                Arguments.of("255", 0, "256", 2),
                Arguments.of("127", 3, "128", 0),
                Arguments.of("-128", 0, "-129", 0),
                Arguments.of("-1", 3, "-256", 0),
                Arguments.of("-1", 0, "0", 2),
                Arguments.of("0", 3, "0", 0),
                Arguments.of("-32768", 1, "32767", 0),
                Arguments.of("99999999999999999999", 0, "-99999999999999999999", 2),
                Arguments.of("99999999999999999999", 2, "100000000000000000000", 0),
                Arguments.of("-99999999999999999999", 0, "-100000000000000000000", 3),
                Arguments.of("604462909807314587353087", 0, "604462909807314587353088", 0),
                Arguments.of("-604462909807314587353088", 1, "-604462909807314587353089", 0),
                Arguments.of("7", 0, "1208925819614629174706175", 1));
    }

    @ParameterizedTest(name = "{0} extended by {1} bytes against {2} extended by {3} bytes")
    @MethodSource("mixedLengthPairs")
    void encodingsOfMixedLengthsCompareAsTheirNumbers(String a, int aSignBytes, String b, int bSignBytes) {
        BigInteger left = new BigInteger(a);
        BigInteger right = new BigInteger(b);
        MemorySegment leftBytes = padded(left, left.toByteArray().length + aSignBytes);
        MemorySegment rightBytes = padded(right, right.toByteArray().length + bSignBytes);

        assertThat(Integer.signum(SignedBytesOrder.compare(leftBytes, rightBytes)))
                .isEqualTo(left.compareTo(right));
        assertThat(Integer.signum(SignedBytesOrder.compare(rightBytes, leftBytes)))
                .isEqualTo(right.compareTo(left));
    }

    @Test
    void anEmptyValueHoldsZero() {
        MemorySegment empty = MemorySegment.ofArray(new byte[0]);

        assertThat(SignedBytesOrder.compare(empty, minimal(0L))).isZero();
        assertThat(SignedBytesOrder.compare(empty, minimal(-1L))).isPositive();
        assertThat(SignedBytesOrder.compare(empty, minimal(1L))).isNegative();
    }

    private static MemorySegment minimal(long number) {
        return MemorySegment.ofArray(BigInteger.valueOf(number).toByteArray());
    }

    private static MemorySegment padded(long number, int length) {
        return padded(BigInteger.valueOf(number), length);
    }

    private static MemorySegment padded(BigInteger number, int length) {
        return MemorySegment.ofArray(WriteFixtures.signedBytes(number, length));
    }
}
