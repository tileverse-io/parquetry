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
package io.tileverse.parquetry.geotools.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.math.BigInteger;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.LogicalType.TimeUnit;
import io.tileverse.parquetry.schema.PrimitiveKind;

class ScalarAttributeValuesTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("integerDecimals")
    void reconstructsAnIntegerBackedDecimal(String name, long unscaled, int scale, String expected) {
        assertThat(ScalarAttributeValues.decimal(unscaled, scale)).isEqualByComparingTo(expected);
        assertThat(ScalarAttributeValues.decimal(unscaled, scale).scale()).isEqualTo(scale);
    }

    private static Stream<Arguments> integerDecimals() {
        return Stream.of(
                Arguments.of("positive", 12345L, 2, "123.45"),
                Arguments.of("negative", -625L, 2, "-6.25"),
                Arguments.of("zero", 0L, 3, "0.000"),
                Arguments.of("scale zero", 42L, 0, "42"),
                Arguments.of("eighteen digits", 123456789012345678L, 3, "123456789012345.678"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("fixedDecimals")
    void reconstructsAFixedLengthDecimal(String name, byte[] bigEndianTwosComplement, int scale, String expected) {
        MemorySegment bytes = MemorySegment.ofArray(bigEndianTwosComplement).asReadOnly();
        assertThat(ScalarAttributeValues.decimal(bytes, scale)).isEqualByComparingTo(expected);
        assertThat(ScalarAttributeValues.decimal(bytes, scale).scale()).isEqualTo(scale);
    }

    private static Stream<Arguments> fixedDecimals() {
        return Stream.of(
                Arguments.of("positive in 4 bytes", new byte[] {0, 0, 0x30, 0x39}, 2, "123.45"),
                Arguments.of(
                        "negative in 4 bytes",
                        new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xfd, (byte) 0x8f},
                        2,
                        "-6.25"),
                Arguments.of("zero in 9 bytes", new byte[9], 3, "0.000"),
                Arguments.of(
                        "twenty digits in 9 bytes",
                        bigEndian(new BigInteger("123456789012345678901"), 9),
                        3,
                        "123456789012345678.901"));
    }

    /** The two's-complement big-endian encoding of {@code value}, left-padded with its sign byte to {@code width}. */
    private static byte[] bigEndian(BigInteger value, int width) {
        byte[] minimal = value.toByteArray();
        if (minimal.length > width) {
            throw new IllegalArgumentException(value + " does not fit in " + width + " bytes");
        }
        byte[] padded = new byte[width];
        byte signByte = (byte) (value.signum() < 0 ? 0xff : 0x00);
        Arrays.fill(padded, 0, width - minimal.length, signByte);
        System.arraycopy(minimal, 0, padded, width - minimal.length, minimal.length);
        return padded;
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("times")
    void reconstructsATime(String name, long value, PrimitiveKind kind, TimeUnit unit, LocalTime expected) {
        assertThat(ScalarAttributeValues.time(value, kind, unit)).isEqualTo(expected);
    }

    private static Stream<Arguments> times() {
        LocalTime t = LocalTime.of(1, 30, 15, 250_000_000);
        return Stream.of(
                Arguments.of("INT64 micros", 5_415_250_000L, PrimitiveKind.INT64, TimeUnit.MICROS, t),
                Arguments.of("INT64 nanos", 5_415_250_000_000L, PrimitiveKind.INT64, TimeUnit.NANOS, t),
                Arguments.of("INT32 millis", 5_415_250L, PrimitiveKind.INT32, TimeUnit.MILLIS, t),
                Arguments.of(
                        "INT32 annotated micros still reads millis",
                        5_415_250L,
                        PrimitiveKind.INT32,
                        TimeUnit.MICROS,
                        t),
                Arguments.of("midnight", 0L, PrimitiveKind.INT64, TimeUnit.MICROS, LocalTime.MIDNIGHT),
                Arguments.of(
                        "last micro of the day",
                        86_399_999_999L,
                        PrimitiveKind.INT64,
                        TimeUnit.MICROS,
                        LocalTime.of(23, 59, 59, 999_999_000)));
    }
}
