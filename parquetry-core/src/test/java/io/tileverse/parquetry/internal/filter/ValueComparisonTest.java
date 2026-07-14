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
package io.tileverse.parquetry.internal.filter;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.filter.Value;

class ValueComparisonTest {

    @Test
    void boxedIntComparesToIntVal() {
        assertThat(ValueComparison.compareBoxed(5, new Value.IntVal(3))).isPositive();
        assertThat(ValueComparison.compareBoxed(3, new Value.IntVal(3))).isZero();
    }

    @Test
    void compareValuesOrdersSameTypedStrings() {
        assertThat(ValueComparison.compareValues(new Value.StringVal("a"), new Value.StringVal("b")))
                .isNegative();
        assertThat(ValueComparison.compareValues(new Value.StringVal("b"), new Value.StringVal("a")))
                .isPositive();
        assertThat(ValueComparison.compareValues(new Value.StringVal("a"), new Value.StringVal("a")))
                .isZero();
    }

    @Test
    void compareValuesOrdersSameTypedDates() {
        LocalDate earlier = LocalDate.of(2020, 1, 1);
        LocalDate later = LocalDate.of(2021, 1, 1);
        assertThat(ValueComparison.compareValues(new Value.DateVal(earlier), new Value.DateVal(later)))
                .isNegative();
        assertThat(ValueComparison.compareValues(new Value.DateVal(later), new Value.DateVal(earlier)))
                .isPositive();
        assertThat(ValueComparison.compareValues(new Value.DateVal(earlier), new Value.DateVal(earlier)))
                .isZero();
    }

    @Test
    void compareValuesOrdersSameTypedUuidsUnsigned() {
        // The high bit is set on the larger uuid; under signed long ordering it would compare LESS, hence
        // this case pins that the engine uses unsigned byte ordering to agree with file statistics.
        UUID highBitSet = UUID.fromString("ffffffff-0000-0000-0000-000000000000");
        UUID allZero = UUID.fromString("00000000-0000-0000-0000-000000000000");
        assertThat(ValueComparison.compareValues(new Value.UuidVal(allZero), new Value.UuidVal(highBitSet)))
                .isNegative();
        assertThat(ValueComparison.compareValues(new Value.UuidVal(highBitSet), new Value.UuidVal(allZero)))
                .isPositive();
        assertThat(ValueComparison.compareValues(new Value.UuidVal(highBitSet), new Value.UuidVal(highBitSet)))
                .isZero();
    }

    @Test
    void valueVsValueBinaryIsUnsignedLexicographic() {
        Value a = new Value.BinaryVal(MemorySegment.ofArray(new byte[] {(byte) 0x80}));
        Value b = new Value.BinaryVal(MemorySegment.ofArray(new byte[] {0x7f}));
        assertThat(ValueComparison.compareValues(a, b)).isPositive();
    }

    @Test
    void stringActualComparesToBinaryBound() {
        Object actual = "b";
        Value bound = new Value.BinaryVal(MemorySegment.ofArray("a".getBytes(StandardCharsets.UTF_8)));
        assertThat(ValueComparison.compareBoxed(actual, bound)).isPositive();
    }

    @Test
    void compareValuesWidensDoubleQueryAgainstFloatBound() {
        assertThat(ValueComparison.compareValues(new Value.DoubleVal(1.5), new Value.FloatVal(2.0f)))
                .isNegative();
        assertThat(ValueComparison.compareValues(new Value.DoubleVal(2.0), new Value.FloatVal(2.0f)))
                .isZero();
        assertThat(ValueComparison.compareValues(new Value.DoubleVal(3.0), new Value.FloatVal(2.0f)))
                .isPositive();
    }

    @Test
    void compareValuesWidensFloatQueryAgainstDoubleBound() {
        assertThat(ValueComparison.compareValues(new Value.FloatVal(2.0f), new Value.DoubleVal(1.5)))
                .isPositive();
    }

    @Test
    void compareBoxedWidensFloatActualAgainstDoubleBound() {
        assertThat(ValueComparison.compareBoxed(2.0f, new Value.DoubleVal(1.5))).isPositive();
        assertThat(ValueComparison.compareBoxed(2.0f, new Value.DoubleVal(2.0))).isZero();
    }

    @Test
    void compareBoxedWidensDoubleActualAgainstFloatBound() {
        assertThat(ValueComparison.compareBoxed(1.5, new Value.FloatVal(2.0f))).isNegative();
    }

    @Test
    void compareBoxedWidensIntValAgainstLongActual() {
        assertThat(ValueComparison.compareBoxed(5L, new Value.IntVal(3))).isPositive();
        assertThat(ValueComparison.compareBoxed(3L, new Value.IntVal(3))).isZero();
        assertThat(ValueComparison.compareBoxed(2L, new Value.IntVal(3))).isNegative();
    }

    @Test
    void compareBoxedWidensLongValAgainstIntActual() {
        assertThat(ValueComparison.compareBoxed(5, new Value.LongVal(3L))).isPositive();
        assertThat(ValueComparison.compareBoxed(3, new Value.LongVal(3L))).isZero();
        assertThat(ValueComparison.compareBoxed(2, new Value.LongVal(3L))).isNegative();
    }

    @Test
    void compareValuesWidensIntQueryAgainstLongBound() {
        assertThat(ValueComparison.compareValues(new Value.IntVal(5), new Value.LongVal(3L)))
                .isPositive();
        assertThat(ValueComparison.compareValues(new Value.IntVal(3), new Value.LongVal(3L)))
                .isZero();
        assertThat(ValueComparison.compareValues(new Value.IntVal(2), new Value.LongVal(3L)))
                .isNegative();
    }

    @Test
    void compareValuesWidensLongQueryAgainstIntBound() {
        assertThat(ValueComparison.compareValues(new Value.LongVal(5L), new Value.IntVal(3)))
                .isPositive();
        assertThat(ValueComparison.compareValues(new Value.LongVal(3L), new Value.IntVal(3)))
                .isZero();
        assertThat(ValueComparison.compareValues(new Value.LongVal(2L), new Value.IntVal(3)))
                .isNegative();
    }

    @Test
    void compareIntWidensAgainstLongBound() {
        assertThat(ValueComparison.compareInt(5, new Value.LongVal(3L))).isPositive();
        assertThat(ValueComparison.compareInt(3, new Value.LongVal(3L))).isZero();
        assertThat(ValueComparison.compareInt(2, new Value.LongVal(3L))).isNegative();
    }

    @Test
    void compareLongWidensAgainstIntBound() {
        assertThat(ValueComparison.compareLong(5L, new Value.IntVal(3))).isPositive();
        assertThat(ValueComparison.compareLong(3L, new Value.IntVal(3))).isZero();
        assertThat(ValueComparison.compareLong(2L, new Value.IntVal(3))).isNegative();
    }

    @Test
    void compareFloatWidensAgainstDoubleBound() {
        assertThat(ValueComparison.compareFloat(2.0f, new Value.DoubleVal(1.5))).isPositive();
    }

    @Test
    void compareDoubleWidensAgainstFloatBound() {
        assertThat(ValueComparison.compareDouble(1.5, new Value.FloatVal(2.0f))).isNegative();
    }

    @ParameterizedTest(name = "compareBoxed({0}, {1}) sign == {2}")
    @MethodSource("boxedCoercionCases")
    void compareBoxedCoercesAndFallsThrough(Object actual, Value bound, int expectedSign) {
        assertThat(Integer.signum(ValueComparison.compareBoxed(actual, bound))).isEqualTo(expectedSign);
    }

    static Stream<Arguments> boxedCoercionCases() {
        LocalDate date = LocalDate.of(2020, 1, 2);
        int epochDay = (int) date.toEpochDay();
        LocalDateTime ts = LocalDateTime.of(2020, 1, 2, 3, 4, 5);
        return Stream.of(
                // DateVal bound vs Integer record value (epoch-day coercion)
                Arguments.of(Integer.valueOf(epochDay), new Value.DateVal(date), 0),
                Arguments.of(Integer.valueOf(epochDay + 1), new Value.DateVal(date), 1),
                Arguments.of(Integer.valueOf(epochDay - 1), new Value.DateVal(date), -1),
                // TimestampVal bound vs LocalDateTime record value
                Arguments.of(ts, new Value.TimestampVal(ts, true), 0),
                Arguments.of(ts.plusSeconds(1), new Value.TimestampVal(ts, true), 1),
                // genuine type mismatch falls through to 0
                Arguments.of(Integer.valueOf(7), new Value.BoolVal(true), 0));
    }

    @ParameterizedTest(name = "compareValues({0}, {1}) sign == {2}")
    @MethodSource("valueCoercionCases")
    void compareValuesCoercesAndFallsThrough(Value query, Value bound, int expectedSign) {
        assertThat(Integer.signum(ValueComparison.compareValues(query, bound))).isEqualTo(expectedSign);
    }

    static Stream<Arguments> valueCoercionCases() {
        LocalDate date = LocalDate.of(2020, 1, 2);
        int epochDay = (int) date.toEpochDay();
        return Stream.of(
                // DateVal query vs IntVal bound (epoch-day coercion)
                Arguments.of(new Value.DateVal(date), new Value.IntVal(epochDay), 0),
                Arguments.of(new Value.DateVal(date), new Value.IntVal(epochDay - 1), 1),
                Arguments.of(new Value.DateVal(date), new Value.IntVal(epochDay + 1), -1),
                // genuine type mismatch falls through to 0
                Arguments.of(new Value.IntVal(7), new Value.BoolVal(true), 0));
    }

    @Test
    void decimalReflexiveIsScaleIndependent() {
        Value.DecimalVal a = new Value.DecimalVal(new BigDecimal("1.0"));
        Value.DecimalVal b = new Value.DecimalVal(new BigDecimal("1.00"));
        assertThat(ValueComparison.compareValues(a, b)).isZero();
    }

    @Test
    void timestampReflexiveDoesNotCoerceToMillis() {
        // The two instants share the same millisecond (123 ms) but differ in the sub-millisecond part. A
        // millis-truncating comparison would read them as equal; comparing the full LocalDateTime must not.
        LocalDateTime sameMillis = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_000_000);
        LocalDateTime laterMicros = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 123_456_000);
        Value.TimestampVal q = new Value.TimestampVal(sameMillis, true);
        Value.TimestampVal bound = new Value.TimestampVal(laterMicros, true);
        assertThat(ValueComparison.compareValues(q, bound)).isNegative();
    }

    @Test
    void timeReflexiveComparesByTimeOfDay() {
        Value.TimeVal early = new Value.TimeVal(LocalTime.of(1, 0));
        Value.TimeVal late = new Value.TimeVal(LocalTime.of(2, 0));
        assertThat(ValueComparison.compareValues(early, late)).isNegative();
    }

    @Test
    void missingArmMustNotReadAsEqual() {
        Value.DecimalVal small = new Value.DecimalVal(new BigDecimal("1.00"));
        Value.DecimalVal big = new Value.DecimalVal(new BigDecimal("9.00"));
        assertThat(ValueComparison.compareValues(small, big)).isNegative();
    }
}
