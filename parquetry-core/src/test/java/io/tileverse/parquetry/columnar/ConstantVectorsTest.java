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
package io.tileverse.parquetry.columnar;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.UuidConverter;
import io.tileverse.parquetry.testsupport.VectorArrays;

class ConstantVectorsTest {

    @Test
    void fillsIntConstant() {
        ColumnVector vector = ConstantVectors.of(new Value.IntVal(2024), 3);
        assertThat(vector).isInstanceOf(IntVector.class);
        assertThat(vector.size()).isEqualTo(3);
        assertThat(VectorArrays.toArray(((IntVector) vector))).containsExactly(2024, 2024, 2024);
    }

    @Test
    void fillsLongConstant() {
        ColumnVector vector = ConstantVectors.of(new Value.LongVal(9000000000L), 2);
        assertThat(vector).isInstanceOf(LongVector.class);
        assertThat(VectorArrays.toArray(((LongVector) vector))).containsExactly(9000000000L, 9000000000L);
    }

    @Test
    void fillsFloatConstant() {
        ColumnVector vector = ConstantVectors.of(new Value.FloatVal(1.5f), 2);
        assertThat(vector).isInstanceOf(FloatVector.class);
        assertThat(vector.size()).isEqualTo(2);
        assertThat(VectorArrays.toArray(((FloatVector) vector))).containsExactly(1.5f, 1.5f);
    }

    @Test
    void fillsDateAsEpochDayInt() {
        ColumnVector vector = ConstantVectors.of(new Value.DateVal(LocalDate.parse("2024-01-15")), 2);
        assertThat(vector).isInstanceOf(IntVector.class);
        int epochDay = (int) LocalDate.parse("2024-01-15").toEpochDay();
        assertThat(VectorArrays.toArray(((IntVector) vector))).containsExactly(epochDay, epochDay);
    }

    @Test
    void fillsStringConstant() {
        ColumnVector vector = ConstantVectors.of(new Value.StringVal("buildings"), 2);
        assertThat(vector).isInstanceOf(BinaryVector.class);
        assertThat(vector.size()).isEqualTo(2);
    }

    @Test
    void nullValueFillsAllNull() {
        ColumnVector vector = ConstantVectors.ofNull(new Value.IntVal(0), 3);
        assertThat(vector.size()).isEqualTo(3);
        assertThat(vector.isNull(0)).isTrue();
        assertThat(vector.isNull(2)).isTrue();
    }

    @Test
    void ofNullUuidIsAllNullSixteenByteFixed() {
        ColumnVector vector = ConstantVectors.ofNull(new Value.UuidVal(new UUID(0L, 0L)), 3);
        assertThat(vector).isInstanceOf(FixedLenBinaryVector.class);
        assertThat(((FixedLenBinaryVector) vector).byteWidth()).isEqualTo(16);
        assertThat(vector.size()).isEqualTo(3);
        assertThat(vector.isNull(0)).isTrue();
        assertThat(vector.isNull(1)).isTrue();
        assertThat(vector.isNull(2)).isTrue();
    }

    @Test
    void ofNullDecimalIsAllNullBinaryVector() {
        ColumnVector vector = ConstantVectors.ofNull(new Value.DecimalVal(BigDecimal.ONE), 3);
        assertThat(vector).isInstanceOf(BinaryVector.class);
        assertThat(vector.size()).isEqualTo(3);
        assertThat(vector.isNull(0)).isTrue();
        assertThat(vector.isNull(2)).isTrue();
    }

    @Test
    void ofNullTimeIsAllNullLongVector() {
        ColumnVector vector = ConstantVectors.ofNull(new Value.TimeVal(LocalTime.NOON), 3);
        assertThat(vector).isInstanceOf(LongVector.class);
        assertThat(vector.size()).isEqualTo(3);
        assertThat(vector.isNull(0)).isTrue();
        assertThat(vector.isNull(2)).isTrue();
    }

    @Test
    void fillsUuidConstantAsSixteenByteFixed() {
        UUID uuid = UUID.fromString("f79c3e09-677c-4bbd-a479-3f349cb785e7");
        ColumnVector vector = ConstantVectors.of(new Value.UuidVal(uuid), 2);
        assertThat(vector).isInstanceOf(FixedLenBinaryVector.class);
        FixedLenBinaryVector fixed = (FixedLenBinaryVector) vector;
        assertThat(fixed.byteWidth()).isEqualTo(16);
        assertThat(UuidConverter.fromSegment(fixed.get(1))).isEqualTo(uuid);
    }

    @Test
    void fillsDecimalConstantAsMinimalTwosComplementFixed() {
        ColumnVector vector = ConstantVectors.of(new Value.DecimalVal(new BigDecimal("-6.25")), 2);
        assertThat(vector).isInstanceOf(FixedLenBinaryVector.class);
        FixedLenBinaryVector fixed = (FixedLenBinaryVector) vector;
        assertThat(fixed.byteWidth()).isEqualTo(2);
        assertThat(fixed.get(0).toArray(ValueLayout.JAVA_BYTE)).containsExactly((byte) 0xfd, (byte) 0x8f);
    }

    @Test
    void fillsZeroDecimalConstantAsOneZeroByteInEveryRow() {
        ColumnVector vector = ConstantVectors.of(new Value.DecimalVal(BigDecimal.valueOf(0L, 2)), 3);
        assertThat(vector).isInstanceOf(FixedLenBinaryVector.class);
        FixedLenBinaryVector fixed = (FixedLenBinaryVector) vector;
        assertThat(fixed.byteWidth()).isEqualTo(1);
        assertThat(fixed.get(0).toArray(ValueLayout.JAVA_BYTE)).containsExactly((byte) 0);
        assertThat(fixed.get(1).toArray(ValueLayout.JAVA_BYTE)).containsExactly((byte) 0);
        assertThat(fixed.get(2).toArray(ValueLayout.JAVA_BYTE)).containsExactly((byte) 0);
    }

    @Test
    void fillsTimeConstantAsMicrosSinceMidnight() {
        ColumnVector vector = ConstantVectors.of(new Value.TimeVal(LocalTime.of(12, 34, 56, 789_000)), 2);
        assertThat(vector).isInstanceOf(LongVector.class);
        assertThat(VectorArrays.toArray((LongVector) vector)).containsExactly(45_296_000_789L, 45_296_000_789L);
    }

    @Test
    void fillsTimestampConstantAsEpochMicros() {
        LocalDateTime timestamp = LocalDateTime.of(2020, 2, 3, 4, 5, 6, 7_000);
        ColumnVector vector = ConstantVectors.of(new Value.TimestampVal(timestamp, true), 1);
        long expected = timestamp.toEpochSecond(ZoneOffset.UTC) * 1_000_000L + 7L;
        assertThat(VectorArrays.toArray((LongVector) vector)).containsExactly(expected);
    }

    @Test
    void fillsPre1970TimestampConstantExactly() {
        LocalDateTime timestamp = LocalDateTime.of(1969, 12, 31, 23, 59, 59, 500_000_000);
        ColumnVector vector = ConstantVectors.of(new Value.TimestampVal(timestamp, false), 1);
        assertThat(VectorArrays.toArray((LongVector) vector)).containsExactly(-500_000L);
    }

    @Test
    void fillsBinaryConstant() {
        MemorySegment bytes = MemorySegment.ofArray(new byte[] {0x0a, (byte) 0xff});
        ColumnVector vector = ConstantVectors.of(new Value.BinaryVal(bytes), 2);
        assertThat(vector).isInstanceOf(BinaryVector.class);
        assertThat(((BinaryVector) vector).get(1).toArray(ValueLayout.JAVA_BYTE))
                .containsExactly((byte) 0x0a, (byte) 0xff);
    }

    @Test
    void ofNullBinaryIsAllNullBinaryVector() {
        ColumnVector vector = ConstantVectors.ofNull(new Value.BinaryVal(MemorySegment.ofArray(new byte[0])), 3);
        assertThat(vector).isInstanceOf(BinaryVector.class);
        assertThat(vector.isNull(0)).isTrue();
        assertThat(vector.isNull(2)).isTrue();
    }

    @Test
    void ofNullTimestampIsAllNullLongVector() {
        ColumnVector vector =
                ConstantVectors.ofNull(new Value.TimestampVal(LocalDateTime.of(2020, 1, 1, 0, 0), true), 3);
        assertThat(vector).isInstanceOf(LongVector.class);
        assertThat(vector.isNull(0)).isTrue();
        assertThat(vector.isNull(2)).isTrue();
    }
}
