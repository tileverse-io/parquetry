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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.LocalDateTime;
import java.time.LocalTime;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType.TimeUnit;

class TemporalValuesTest {

    @Test
    void timestampRoundTripsAtEachUnit() {
        LocalDateTime dt = LocalDateTime.of(2020, 1, 2, 3, 4, 5, 678_000_000);
        for (TimeUnit unit : new TimeUnit[] {TimeUnit.MILLIS, TimeUnit.MICROS, TimeUnit.NANOS}) {
            long epoch = TemporalValues.toEpochUnit(dt, unit);
            assertThat(TemporalValues.toLocalDateTime(epoch, unit)).isEqualTo(dt);
        }
    }

    @Test
    void microsAndMillisDifferByAThousand() {
        LocalDateTime dt = LocalDateTime.of(1970, 1, 1, 0, 0, 1);
        assertThat(TemporalValues.toEpochUnit(dt, TimeUnit.MILLIS)).isEqualTo(1_000L);
        assertThat(TemporalValues.toEpochUnit(dt, TimeUnit.MICROS)).isEqualTo(1_000_000L);
    }

    @Test
    void pre1970TimestampFloorsExactly() {
        LocalDateTime dt = LocalDateTime.of(1969, 12, 31, 23, 59, 59);
        long micros = TemporalValues.toEpochUnit(dt, TimeUnit.MICROS);
        assertThat(micros).isEqualTo(-1_000_000L);
        assertThat(TemporalValues.toLocalDateTime(micros, TimeUnit.MICROS)).isEqualTo(dt);
    }

    @Test
    void epochUnitIsExactAtTheEndsOfTheInt64Range() {
        LocalDateTime last = TemporalValues.toLocalDateTime(Long.MAX_VALUE, TimeUnit.NANOS);
        LocalDateTime first = TemporalValues.toLocalDateTime(Long.MIN_VALUE, TimeUnit.NANOS);

        assertThat(TemporalValues.toEpochUnitIfInRange(last, TimeUnit.NANOS)).hasValue(Long.MAX_VALUE);
        assertThat(TemporalValues.toEpochUnitIfInRange(first, TimeUnit.NANOS)).hasValue(Long.MIN_VALUE);
    }

    @Test
    void timestampOneNanosecondBeyondTheInt64RangeHasNoEpochUnit() {
        LocalDateTime last = TemporalValues.toLocalDateTime(Long.MAX_VALUE, TimeUnit.NANOS);
        LocalDateTime first = TemporalValues.toLocalDateTime(Long.MIN_VALUE, TimeUnit.NANOS);

        assertThat(TemporalValues.toEpochUnitIfInRange(last.plusNanos(1), TimeUnit.NANOS))
                .isEmpty();
        assertThat(TemporalValues.toEpochUnitIfInRange(first.minusNanos(1), TimeUnit.NANOS))
                .isEmpty();
    }

    @Test
    void widestTimestampsHaveNoEpochUnitAtAnyUnit() {
        for (TimeUnit unit : TimeUnit.values()) {
            assertThat(TemporalValues.toEpochUnitIfInRange(LocalDateTime.MAX, unit))
                    .as("latest timestamp in %s", unit)
                    .isEmpty();
            assertThat(TemporalValues.toEpochUnitIfInRange(LocalDateTime.MIN, unit))
                    .as("earliest timestamp in %s", unit)
                    .isEmpty();
        }
    }

    @Test
    void year9999HasAnEpochUnitInMicrosAndNoneInNanos() {
        LocalDateTime year9999 = LocalDateTime.of(9999, 12, 31, 0, 0);

        assertThat(TemporalValues.toEpochUnitIfInRange(year9999, TimeUnit.MICROS))
                .hasValue(253_402_214_400_000_000L);
        assertThat(TemporalValues.toEpochUnitIfInRange(year9999, TimeUnit.NANOS))
                .isEmpty();
    }

    @Test
    void toEpochUnitRejectsATimestampBeyondTheInt64Range() {
        LocalDateTime year9999 = LocalDateTime.of(9999, 12, 31, 0, 0);

        assertThatThrownBy(() -> TemporalValues.toEpochUnit(year9999, TimeUnit.NANOS))
                .isInstanceOf(ArithmeticException.class)
                .hasMessageContaining("9999-12-31T00:00")
                .hasMessageContaining("NANOS");
    }

    @Test
    void timeRoundTripsInMicros() {
        LocalTime t = LocalTime.of(12, 34, 56, 789_012_000);
        long micros = TemporalValues.toTimeUnit(t, TimeUnit.MICROS);
        assertThat(micros).isEqualTo(45_296_789_012L);
        assertThat(TemporalValues.toLocalTime(micros, TimeUnit.MICROS)).isEqualTo(t);
    }
}
