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
package io.tileverse.parquetry.geotools.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType.TimeUnit;

class TemporalValuesTest {

    private static final long TWENTY_TWENTY_FOUR_EPOCH_SECOND = 1_704_067_200L; // 2024-01-01T00:00:00Z

    @Test
    void epochDayBecomesLocalDate() {
        assertThat(TemporalValues.toLocalDate(0)).isEqualTo(LocalDate.parse("1970-01-01"));
        assertThat(TemporalValues.toLocalDate(
                        (int) LocalDate.parse("2024-01-01").toEpochDay()))
                .isEqualTo(LocalDate.parse("2024-01-01"));
    }

    @Test
    void microsUtcBecomesInstantWithSubSecondPrecision() {
        long micros = TWENTY_TWENTY_FOUR_EPOCH_SECOND * 1_000_000L + 1;
        assertThat(TemporalValues.toInstant(micros, TimeUnit.MICROS))
                .isEqualTo(Instant.ofEpochSecond(TWENTY_TWENTY_FOUR_EPOCH_SECOND, 1_000L));
    }

    @Test
    void nanosUtcBecomesInstantWithNanoPrecision() {
        long nanos = TWENTY_TWENTY_FOUR_EPOCH_SECOND * 1_000_000_000L + 123_456_789L;
        assertThat(TemporalValues.toInstant(nanos, TimeUnit.NANOS))
                .isEqualTo(Instant.ofEpochSecond(TWENTY_TWENTY_FOUR_EPOCH_SECOND, 123_456_789L));
    }

    @Test
    void millisUtcBecomesInstant() {
        long millis = TWENTY_TWENTY_FOUR_EPOCH_SECOND * 1_000L + 1;
        assertThat(TemporalValues.toInstant(millis, TimeUnit.MILLIS))
                .isEqualTo(Instant.ofEpochSecond(TWENTY_TWENTY_FOUR_EPOCH_SECOND, 1_000_000L));
    }

    @Test
    void microsLocalBecomesLocalDateTime() {
        long micros = TWENTY_TWENTY_FOUR_EPOCH_SECOND * 1_000_000L + 500_000L;
        assertThat(TemporalValues.toLocalDateTime(micros, TimeUnit.MICROS))
                .isEqualTo(LocalDateTime.ofEpochSecond(TWENTY_TWENTY_FOUR_EPOCH_SECOND, 500_000_000, ZoneOffset.UTC));
    }

    @Test
    void nanosLocalBecomesLocalDateTime() {
        long nanos = TWENTY_TWENTY_FOUR_EPOCH_SECOND * 1_000_000_000L + 123_456_789L;
        assertThat(TemporalValues.toLocalDateTime(nanos, TimeUnit.NANOS))
                .isEqualTo(LocalDateTime.ofEpochSecond(TWENTY_TWENTY_FOUR_EPOCH_SECOND, 123_456_789, ZoneOffset.UTC));
    }

    @Test
    void negativeMicrosBeforeEpochUsesFloorSemantics() {
        assertThat(TemporalValues.toInstant(-1L, TimeUnit.MICROS)).isEqualTo(Instant.ofEpochSecond(-1L, 999_999_000L));
    }

    @Test
    void negativeNanosBeforeEpochUsesFloorSemantics() {
        assertThat(TemporalValues.toLocalDateTime(-1L, TimeUnit.NANOS))
                .isEqualTo(LocalDateTime.ofEpochSecond(-1L, 999_999_999, ZoneOffset.UTC));
    }
}
