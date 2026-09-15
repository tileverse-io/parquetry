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

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.stream.Stream;

import org.geotools.temporal.object.DefaultInstant;
import org.geotools.temporal.object.DefaultPosition;
import org.geotools.util.Converters;
import org.geotools.util.factory.Hints;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Every conversion goes through {@link Converters}, which proves the service registration as well as the rules: a
 * zone-less value is UTC wall-clock time, a SQL date or time reads as its own calendar value, an offset in a text is
 * honored, a truncation is refused under the safe conversion hint, and an unparseable text or an unsupported pair
 * converts to null instead of throwing.
 */
class JavaTimeConverterFactoryTest {

    private static final LocalDateTime NOON = LocalDateTime.of(2024, 6, 15, 12, 30, 45, 123_000_000);
    private static final Instant NOON_UTC = NOON.toInstant(ZoneOffset.UTC);
    private static final Date NOON_DATE = Date.from(NOON_UTC);

    @ParameterizedTest(name = "{0}")
    @MethodSource("toJavaTime")
    void convertsToJavaTime(String name, Object source, Class<?> target, Object expected) {
        assertThat(Converters.convert(source, target)).isEqualTo(expected);
    }

    private static Stream<Arguments> toJavaTime() {
        Calendar calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
        calendar.setTime(NOON_DATE);
        return Stream.of(
                Arguments.of("Date to Instant", NOON_DATE, Instant.class, NOON_UTC),
                Arguments.of("Date to LocalDateTime at UTC", NOON_DATE, LocalDateTime.class, NOON),
                Arguments.of("Date to LocalDate at UTC", NOON_DATE, LocalDate.class, LocalDate.of(2024, 6, 15)),
                Arguments.of(
                        "Date to LocalTime at UTC", NOON_DATE, LocalTime.class, LocalTime.of(12, 30, 45, 123_000_000)),
                Arguments.of("sql Timestamp to Instant", Timestamp.from(NOON_UTC), Instant.class, NOON_UTC),
                Arguments.of(
                        "sql Date to LocalDate is its calendar date",
                        java.sql.Date.valueOf("2024-06-15"),
                        LocalDate.class,
                        LocalDate.of(2024, 6, 15)),
                Arguments.of(
                        "sql Date to LocalDateTime is midnight on its calendar date",
                        java.sql.Date.valueOf("2024-06-15"),
                        LocalDateTime.class,
                        LocalDate.of(2024, 6, 15).atStartOfDay()),
                Arguments.of(
                        "sql Time to LocalTime is its clock time",
                        java.sql.Time.valueOf("12:30:45"),
                        LocalTime.class,
                        LocalTime.of(12, 30, 45)),
                Arguments.of(
                        "sql Date built from an instant reads its date in the default zone",
                        new java.sql.Date(NOON_UTC.toEpochMilli()),
                        LocalDate.class,
                        NOON_UTC.atZone(ZoneId.systemDefault()).toLocalDate()),
                Arguments.of(
                        "sql Time built from an instant reads its clock in the default zone, to the second",
                        new java.sql.Time(NOON_UTC.toEpochMilli()),
                        LocalTime.class,
                        NOON_UTC.atZone(ZoneId.systemDefault()).toLocalTime().withNano(0)),
                Arguments.of("Calendar to LocalDateTime", calendar, LocalDateTime.class, NOON),
                Arguments.of("zone-less text to Instant is UTC", "2024-06-15T12:30:45.123", Instant.class, NOON_UTC),
                Arguments.of(
                        "offset text to Instant honors the offset",
                        "2024-06-15T14:30:45.123+02:00",
                        Instant.class,
                        NOON_UTC),
                Arguments.of(
                        "offset text to LocalDateTime is the UTC wall clock",
                        "2024-06-15T14:30:45.123+02:00",
                        LocalDateTime.class,
                        NOON),
                Arguments.of("Z text to LocalDateTime", "2024-06-15T12:30:45.123Z", LocalDateTime.class, NOON),
                Arguments.of("date-only text to LocalDate", "2024-06-15", LocalDate.class, LocalDate.of(2024, 6, 15)),
                Arguments.of(
                        "date-only text to LocalDateTime is midnight",
                        "2024-06-15",
                        LocalDateTime.class,
                        LocalDate.of(2024, 6, 15).atStartOfDay()),
                Arguments.of(
                        "date-only text to Instant is midnight UTC",
                        "2024-06-15",
                        Instant.class,
                        LocalDate.of(2024, 6, 15).atStartOfDay(ZoneOffset.UTC).toInstant()),
                Arguments.of(
                        "time text to LocalTime",
                        "12:30:45.123",
                        LocalTime.class,
                        LocalTime.of(12, 30, 45, 123_000_000)),
                Arguments.of(
                        "date-time text to LocalTime is its time part",
                        "2024-06-15T12:30:45.123Z",
                        LocalTime.class,
                        LocalTime.of(12, 30, 45, 123_000_000)),
                Arguments.of(
                        "nanosecond text keeps its nanos",
                        "2024-06-15T12:30:45.123456789",
                        LocalDateTime.class,
                        NOON.withNano(123_456_789)),
                Arguments.of(
                        "GeoTools Instant to Instant",
                        new DefaultInstant(new DefaultPosition(NOON_DATE)),
                        Instant.class,
                        NOON_UTC),
                Arguments.of(
                        "GeoTools Instant to LocalDateTime",
                        new DefaultInstant(new DefaultPosition(NOON_DATE)),
                        LocalDateTime.class,
                        NOON),
                Arguments.of("Instant to LocalDateTime at UTC", NOON_UTC, LocalDateTime.class, NOON),
                Arguments.of(
                        "Instant to LocalTime truncates",
                        NOON_UTC,
                        LocalTime.class,
                        LocalTime.of(12, 30, 45, 123_000_000)),
                Arguments.of("LocalDateTime to Instant at UTC", NOON, Instant.class, NOON_UTC),
                Arguments.of(
                        "LocalDate to LocalDateTime is midnight",
                        LocalDate.of(2024, 6, 15),
                        LocalDateTime.class,
                        LocalDate.of(2024, 6, 15).atStartOfDay()),
                Arguments.of(
                        "LocalDate to Instant is midnight UTC",
                        LocalDate.of(2024, 6, 15),
                        Instant.class,
                        LocalDate.of(2024, 6, 15).atStartOfDay(ZoneOffset.UTC).toInstant()),
                Arguments.of("LocalDateTime to LocalDate truncates", NOON, LocalDate.class, LocalDate.of(2024, 6, 15)),
                Arguments.of(
                        "LocalDateTime to LocalTime truncates",
                        NOON,
                        LocalTime.class,
                        LocalTime.of(12, 30, 45, 123_000_000)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("fromJavaTime")
    void convertsFromJavaTime(String name, Object source, Class<?> target, Object expected) {
        assertThat(Converters.convert(source, target)).isEqualTo(expected);
    }

    private static Stream<Arguments> fromJavaTime() {
        return Stream.of(
                Arguments.of("Instant to Date", NOON_UTC, Date.class, NOON_DATE),
                Arguments.of("LocalDateTime to Date at UTC", NOON, Date.class, NOON_DATE),
                Arguments.of(
                        "LocalDate to Date is midnight UTC",
                        LocalDate.of(2024, 6, 15),
                        Date.class,
                        Date.from(LocalDate.of(2024, 6, 15)
                                .atStartOfDay(ZoneOffset.UTC)
                                .toInstant())),
                Arguments.of("Instant to sql Timestamp", NOON_UTC, Timestamp.class, Timestamp.from(NOON_UTC)),
                Arguments.of("Instant to String is ISO", NOON_UTC, String.class, "2024-06-15T12:30:45.123Z"),
                Arguments.of("LocalDateTime to String is ISO", NOON, String.class, "2024-06-15T12:30:45.123"),
                Arguments.of("LocalDate to String is ISO", LocalDate.of(2024, 6, 15), String.class, "2024-06-15"),
                Arguments.of(
                        "LocalTime to String is ISO",
                        LocalTime.of(12, 30, 45, 123_000_000),
                        String.class,
                        "12:30:45.123"));
    }

    @Test
    void convertsJavaTimeToAGeoToolsInstant() {
        org.geotools.api.temporal.Instant instant = Converters.convert(NOON, org.geotools.api.temporal.Instant.class);
        assertThat(instant).isNotNull();
        assertThat(instant.getPosition().getDate()).isEqualTo(NOON_DATE);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unsupported")
    void unsupportedOrUnparseableConvertsToNull(String name, Object source, Class<?> target) {
        assertThat(Converters.convert(source, target)).isNull();
    }

    private static Stream<Arguments> unsupported() {
        return Stream.of(
                Arguments.of("garbage text to Instant", "not a date", Instant.class),
                Arguments.of("garbage text to LocalTime", "25:99", LocalTime.class),
                Arguments.of("LocalTime to Date has no day", LocalTime.NOON, Date.class),
                Arguments.of("LocalTime to Instant has no day", LocalTime.NOON, Instant.class));
    }

    @Test
    void safeConversionRefusesATruncation() {
        Hints safe = new Hints(org.geotools.util.ConverterFactory.SAFE_CONVERSION, Boolean.TRUE);
        assertThat(Converters.convert(NOON, LocalDate.class, safe)).isNull();
        assertThat(Converters.convert(NOON, LocalTime.class, safe)).isNull();
        assertThat(Converters.convert(NOON, Instant.class, safe)).isEqualTo(NOON_UTC);
    }
}
