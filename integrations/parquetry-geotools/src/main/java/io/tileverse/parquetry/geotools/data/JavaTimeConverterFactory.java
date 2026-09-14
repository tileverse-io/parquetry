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

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.geotools.temporal.object.DefaultInstant;
import org.geotools.temporal.object.DefaultPosition;
import org.geotools.util.Converter;
import org.geotools.util.ConverterFactory;
import org.geotools.util.factory.Hints;

/**
 * Conversions between the java.time bindings of a parquetry feature type ({@link Instant}, {@link LocalDate},
 * {@link LocalDateTime}, {@link LocalTime}) and the temporal shapes produced and consumed by GeoTools: {@link Date} and
 * its SQL subclasses, {@link Calendar}, ISO-8601 text, and the GeoTools temporal
 * {@link org.geotools.api.temporal.Instant}.
 *
 * <p>A zone-less value is UTC wall-clock time: a {@link LocalDateTime} converts to the {@link Instant} at UTC and back,
 * a {@link Date} reads as its UTC wall clock, and a text without an offset is read at UTC. A {@link java.sql.Date} and
 * a {@link java.sql.Time} are the exception: each names a calendar value rather than an instant, and each converts as
 * that value, with no zone shift applied. A text with an offset or a zone is honored and then read at UTC. A date-only
 * value becomes midnight for a date-time target. A conversion that drops information (a date-time to a date or a time)
 * is refused under {@link ConverterFactory#SAFE_CONVERSION}.
 *
 * <p>Registered through {@code META-INF/services/org.geotools.util.ConverterFactory}, which makes the conversions
 * available to {@link org.geotools.util.Converters} wherever GeoTools coerces a value: filter literals, the in-memory
 * evaluation of a filter against a feature, and encoders.
 */
public final class JavaTimeConverterFactory implements ConverterFactory {

    private static final Set<Class<?>> JAVA_TIME =
            Set.of(Instant.class, LocalDate.class, LocalDateTime.class, LocalTime.class);

    /** The ISO-8601 shapes accepted for a text value, most specific first. */
    private static final List<Function<String, TemporalAccessor>> TEXT_PARSERS = List.of(
            text -> OffsetDateTime.parse(text)
                    .withOffsetSameInstant(ZoneOffset.UTC)
                    .toLocalDateTime(),
            text -> ZonedDateTime.parse(text)
                    .withZoneSameInstant(ZoneOffset.UTC)
                    .toLocalDateTime(),
            LocalDateTime::parse,
            LocalDate::parse,
            text -> OffsetTime.parse(text).withOffsetSameInstant(ZoneOffset.UTC).toLocalTime(),
            LocalTime::parse);

    private static final Converter TO_JAVA_TIME = new ToJavaTime(false);
    private static final Converter TO_JAVA_TIME_SAFE = new ToJavaTime(true);
    private static final Converter FROM_JAVA_TIME = new FromJavaTime();

    @Override
    public Converter createConverter(Class<?> source, Class<?> target, Hints hints) {
        boolean safe = hints != null && Boolean.TRUE.equals(hints.get(SAFE_CONVERSION));
        if (JAVA_TIME.contains(target) && convertsToJavaTime(source)) {
            return safe ? TO_JAVA_TIME_SAFE : TO_JAVA_TIME;
        }
        if (JAVA_TIME.contains(source) && convertsFromJavaTime(target)) {
            return FROM_JAVA_TIME;
        }
        return null;
    }

    private static boolean convertsToJavaTime(Class<?> source) {
        return Date.class.isAssignableFrom(source)
                || Calendar.class.isAssignableFrom(source)
                || String.class.equals(source)
                || org.geotools.api.temporal.Instant.class.isAssignableFrom(source)
                || JAVA_TIME.contains(source);
    }

    private static boolean convertsFromJavaTime(Class<?> target) {
        return Date.class.equals(target)
                || Timestamp.class.equals(target)
                || String.class.equals(target)
                || org.geotools.api.temporal.Instant.class.equals(target);
    }

    /** Converts a temporal source to a java.time target through one normalized UTC wall-clock reading. */
    private record ToJavaTime(boolean safe) implements Converter {

        @Override
        public <T> T convert(Object source, Class<T> target) {
            Optional<TemporalAccessor> normalized = normalize(source);
            if (normalized.isEmpty()) {
                return null;
            }
            TemporalAccessor value = normalized.get();
            if (safe && truncates(value, target)) {
                return null;
            }
            return target.cast(toTarget(value, target));
        }
    }

    /**
     * The source as one of {@link LocalDateTime}, {@link LocalDate}, or {@link LocalTime}, at UTC. A date-time-like
     * source always becomes a {@link LocalDateTime}; a text is matched against each ISO-8601 shape in turn, most
     * specific first.
     *
     * <p>A {@link java.sql.Date} names a calendar date and a {@link java.sql.Time} names a clock time; each reads as
     * that value, which is why neither goes through an instant; a {@link java.sql.Time} keeps hours, minutes, and
     * seconds only, as {@link java.sql.Time#toLocalTime()} defines. Any other {@link Date} is read through its epoch
     * milliseconds. A {@link Timestamp} has a case of its own, where {@link Timestamp#toInstant()} keeps the
     * sub-millisecond digits.
     */
    private static Optional<TemporalAccessor> normalize(Object source) {
        return switch (source) {
            case Timestamp timestamp -> Optional.of(utcWallClock(timestamp.toInstant()));
            case java.sql.Date sqlDate -> Optional.of(sqlDate.toLocalDate());
            case java.sql.Time sqlTime -> Optional.of(sqlTime.toLocalTime());
            case Date date -> Optional.of(utcWallClock(Instant.ofEpochMilli(date.getTime())));
            case Calendar calendar -> Optional.of(utcWallClock(calendar.toInstant()));
            case org.geotools.api.temporal.Instant instant ->
                Optional.of(utcWallClock(instant.getPosition().getDate().toInstant()));
            case Instant instant -> Optional.of(utcWallClock(instant));
            case LocalDateTime dateTime -> Optional.of(dateTime);
            case LocalDate date -> Optional.of(date);
            case LocalTime time -> Optional.of(time);
            case String text -> parse(text.strip());
            default -> Optional.empty();
        };
    }

    private static LocalDateTime utcWallClock(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    /** ISO-8601 text as a date-time, a date, or a time; an offset or zone is applied and the result read at UTC. */
    private static Optional<TemporalAccessor> parse(String text) {
        for (Function<String, TemporalAccessor> parser : TEXT_PARSERS) {
            try {
                return Optional.of(parser.apply(text));
            } catch (DateTimeParseException _) {
                // the text does not have this shape; try the next one
            }
        }
        return Optional.empty();
    }

    /** Whether producing {@code target} from {@code value} drops a component. */
    private static boolean truncates(TemporalAccessor value, Class<?> target) {
        boolean fromDateTime = value instanceof LocalDateTime;
        return fromDateTime && (target == LocalDate.class || target == LocalTime.class);
    }

    private static Object toTarget(TemporalAccessor value, Class<?> target) {
        return switch (value) {
            case LocalDateTime dateTime -> fromDateTime(dateTime, target);
            case LocalDate date -> fromDate(date, target);
            case LocalTime time -> target == LocalTime.class ? time : null;
            default -> null;
        };
    }

    private static Object fromDateTime(LocalDateTime dateTime, Class<?> target) {
        if (target == LocalDateTime.class) {
            return dateTime;
        }
        if (target == Instant.class) {
            return dateTime.toInstant(ZoneOffset.UTC);
        }
        if (target == LocalDate.class) {
            return dateTime.toLocalDate();
        }
        return dateTime.toLocalTime();
    }

    private static Object fromDate(LocalDate date, Class<?> target) {
        if (target == LocalDate.class) {
            return date;
        }
        if (target == LocalDateTime.class) {
            return date.atStartOfDay();
        }
        if (target == Instant.class) {
            return date.atStartOfDay(ZoneOffset.UTC).toInstant();
        }
        return null;
    }

    /**
     * Converts a java.time source to the shapes consumed by GeoTools; a {@link LocalTime} has no day and stays null.
     */
    private static final class FromJavaTime implements Converter {

        @Override
        public <T> T convert(Object source, Class<T> target) {
            if (target == String.class) {
                return target.cast(source.toString());
            }
            Optional<Instant> instant = instantOf(source);
            if (instant.isEmpty()) {
                return null;
            }
            Date date = Date.from(instant.get());
            if (target == Date.class) {
                return target.cast(date);
            }
            if (target == Timestamp.class) {
                return target.cast(Timestamp.from(instant.get()));
            }
            return target.cast(new DefaultInstant(new DefaultPosition(date)));
        }

        private static Optional<Instant> instantOf(Object source) {
            return switch (source) {
                case Instant instant -> Optional.of(instant);
                case LocalDateTime dateTime -> Optional.of(dateTime.toInstant(ZoneOffset.UTC));
                case LocalDate date ->
                    Optional.of(date.atStartOfDay(ZoneOffset.UTC).toInstant());
                default -> Optional.empty();
            };
        }
    }
}
