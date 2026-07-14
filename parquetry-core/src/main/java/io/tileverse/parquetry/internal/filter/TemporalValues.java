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

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

import io.tileverse.parquetry.format.LogicalType.TimeUnit;

/**
 * Unit-aware conversion between a Parquet physical temporal value (an INT64 count of MILLIS/MICROS/NANOS) and its
 * {@code java.time} form. Timestamps reference 1970-01-01T00:00 UTC uniformly; the column's adjusted-to-UTC flag does
 * not change ordering within one column, only its display. Times count from midnight. Division floors, hence pre-1970
 * timestamps round-trip exactly.
 */
public final class TemporalValues {

    private TemporalValues() {}

    /** The wall-clock timestamp an INT64 {@code value} in {@code unit} encodes. */
    public static LocalDateTime toLocalDateTime(long value, TimeUnit unit) {
        long perSecond = perSecond(unit);
        long seconds = Math.floorDiv(value, perSecond);
        long nanoOfSecond = Math.floorMod(value, perSecond) * nanosPerUnit(unit);
        return LocalDateTime.ofEpochSecond(seconds, (int) nanoOfSecond, ZoneOffset.UTC);
    }

    /** The INT64 encoding, in {@code unit}, of a wall-clock timestamp. */
    public static long toEpochUnit(LocalDateTime value, TimeUnit unit) {
        long seconds = value.toEpochSecond(ZoneOffset.UTC);
        long subSecond = value.getNano() / nanosPerUnit(unit);
        // The multiply keeps the value in the INT64 physical range, overflowing only outside it (past
        // year ~2262 for NANOS) - the same boundary the physical representation already has.
        return seconds * perSecond(unit) + subSecond;
    }

    /** The time-of-day an INT64 {@code value} (count since midnight) in {@code unit} encodes. */
    public static LocalTime toLocalTime(long value, TimeUnit unit) {
        return LocalTime.ofNanoOfDay(value * nanosPerUnit(unit));
    }

    /** The INT64 encoding, in {@code unit}, of a time-of-day (count since midnight). */
    public static long toTimeUnit(LocalTime value, TimeUnit unit) {
        return value.toNanoOfDay() / nanosPerUnit(unit);
    }

    private static long perSecond(TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> 1_000L;
            case MICROS -> 1_000_000L;
            case NANOS -> 1_000_000_000L;
        };
    }

    private static long nanosPerUnit(TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> 1_000_000L;
            case MICROS -> 1_000L;
            case NANOS -> 1L;
        };
    }
}
