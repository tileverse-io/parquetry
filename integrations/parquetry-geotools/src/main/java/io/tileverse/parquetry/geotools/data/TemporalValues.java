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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import io.tileverse.parquetry.format.LogicalType.TimeUnit;

/**
 * Converts a raw Parquet temporal encoding to its {@code java.time} value for a GeoTools attribute. A DATE column is
 * days since the Unix epoch; a TIMESTAMP column is a signed count of {@link TimeUnit} ticks since the epoch. Splitting
 * that count into whole seconds and a non-negative nanosecond remainder with
 * {@link Math#floorDiv}/{@link Math#floorMod} keeps pre-1970 (negative) values exact.
 */
final class TemporalValues {

    private TemporalValues() {}

    /** The DATE value (days since the Unix epoch) as a {@link LocalDate}. */
    static LocalDate toLocalDate(int epochDay) {
        return LocalDate.ofEpochDay(epochDay);
    }

    /** A UTC-adjusted TIMESTAMP as the instant it names. */
    static Instant toInstant(long value, TimeUnit unit) {
        long ticksPerSecond = ticksPerSecond(unit);
        long seconds = Math.floorDiv(value, ticksPerSecond);
        long nanoOfSecond = Math.floorMod(value, ticksPerSecond) * nanosPerTick(unit);
        return Instant.ofEpochSecond(seconds, nanoOfSecond);
    }

    /** A zone-less TIMESTAMP as the wall-clock date-time it names, read at UTC. */
    static LocalDateTime toLocalDateTime(long value, TimeUnit unit) {
        long ticksPerSecond = ticksPerSecond(unit);
        long seconds = Math.floorDiv(value, ticksPerSecond);
        long nanoOfSecond = Math.floorMod(value, ticksPerSecond) * nanosPerTick(unit);
        return LocalDateTime.ofEpochSecond(seconds, (int) nanoOfSecond, ZoneOffset.UTC);
    }

    private static long ticksPerSecond(TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> 1_000L;
            case MICROS -> 1_000_000L;
            case NANOS -> 1_000_000_000L;
        };
    }

    private static long nanosPerTick(TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> 1_000_000L;
            case MICROS -> 1_000L;
            case NANOS -> 1L;
        };
    }
}
