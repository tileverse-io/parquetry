/*
 * Copyright (c) 2026 Multivers.io
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
package io.tileverse.parquetry.avro;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.UUID;

/**
 * Converts a raw decoded value into the Java value for its {@link LogicalType}. The casts are safe by construction: the
 * parser validates that each annotation applies to its underlying type; inapplicable annotations never reach here.
 */
final class LogicalValues {

    private LogicalValues() {}

    static Object convert(LogicalType logical, Object raw) {
        try {
            return convertRaw(logical, raw);
        } catch (DateTimeException outOfRange) {
            // java.time rejects values outside its supported range; a malformed temporal value is a
            // format error, not an internal one
            throw new AvroFormatException("Temporal value out of range: " + outOfRange.getMessage(), outOfRange);
        }
    }

    private static Object convertRaw(LogicalType logical, Object raw) {
        return switch (logical) {
            case LogicalType.Decimal(int _, int scale) -> decimal(raw, scale);
            case LogicalType.Uuid _ -> uuid(raw);
            case LogicalType.Date _ -> LocalDate.ofEpochDay((Integer) raw);
            case LogicalType.TimeMillis _ -> timeFromMillis((Integer) raw);
            case LogicalType.TimeMicros _ -> timeFromMicros((Long) raw);
            case LogicalType.TimestampMillis _ -> Instant.ofEpochMilli((Long) raw);
            case LogicalType.TimestampMicros _ -> instantFromMicros((Long) raw);
            case LogicalType.LocalTimestampMillis _ ->
                LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) raw), ZoneOffset.UTC);
            case LogicalType.LocalTimestampMicros _ ->
                LocalDateTime.ofInstant(instantFromMicros((Long) raw), ZoneOffset.UTC);
            case LogicalType.Duration _ -> duration((MemorySegment) raw);
            case LogicalType.Unknown _ -> raw;
        };
    }

    private static BigDecimal decimal(Object raw, int scale) {
        MemorySegment segment = (MemorySegment) raw;
        byte[] unscaled = segment.toArray(ValueLayout.JAVA_BYTE);
        if (unscaled.length == 0) {
            throw new AvroFormatException("Malformed decimal: empty unscaled value");
        }
        return new BigDecimal(new BigInteger(unscaled), scale);
    }

    private static UUID uuid(Object raw) {
        if (raw instanceof String text) {
            try {
                return UUID.fromString(text);
            } catch (IllegalArgumentException malformed) {
                throw new AvroFormatException("Malformed uuid string: " + text, malformed);
            }
        }
        MemorySegment segment = (MemorySegment) raw;
        ValueLayout.OfLong bigEndianLong = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
        return new UUID(segment.get(bigEndianLong, 0), segment.get(bigEndianLong, 8));
    }

    private static LocalTime timeFromMillis(int millis) {
        if (millis < 0 || millis > 86_399_999) {
            throw new AvroFormatException("time-millis value out of day range: " + millis);
        }
        return LocalTime.ofNanoOfDay(1_000_000L * millis);
    }

    private static LocalTime timeFromMicros(long micros) {
        if (micros < 0 || micros > 86_399_999_999L) {
            throw new AvroFormatException("time-micros value out of day range: " + micros);
        }
        return LocalTime.ofNanoOfDay(1_000L * micros);
    }

    private static Instant instantFromMicros(long micros) {
        long seconds = Math.floorDiv(micros, 1_000_000L);
        long nanos = Math.floorMod(micros, 1_000_000L) * 1_000L;
        return Instant.ofEpochSecond(seconds, nanos);
    }

    private static AvroDuration duration(MemorySegment fixed12) {
        ValueLayout.OfInt littleEndianInt = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
        long months = Integer.toUnsignedLong(fixed12.get(littleEndianInt, 0));
        long days = Integer.toUnsignedLong(fixed12.get(littleEndianInt, 4));
        long millis = Integer.toUnsignedLong(fixed12.get(littleEndianInt, 8));
        return new AvroDuration(months, days, millis);
    }
}
