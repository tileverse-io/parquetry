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
package io.tileverse.parquetry.avro;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.UUID;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

/**
 * Encodes a logical-typed value to its physical Avro form, inverting {@link LogicalValues}. When the caller already
 * supplies the raw underlying value (an {@code Integer} for a date, bytes for a decimal), this defers to the physical
 * encoding by returning false; otherwise it converts the typed value and writes it.
 */
final class LogicalEncoding {

    private static final ValueLayout.OfInt LITTLE_ENDIAN_INT =
            ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    boolean encodes(AvroSchema schema, Object value, AvroBinaryEncoder out) {
        if (schema.logicalType().isEmpty()) {
            return false;
        }
        LogicalType logical = schema.logicalType().orElseThrow();
        return switch (logical) {
            case LogicalType.Decimal decimal -> encodeDecimal(schema, decimal, value, out);
            case LogicalType.Uuid ignored -> encodeUuid(schema, value, out);
            case LogicalType.Date ignored -> encodeInt(value, LocalDate.class, date -> (int) date.toEpochDay(), out);
            case LogicalType.TimeMillis ignored ->
                encodeInt(value, LocalTime.class, time -> (int) (time.toNanoOfDay() / 1_000_000L), out);
            case LogicalType.TimeMicros ignored ->
                encodeLong(value, LocalTime.class, time -> time.toNanoOfDay() / 1_000L, out);
            case LogicalType.TimestampMillis ignored -> encodeLong(value, Instant.class, Instant::toEpochMilli, out);
            case LogicalType.TimestampMicros ignored ->
                encodeLong(value, Instant.class, LogicalEncoding::epochMicros, out);
            case LogicalType.LocalTimestampMillis ignored ->
                encodeLong(
                        value,
                        LocalDateTime.class,
                        dt -> dt.toInstant(ZoneOffset.UTC).toEpochMilli(),
                        out);
            case LogicalType.LocalTimestampMicros ignored ->
                encodeLong(value, LocalDateTime.class, dt -> epochMicros(dt.toInstant(ZoneOffset.UTC)), out);
            case LogicalType.Duration ignored -> encodeDuration(value, out);
            case LogicalType.Unknown ignored -> false;
        };
    }

    private <T> boolean encodeInt(Object value, Class<T> typed, ToIntFunction<T> toRaw, AvroBinaryEncoder out) {
        if (!typed.isInstance(value)) {
            return false;
        }
        out.writeInt(toRaw.applyAsInt(typed.cast(value)));
        return true;
    }

    private <T> boolean encodeLong(Object value, Class<T> typed, ToLongFunction<T> toRaw, AvroBinaryEncoder out) {
        if (!typed.isInstance(value)) {
            return false;
        }
        out.writeLong(toRaw.applyAsLong(typed.cast(value)));
        return true;
    }

    private boolean encodeUuid(AvroSchema schema, Object value, AvroBinaryEncoder out) {
        if (!(value instanceof UUID id)) {
            return false;
        }
        if (schema instanceof AvroSchema.Fixed) {
            out.writeFixed(uuidToBigEndianBytes(id));
        } else {
            out.writeString(id.toString());
        }
        return true;
    }

    private byte[] uuidToBigEndianBytes(UUID id) {
        byte[] bytes = new byte[16];
        MemorySegment segment = MemorySegment.ofArray(bytes);
        ValueLayout.OfLong bigEndianLong = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
        segment.set(bigEndianLong, 0, id.getMostSignificantBits());
        segment.set(bigEndianLong, 8, id.getLeastSignificantBits());
        return bytes;
    }

    private boolean encodeDecimal(AvroSchema schema, LogicalType.Decimal decimal, Object value, AvroBinaryEncoder out) {
        if (!(value instanceof BigDecimal number)) {
            return false;
        }
        byte[] unscaled = scaledUnscaledBytes(number, decimal.scale());
        if (schema instanceof AvroSchema.Fixed fixed) {
            out.writeFixed(signExtend(unscaled, fixed.size()));
        } else {
            out.writeBytes(unscaled);
        }
        return true;
    }

    private byte[] scaledUnscaledBytes(BigDecimal number, int scale) {
        try {
            return number.setScale(scale, RoundingMode.UNNECESSARY)
                    .unscaledValue()
                    .toByteArray();
        } catch (ArithmeticException mismatch) {
            throw new AvroFormatException("Decimal " + number + " does not fit scale " + scale, mismatch);
        }
    }

    private byte[] signExtend(byte[] unscaled, int size) {
        if (unscaled.length > size) {
            throw new AvroFormatException("Decimal needs " + unscaled.length + " bytes, exceeds fixed size " + size);
        }
        byte[] padded = new byte[size];
        byte fill = (byte) (unscaled[0] < 0 ? 0xFF : 0x00);
        Arrays.fill(padded, 0, size - unscaled.length, fill);
        System.arraycopy(unscaled, 0, padded, size - unscaled.length, unscaled.length);
        return padded;
    }

    private boolean encodeDuration(Object value, AvroBinaryEncoder out) {
        if (!(value instanceof AvroDuration duration)) {
            return false;
        }
        byte[] fixed12 = new byte[12];
        MemorySegment segment = MemorySegment.ofArray(fixed12);
        segment.set(LITTLE_ENDIAN_INT, 0, (int) duration.months());
        segment.set(LITTLE_ENDIAN_INT, 4, (int) duration.days());
        segment.set(LITTLE_ENDIAN_INT, 8, (int) duration.millis());
        out.writeFixed(fixed12);
        return true;
    }

    private static long epochMicros(Instant instant) {
        return Math.multiplyExact(instant.getEpochSecond(), 1_000_000L) + instant.getNano() / 1_000L;
    }
}
