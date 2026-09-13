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
package io.tileverse.parquetry.iceberg;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.UUID;
import java.util.function.Supplier;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType.TimeUnit;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.UuidConverter;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.exc.JsonNodeException;

/**
 * The decoding of an Iceberg scalar value into a parquetry {@link Value}, one exhaustive switch over
 * {@link IcebergType} per source: a manifest bound (Iceberg's binary single-value serialization), a data-file cell (an
 * equality-delete tuple component), a JSON single value (an {@code initial-default}), a raw Avro partition value, and
 * the all-null archetype whose kind selects a null-filled column.
 *
 * <p>No switch has a {@code default} arm: a type added to the family fails to compile until every source says how it
 * decodes. Malformed input raises an {@link IcebergFormatException} that names the field or column when the source
 * knows that name.
 */
final class IcebergScalarValues {

    private static final ValueLayout.OfInt LE_INT = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final ValueLayout.OfLong LE_LONG =
            ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    private static final long MICROS_PER_DAY = 86_400_000_000L;

    private IcebergScalarValues() {}

    /** Decodes a manifest lower or upper bound: Iceberg's binary single-value serialization of {@code type}. */
    static Value fromBound(IcebergType type, MemorySegment bytes) {
        return switch (type) {
            case IcebergType.BoolType _ -> new Value.BoolVal(readBoolean(bytes));
            case IcebergType.IntType _ -> new Value.IntVal(readLeInt(bytes));
            case IcebergType.LongType _ -> new Value.LongVal(readLeLong(bytes));
            case IcebergType.FloatType _ -> new Value.FloatVal(Float.intBitsToFloat(readLeInt(bytes)));
            case IcebergType.DoubleType _ -> new Value.DoubleVal(Double.longBitsToDouble(readLeLong(bytes)));
            case IcebergType.DateType _ -> new Value.DateVal(LocalDate.ofEpochDay(readLeInt(bytes)));
            case IcebergType.StringType _ ->
                new Value.StringVal(new String(bytes.toArray(JAVA_BYTE), StandardCharsets.UTF_8));
            case IcebergType.UuidType _ -> new Value.UuidVal(readUuid(bytes));
            case IcebergType.TimestampType(boolean adjustToUtc, TimeUnit unit) ->
                new Value.TimestampVal(epochToLocalDateTime(readLeLong(bytes), unit), adjustToUtc);
            case IcebergType.TimeType _ -> new Value.TimeVal(microsToLocalTime(readLeLong(bytes)));
            case IcebergType.DecimalType(int _, int scale) ->
                new Value.DecimalVal(decimalOf(bytes.toArray(JAVA_BYTE), scale));
            case IcebergType.BinaryType _, IcebergType.FixedType _ -> new Value.BinaryVal(bytes);
            case IcebergType.GeometryType _, IcebergType.GeographyType _ ->
                throw new IcebergFormatException("a geometry bound is a point, not a scalar value: " + type.token());
            case IcebergType.UnknownType _ -> throw new IcebergFormatException("an unknown-typed column has no bound");
        };
    }

    /**
     * Decodes one cell of a data file read through its physical {@code path}, as an equality-delete tuple component.
     * Binary cells are copied: the tuple outlives the batch viewed by the row. A decimal cell decodes from the physical
     * type chosen by the writer (INT32, INT64, or fixed-length bytes).
     *
     * <p>The cell at {@code path} must be non-null; the caller tests {@link ParquetRecord#isNull(ColumnPath)} first and
     * handles a null cell itself. A null cell never decodes cleanly, and it goes wrong differently per arm. The
     * numeric, boolean, date, time and timestamp arms raise {@link IllegalStateException} from the column vector, the
     * binary and decimal arms raise {@link NullPointerException}, and the string and uuid arms wrap a null with no
     * error, which would then stand in the tuple as a real component and delete the wrong rows.
     */
    static Value fromCell(IcebergType type, ParquetRecord row, ColumnPath path) {
        return switch (type) {
            case IcebergType.BoolType _ -> new Value.BoolVal(row.getBoolean(path));
            case IcebergType.IntType _ -> new Value.IntVal(row.getInt(path));
            case IcebergType.LongType _ -> new Value.LongVal(row.getLong(path));
            case IcebergType.FloatType _ -> new Value.FloatVal(row.getFloat(path));
            case IcebergType.DoubleType _ -> new Value.DoubleVal(row.getDouble(path));
            case IcebergType.DateType _ -> new Value.DateVal(LocalDate.ofEpochDay(row.getInt(path)));
            case IcebergType.StringType _ -> new Value.StringVal(row.getString(path));
            case IcebergType.UuidType _ -> new Value.UuidVal(row.getUuid(path));
            case IcebergType.TimestampType(boolean adjustToUtc, TimeUnit unit) ->
                new Value.TimestampVal(epochToLocalDateTime(row.getLong(path), unit), adjustToUtc);
            case IcebergType.TimeType _ -> new Value.TimeVal(microsToLocalTime(row.getLong(path)));
            case IcebergType.DecimalType(int _, int scale) -> new Value.DecimalVal(decimalCell(row, path, scale));
            case IcebergType.BinaryType _, IcebergType.FixedType _ ->
                new Value.BinaryVal(MemorySegment.ofArray(row.getBinary(path)));
            case IcebergType.GeometryType _, IcebergType.GeographyType _, IcebergType.UnknownType _ ->
                throw new IcebergFormatException(
                        "cannot read equality delete column %s of type %s".formatted(path.dot(), type.token()));
        };
    }

    /**
     * A decimal cell is its unscaled value in the physical type chosen by the writer: Iceberg stores up to nine digits
     * as INT32, up to eighteen as INT64, and wider decimals as fixed-length two's-complement bytes.
     */
    private static BigDecimal decimalCell(ParquetRecord row, ColumnPath path, int scale) {
        return switch (row.get(path)) {
            case Integer unscaled -> BigDecimal.valueOf(unscaled, scale);
            case Long unscaled -> BigDecimal.valueOf(unscaled, scale);
            case MemorySegment bytes -> decimalOf(bytes.toArray(JAVA_BYTE), scale);
            case Object other ->
                throw new IcebergFormatException("equality delete column %s holds %s, not a decimal cell"
                        .formatted(path.dot(), other.getClass().getSimpleName()));
        };
    }

    /** Decodes an {@code initial-default} from Iceberg's JSON single-value serialization of {@code type}. */
    static Value fromJson(IcebergType type, JsonNode node, String fieldName) {
        return switch (type) {
            case IcebergType.BoolType _ -> new Value.BoolVal(booleanOf(node, type, fieldName));
            case IcebergType.IntType _ -> new Value.IntVal(numberOf(node, type, fieldName, node::intValue));
            case IcebergType.LongType _ -> new Value.LongVal(numberOf(node, type, fieldName, node::longValue));
            case IcebergType.FloatType _ -> new Value.FloatVal(numberOf(node, type, fieldName, node::floatValue));
            case IcebergType.DoubleType _ -> new Value.DoubleVal(numberOf(node, type, fieldName, node::doubleValue));
            case IcebergType.DateType _ ->
                new Value.DateVal(parsed(() -> LocalDate.parse(textOf(node, type, fieldName)), type, fieldName));
            case IcebergType.StringType _ -> new Value.StringVal(textOf(node, type, fieldName));
            case IcebergType.UuidType _ ->
                new Value.UuidVal(parsed(() -> UUID.fromString(textOf(node, type, fieldName)), type, fieldName));
            case IcebergType.TimestampType(boolean adjustToUtc, TimeUnit unit) ->
                timestampDefault(node, type, fieldName, adjustToUtc, unit);
            case IcebergType.TimeType _ ->
                new Value.TimeVal(parsed(() -> LocalTime.parse(textOf(node, type, fieldName)), type, fieldName));
            case IcebergType.DecimalType(int _, int scale) ->
                new Value.DecimalVal(decimalDefault(textOf(node, type, fieldName), scale, type, fieldName));
            case IcebergType.BinaryType _, IcebergType.FixedType _ ->
                new Value.BinaryVal(MemorySegment.ofArray(
                        parsed(() -> HexFormat.of().parseHex(textOf(node, type, fieldName)), type, fieldName)));
            case IcebergType.GeometryType _, IcebergType.GeographyType _, IcebergType.UnknownType _ ->
                throw new IcebergFormatException(
                        "cannot read initial-default for field %s of type %s".formatted(fieldName, type.token()));
        };
    }

    /**
     * Converts a raw partition-tuple value read from a manifest (an Avro datum with its logical type applied).
     *
     * <p>{@code raw} must be non-null; the caller skips a null tuple slot and never calls this for one.
     */
    static Value fromPartition(IcebergType type, Object raw, String columnName) {
        return switch (type) {
            case IcebergType.BoolType _ -> new Value.BoolVal(as(Boolean.class, raw, type, columnName));
            case IcebergType.IntType _ ->
                new Value.IntVal(as(Number.class, raw, type, columnName).intValue());
            case IcebergType.LongType _ ->
                new Value.LongVal(as(Number.class, raw, type, columnName).longValue());
            case IcebergType.FloatType _ ->
                new Value.FloatVal(as(Number.class, raw, type, columnName).floatValue());
            case IcebergType.DoubleType _ ->
                new Value.DoubleVal(as(Number.class, raw, type, columnName).doubleValue());
            case IcebergType.DateType _ -> new Value.DateVal(partitionDate(raw, type, columnName));
            case IcebergType.StringType _ -> new Value.StringVal(raw.toString());
            case IcebergType.UuidType _ -> new Value.UuidVal(partitionUuid(raw, type, columnName));
            case IcebergType.TimestampType(boolean adjustToUtc, TimeUnit unit) ->
                new Value.TimestampVal(partitionTimestamp(raw, unit, type, columnName), adjustToUtc);
            case IcebergType.TimeType _ -> new Value.TimeVal(partitionTime(raw, type, columnName));
            case IcebergType.DecimalType(int _, int scale) ->
                new Value.DecimalVal(partitionDecimal(raw, scale, type, columnName));
            case IcebergType.BinaryType _, IcebergType.FixedType _ ->
                new Value.BinaryVal(segmentOf(raw, type, columnName));
            case IcebergType.GeometryType _, IcebergType.GeographyType _, IcebergType.UnknownType _ ->
                throw new IcebergFormatException("cannot reconstruct identity partition column %s of type %s"
                        .formatted(columnName, type.token()));
        };
    }

    /**
     * The archetype whose kind selects the all-null vector for an added column of {@code type}; the value itself is
     * ignored. A timestamp null-fills as a plain INT64 column. An added geometry column cannot be null-filled.
     */
    static Value nullValue(IcebergType type, String fieldName) {
        return switch (type) {
            case IcebergType.BoolType _ -> new Value.BoolVal(false);
            case IcebergType.IntType _ -> new Value.IntVal(0);
            case IcebergType.LongType _ -> new Value.LongVal(0L);
            case IcebergType.FloatType _ -> new Value.FloatVal(0f);
            case IcebergType.DoubleType _ -> new Value.DoubleVal(0d);
            case IcebergType.DateType _ -> new Value.DateVal(LocalDate.EPOCH);
            case IcebergType.StringType _ -> new Value.StringVal("");
            case IcebergType.UuidType _ -> new Value.UuidVal(new UUID(0L, 0L));
            case IcebergType.TimestampType _ -> new Value.LongVal(0L);
            case IcebergType.TimeType _ -> new Value.TimeVal(LocalTime.MIDNIGHT);
            case IcebergType.DecimalType(int precision, int scale) ->
                new Value.DecimalVal(decimalArchetype(precision, scale));
            case IcebergType.BinaryType _, IcebergType.FixedType _ ->
                new Value.BinaryVal(MemorySegment.ofArray(new byte[0]));
            case IcebergType.UnknownType _ -> new Value.IntVal(0);
            case IcebergType.GeometryType _, IcebergType.GeographyType _ ->
                throw new IcebergFormatException(
                        "cannot inject a null column for added field %s of type %s".formatted(fieldName, type.token()));
        };
    }

    /**
     * The archetype for an added decimal column of {@code precision} digits at {@code scale}: the smallest value
     * spelling out all of them. The digits themselves are never read, and the leaf chosen for the null column takes its
     * precision from this value. A zero would understate the table precision as a single digit, which at any scale
     * above one is an illegal Parquet decimal.
     */
    private static BigDecimal decimalArchetype(int precision, int scale) {
        return new BigDecimal(BigInteger.TEN.pow(precision - 1), scale);
    }

    private static Value timestampDefault(
            JsonNode node, IcebergType type, String fieldName, boolean adjustToUtc, TimeUnit unit) {
        if (unit == TimeUnit.NANOS) {
            // A constant column is materialized at microsecond precision; presenting a nanosecond column through
            // it would misstate every value by a factor of a thousand.
            throw new IcebergFormatException(
                    "initial-default on nanosecond timestamp field %s is not supported".formatted(fieldName));
        }
        String text = textOf(node, type, fieldName);
        LocalDateTime value = parsed(() -> parseTimestamp(text, adjustToUtc), type, fieldName);
        return new Value.TimestampVal(value, adjustToUtc);
    }

    /** A UTC-adjusted timestamp default is an offset date-time; an unadjusted one has no offset at all. */
    private static LocalDateTime parseTimestamp(String text, boolean adjustToUtc) {
        if (!adjustToUtc) {
            return LocalDateTime.parse(text);
        }
        OffsetDateTime offset = OffsetDateTime.parse(text);
        return offset.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime();
    }

    private static BigDecimal decimalDefault(String text, int scale, IcebergType type, String fieldName) {
        BigDecimal value = parsed(() -> new BigDecimal(text), type, fieldName);
        if (value.scale() != scale) {
            throw new IcebergFormatException("initial-default for field %s has scale %d, its type %s has scale %d"
                    .formatted(fieldName, value.scale(), type.token(), scale));
        }
        return value;
    }

    private static <T> T parsed(Supplier<T> parser, IcebergType type, String fieldName) {
        try {
            return parser.get();
        } catch (DateTimeException | IllegalArgumentException e) {
            throw new IcebergFormatException(
                    "initial-default for field %s is not a valid %s: %s"
                            .formatted(fieldName, type.token(), e.getMessage()),
                    e);
        }
    }

    private static String textOf(JsonNode node, IcebergType type, String fieldName) {
        if (!node.isString()) {
            throw wrongJsonShape(node, type, fieldName, "a JSON string");
        }
        return node.stringValue();
    }

    /**
     * The number a JSON default holds, converted by {@code accessor} to the Java type of the field. Jackson's typed
     * accessors refuse any number that the target type cannot hold exactly: one outside its range, or one with a
     * fractional part on an integral field. A lenient narrowing would store a number other than the one that the table
     * declares, and the reader would answer every query on that column with it.
     */
    private static <T> T numberOf(JsonNode node, IcebergType type, String fieldName, Supplier<T> accessor) {
        if (!node.isNumber()) {
            throw wrongJsonShape(node, type, fieldName, "a JSON number");
        }
        return converted(accessor, type, fieldName);
    }

    private static <T> T converted(Supplier<T> accessor, IcebergType type, String fieldName) {
        try {
            return accessor.get();
        } catch (JsonNodeException e) {
            throw new IcebergFormatException(
                    "initial-default for field %s does not fit type %s: %s"
                            .formatted(fieldName, type.token(), e.getMessage()),
                    e);
        }
    }

    private static boolean booleanOf(JsonNode node, IcebergType type, String fieldName) {
        if (!node.isBoolean()) {
            throw wrongJsonShape(node, type, fieldName, "a JSON boolean");
        }
        return node.booleanValue();
    }

    private static IcebergFormatException wrongJsonShape(
            JsonNode node, IcebergType type, String fieldName, String expected) {
        return new IcebergFormatException("initial-default for field %s of type %s must be %s, got %s"
                .formatted(fieldName, type.token(), expected, node));
    }

    private static LocalDate partitionDate(Object raw, IcebergType type, String columnName) {
        return switch (raw) {
            case LocalDate date -> date;
            case Number days -> LocalDate.ofEpochDay(days.longValue());
            default -> throw unconvertible(raw, type, columnName);
        };
    }

    private static UUID partitionUuid(Object raw, IcebergType type, String columnName) {
        return switch (raw) {
            case UUID uuid -> uuid;
            case MemorySegment segment -> readUuid(segment);
            case byte[] bytes -> readUuid(MemorySegment.ofArray(bytes));
            default -> throw unconvertible(raw, type, columnName);
        };
    }

    private static LocalDateTime partitionTimestamp(Object raw, TimeUnit unit, IcebergType type, String columnName) {
        return switch (raw) {
            case Instant instant -> LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
            case LocalDateTime local -> local;
            case Number value -> epochToLocalDateTime(value.longValue(), unit);
            default -> throw unconvertible(raw, type, columnName);
        };
    }

    private static LocalTime partitionTime(Object raw, IcebergType type, String columnName) {
        return switch (raw) {
            case LocalTime time -> time;
            case Number micros -> microsToLocalTime(micros.longValue());
            default -> throw unconvertible(raw, type, columnName);
        };
    }

    private static BigDecimal partitionDecimal(Object raw, int scale, IcebergType type, String columnName) {
        return switch (raw) {
            case BigDecimal decimal -> rescaledExactly(decimal, scale, columnName);
            case MemorySegment segment -> decimalOf(segment.toArray(JAVA_BYTE), scale);
            case byte[] bytes -> decimalOf(bytes, scale);
            default -> throw unconvertible(raw, type, columnName);
        };
    }

    private static BigDecimal rescaledExactly(BigDecimal decimal, int scale, String columnName) {
        try {
            return decimal.setScale(scale);
        } catch (ArithmeticException e) {
            throw new IcebergFormatException(
                    "partition value %s of column %s does not fit scale %d".formatted(decimal, columnName, scale), e);
        }
    }

    private static MemorySegment segmentOf(Object raw, IcebergType type, String columnName) {
        return switch (raw) {
            case MemorySegment segment -> segment;
            case byte[] bytes -> MemorySegment.ofArray(bytes);
            default -> throw unconvertible(raw, type, columnName);
        };
    }

    private static <T> T as(Class<T> expected, Object raw, IcebergType type, String columnName) {
        if (expected.isInstance(raw)) {
            return expected.cast(raw);
        }
        throw unconvertible(raw, type, columnName);
    }

    private static IcebergFormatException unconvertible(Object raw, IcebergType type, String columnName) {
        return new IcebergFormatException("partition value of column %s (%s) has an unexpected shape: %s"
                .formatted(columnName, type.token(), raw.getClass().getName()));
    }

    private static LocalDateTime epochToLocalDateTime(long value, TimeUnit unit) {
        long perSecond = unit == TimeUnit.NANOS ? 1_000_000_000L : 1_000_000L;
        long nanosPerUnit = unit == TimeUnit.NANOS ? 1L : 1_000L;
        long seconds = Math.floorDiv(value, perSecond);
        int nanoOfSecond = (int) (Math.floorMod(value, perSecond) * nanosPerUnit);
        try {
            return LocalDateTime.ofEpochSecond(seconds, nanoOfSecond, ZoneOffset.UTC);
        } catch (DateTimeException e) {
            throw new IcebergFormatException("timestamp value out of range: " + value, e);
        }
    }

    private static LocalTime microsToLocalTime(long micros) {
        if (micros < 0 || micros >= MICROS_PER_DAY) {
            throw new IcebergFormatException("time value out of day range: " + micros);
        }
        return LocalTime.ofNanoOfDay(micros * 1_000L);
    }

    /** A decimal's unscaled value is a big-endian two's-complement integer of any width. */
    private static BigDecimal decimalOf(byte[] unscaledBigEndian, int scale) {
        if (unscaledBigEndian.length == 0) {
            throw new IcebergFormatException("empty decimal value");
        }
        return new BigDecimal(new BigInteger(unscaledBigEndian), scale);
    }

    private static int readLeInt(MemorySegment bytes) {
        requireLength(bytes, 4, "4-byte");
        return bytes.get(LE_INT, 0);
    }

    private static long readLeLong(MemorySegment bytes) {
        requireLength(bytes, 8, "8-byte");
        return bytes.get(LE_LONG, 0);
    }

    private static boolean readBoolean(MemorySegment bytes) {
        requireLength(bytes, 1, "1-byte");
        return bytes.get(JAVA_BYTE, 0) != 0;
    }

    private static UUID readUuid(MemorySegment bytes) {
        requireLength(bytes, UuidConverter.BYTES, "16-byte");
        return UuidConverter.fromSegment(bytes);
    }

    private static void requireLength(MemorySegment bytes, long expected, String description) {
        if (bytes.byteSize() < expected) {
            throw new IcebergFormatException("value too short: expected at least a " + description + " value but got "
                    + bytes.byteSize() + " bytes");
        }
    }
}
