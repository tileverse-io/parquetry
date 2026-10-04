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

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.FLOAT;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.util.Optional;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Decodes a Parquet PLAIN-encoded statistic bound (a row-group {@link io.tileverse.parquetry.format.Statistics} min/max
 * or a column-index page bound) into a typed {@link Value}, honoring the column's logical type annotation:
 *
 * <ul>
 *   <li>INT64 + {@link LogicalType.Timestamp} decodes to {@link Value.TimestampVal} at the column's time unit.
 *   <li>INT64 + {@link LogicalType.Time} decodes to {@link Value.TimeVal} at the column's time unit.
 *   <li>INT32 or INT64 + {@link LogicalType.Decimal} decodes to a {@link Value.DecimalVal} of the unscaled integer at
 *       the column's scale.
 *   <li>FIXED_LEN_BYTE_ARRAY + {@link LogicalType.Decimal} decodes to {@link Value.DecimalVal} at the column's scale,
 *       interpreting the bytes as a signed big-endian two's-complement integer.
 * </ul>
 *
 * All other combinations follow the physical kind and decode to the corresponding primitive {@link Value} subtype
 * ({@link Value.BoolVal}, {@link Value.IntVal}, {@link Value.LongVal}, etc.). Plain BYTE_ARRAY and non-decimal FLBA
 * decode to {@link Value.BinaryVal} for unsigned lexicographic comparison.
 *
 * <p>A bound decodes only when its order matches the order used by the filter to compare the column's cells. A bound in
 * another order decodes to nothing, keeping the column out of min/max pruning.
 *
 * <p>The reflexive comparison arms in {@link ValueComparison#compareValues} then resolve pruning correctly once both
 * the predicate value and the decoded bound are of the same typed subtype.
 */
public final class StatisticsValueDecoder {

    private StatisticsValueDecoder() {}

    /**
     * Decodes {@code raw} to a {@link Value} using {@code kind} for the physical layout and {@code logicalType} for the
     * semantic type. Returns empty for kinds that have no statistics path (INT96), for bounds ordered unlike the
     * compared cells of the column, and for segments too short to hold the required bytes.
     *
     * <p>The decoded value shares no memory with {@code raw}. Callers keep a bound long after the form that it was read
     * from has been released: a catalog holds one min and one max per file column for the lifetime of its store, while
     * the packed footer behind them is cached and evicted on its own schedule.
     */
    public static Optional<Value> decode(PrimitiveKind kind, Optional<LogicalType> logicalType, MemorySegment raw) {
        if (!orderedAsCompared(kind, logicalType)) {
            return Optional.empty();
        }
        long size = raw.byteSize();
        return switch (kind) {
            case BOOLEAN -> size >= 1 ? Optional.of(new Value.BoolVal(raw.get(JAVA_BYTE, 0) != 0)) : Optional.empty();
            case INT32 -> size >= 4 ? decodeInt32(logicalType, raw.get(INT32, 0)) : Optional.empty();
            case INT64 -> size >= 8 ? decodeInt64(logicalType, raw.get(INT64, 0)) : Optional.empty();
            case FLOAT -> size >= 4 ? Optional.of(new Value.FloatVal(raw.get(FLOAT, 0))) : Optional.empty();
            case DOUBLE -> size >= 8 ? Optional.of(new Value.DoubleVal(raw.get(DOUBLE, 0))) : Optional.empty();
            case FIXED_LEN_BYTE_ARRAY -> decodeFlba(logicalType, raw);
            case BYTE_ARRAY -> Optional.of(binaryCopyOf(raw));
            // INT96 is a legacy 12-byte timestamp with no defined statistics ordering; skip it.
            case INT96 -> Optional.empty();
        };
    }

    /**
     * Whether the statistics of a {@code kind} column annotated {@code logicalType} follow the order used by the filter
     * to compare the column's cells. A writer orders the bounds by the annotation, and a bound compared in another
     * order lets a pruning tier drop matching rows: a half float or a wide timestamp read as unsigned bytes, a binary
     * decimal read as unsigned bytes, a full-width unsigned integer read as a signed one. An annotation not defined for
     * the physical type, or one with an order left undefined by the format, takes its statistics out of pruning too.
     */
    private static boolean orderedAsCompared(PrimitiveKind kind, Optional<LogicalType> logicalType) {
        if (logicalType.isEmpty()) {
            return true;
        }
        return switch (logicalType.orElseThrow()) {
            case LogicalType.Decimal _ ->
                kind == PrimitiveKind.INT32
                        || kind == PrimitiveKind.INT64
                        || kind == PrimitiveKind.FIXED_LEN_BYTE_ARRAY;
            case LogicalType.StringType _, LogicalType.EnumType _, LogicalType.JsonType _, LogicalType.BsonType _ ->
                kind == PrimitiveKind.BYTE_ARRAY;
            case LogicalType.UuidType _ -> kind == PrimitiveKind.FIXED_LEN_BYTE_ARRAY;
            case LogicalType.DateType _ -> kind == PrimitiveKind.INT32;
            case LogicalType.Time _ -> kind == PrimitiveKind.INT32 || kind == PrimitiveKind.INT64;
            case LogicalType.Timestamp _ -> kind == PrimitiveKind.INT64;
            case LogicalType.IntType intType -> integerOrderedAsCompared(kind, intType);
            case LogicalType.UnknownType _ -> true;
            // A half float is ordered by the value it represents; the format defines no order for the others.
            case LogicalType.Float16Type _,
                    LogicalType.Geometry _,
                    LogicalType.Geography _,
                    LogicalType.MapType _,
                    LogicalType.ListType _,
                    LogicalType.Variant _ -> false;
        };
    }

    /**
     * Integer cells compare as signed values. An unsigned annotation agrees with that order only when its width leaves
     * the physical sign bit clear.
     */
    private static boolean integerOrderedAsCompared(PrimitiveKind kind, LogicalType.IntType intType) {
        if (kind != PrimitiveKind.INT32 && kind != PrimitiveKind.INT64) {
            return false;
        }
        int physicalBits = kind == PrimitiveKind.INT32 ? Integer.SIZE : Long.SIZE;
        return intType.isSigned() || intType.bitWidth() < physicalBits;
    }

    private static Optional<Value> decodeInt32(Optional<LogicalType> logicalType, int value) {
        if (logicalType.orElse(null) instanceof LogicalType.Decimal(int scale, int _)) {
            return Optional.of(new Value.DecimalVal(BigDecimal.valueOf(value, scale)));
        }
        return Optional.of(new Value.IntVal(value));
    }

    private static Optional<Value> decodeInt64(Optional<LogicalType> logicalType, long value) {
        LogicalType leaf = logicalType.orElse(null);
        if (leaf instanceof LogicalType.Decimal(int scale, int _)) {
            return Optional.of(new Value.DecimalVal(BigDecimal.valueOf(value, scale)));
        }
        if (leaf instanceof LogicalType.Timestamp(boolean adjusted, LogicalType.TimeUnit unit)) {
            return Optional.of(new Value.TimestampVal(TemporalValues.toLocalDateTime(value, unit), adjusted));
        }
        if (leaf instanceof LogicalType.Time(boolean _, LogicalType.TimeUnit unit)) {
            return Optional.of(new Value.TimeVal(TemporalValues.toLocalTime(value, unit)));
        }
        return Optional.of(new Value.LongVal(value));
    }

    private static Optional<Value> decodeFlba(Optional<LogicalType> logicalType, MemorySegment raw) {
        if (logicalType.orElse(null) instanceof LogicalType.Decimal(int scale, int _)) {
            if (raw.byteSize() == 0) {
                // An empty decimal statistic has no usable numeric bound; decline to prune rather than
                // decode a bogus value or fall to an unsigned byte compare.
                return Optional.empty();
            }
            return Optional.of(new Value.DecimalVal(DecimalValues.toBigDecimal(raw, scale)));
        }
        return Optional.of(binaryCopyOf(raw));
    }

    /**
     * A binary bound over this decoder's own copy of {@code raw}. The copy lets the memory behind {@code raw} - a
     * packed footer, a column index page - be released while a caller still holds the bound.
     */
    private static Value binaryCopyOf(MemorySegment raw) {
        byte[] bytes = raw.toArray(JAVA_BYTE);
        return new Value.BinaryVal(MemorySegment.ofArray(bytes));
    }
}
