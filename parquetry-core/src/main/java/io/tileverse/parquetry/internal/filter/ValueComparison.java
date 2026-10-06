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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.UuidConverter;

/**
 * Shared comparison and ordering core for the filter evaluators. The record-level, vectorized, dictionary, statistics,
 * and column-index evaluators route their value comparisons through here, keeping one definition of type coercion,
 * Parquet's unsigned binary ordering, and the IEEE 754 rules for floating-point values.
 */
public final class ValueComparison {

    private ValueComparison() {}

    /**
     * Whether {@code op} holds between a record cell and a predicate literal. A null cell matches no comparison. A
     * floating-point cell against a floating-point literal follows {@link ComparisonOperator#holds(double, double)};
     * the other pairs follow {@link #compareBoxed}.
     */
    public static boolean holds(ComparisonOperator op, Object cell, Value literal) {
        if (cell == null) {
            return false;
        }
        if ((cell instanceof Float || cell instanceof Double) && isFloating(literal)) {
            double value = ((Number) cell).doubleValue();
            return op.holds(value, floatingValue(literal));
        }
        return op.holds(compareBoxed(cell, literal));
    }

    /** Whether {@code literal} is a {@link Value.FloatVal} or a {@link Value.DoubleVal}. */
    public static boolean isFloating(Value literal) {
        return literal instanceof Value.FloatVal || literal instanceof Value.DoubleVal;
    }

    /** Whether {@code literal} is a floating-point NaN. */
    public static boolean isNaN(Value literal) {
        return isFloating(literal) && Double.isNaN(floatingValue(literal));
    }

    /**
     * The value of a floating-point literal, a float widened to double exactly. A column scan reads it once and
     * compares each cell through {@link ComparisonOperator#holds(double, double)}.
     *
     * @throws IllegalArgumentException when {@code literal} is not {@link #isFloating floating-point}
     */
    public static double floatingValue(Value literal) {
        if (literal instanceof Value.FloatVal(float value)) {
            return value;
        }
        if (literal instanceof Value.DoubleVal(double value)) {
            return value;
        }
        throw notFloating(literal);
    }

    /** Built apart from {@link #floatingValue}, keeping the message construction out of a per-cell method. */
    private static IllegalArgumentException notFloating(Value literal) {
        return new IllegalArgumentException("not a floating-point literal: " + literal);
    }

    /**
     * Compares a boxed record value (Boolean/Integer/Long/Float/Double/String/MemorySegment/LocalDate/LocalDateTime)
     * against a predicate-side {@link Value}. Returns 0 for unknown type combinations; the surrounding evaluator treats
     * that as equality, but with NULL filtered upstream the typical path is well-typed.
     */
    // S7475 (bare _ in nested record patterns) is informational only - palantirJavaFormat 2.90 cannot
    // parse the bare-underscore form Sonar suggests; see memory feedback-palantir-unnamed-pattern.
    @SuppressWarnings({"java:S3776", "java:S7475"})
    public static int compareBoxed(Object actual, Value bound) {
        if (actual == null) {
            return -1; // any comparison vs null returns "less than", but the caller already short-circuited on null
        }
        return switch (bound) {
            case Value.BoolVal(boolean bv) when actual instanceof Boolean av -> Boolean.compare(av, bv);
            case Value.IntVal(int bv) when actual instanceof Integer av -> Integer.compare(av, bv);
            case Value.LongVal(long bv) when actual instanceof Long av -> Long.compare(av, bv);
            // Int and long widen to each other for Iceberg's lossless int-to-long promotion: a long-column actual
            // bound against an int predicate, and an int-column actual bound against a long predicate.
            case Value.IntVal(int bv) when actual instanceof Long av -> Long.compare(av, bv);
            case Value.LongVal(long bv) when actual instanceof Integer av -> Long.compare(av, bv);
            case Value.FloatVal(float bv) when actual instanceof Float av -> compareFloating(av, bv);
            case Value.DoubleVal(double bv) when actual instanceof Double av -> compareFloating(av, bv);
            case Value.DoubleVal(double bv) when actual instanceof Float av -> compareFloating(av, bv);
            case Value.FloatVal(float bv) when actual instanceof Double av -> compareFloating(av, bv);
            case Value.StringVal(String bv) when actual instanceof String av -> av.compareTo(bv);
            case Value.StringVal(String bv)
            when actual instanceof MemorySegment av ->
                compareBytes(av, MemorySegment.ofArray(bv.getBytes(StandardCharsets.UTF_8)));
            case Value.BinaryVal(MemorySegment bv) when actual instanceof MemorySegment av -> compareBytes(av, bv);
            case Value.BinaryVal(MemorySegment bv)
            when actual instanceof String av ->
                compareBytes(MemorySegment.ofArray(av.getBytes(StandardCharsets.UTF_8)), bv);
            case Value.DateVal(LocalDate bv) when actual instanceof LocalDate av -> av.compareTo(bv);
            case Value.DateVal(LocalDate bv)
            when actual instanceof Integer av -> Integer.compare(av, (int) bv.toEpochDay());
            case Value.TimestampVal(LocalDateTime bv, boolean _)
            when actual instanceof LocalDateTime av -> av.compareTo(bv);
            case Value.TimestampVal(LocalDateTime bv, boolean _)
            when actual instanceof MemorySegment av && isInt96Cell(av) -> Int96Timestamps.compare(av, bv);
            case Value.DecimalVal(BigDecimal bv) when actual instanceof BigDecimal av -> av.compareTo(bv);
            case Value.TimeVal(LocalTime bv) when actual instanceof LocalTime av -> av.compareTo(bv);
            case Value.UuidVal(UUID bv) when actual instanceof MemorySegment av -> compareSegmentToUuidValue(av, bv);
            default -> 0;
        };
    }

    /**
     * Compares the predicate-side value to a decoded bound value (the statistics path: query vs decoded min/max).
     * Returns negative if {@code query < bound}, zero if equal, positive if {@code query > bound}. Returns 0 for type
     * mismatches (the caller already had its chance via the schema validator).
     */
    // S3776: dispatch table; cyclomatic complexity is inherent.
    // S7475: palantirJavaFormat 2.90 cannot parse bare _ in nested record patterns; see memory
    // feedback-palantir-unnamed-pattern.
    @SuppressWarnings({"java:S3776", "java:S7475"})
    public static int compareValues(Value query, Value bound) {
        return switch (query) {
            case Value.BoolVal(boolean qv) when bound instanceof Value.BoolVal(boolean bv) -> Boolean.compare(qv, bv);
            case Value.IntVal(int qv) when bound instanceof Value.IntVal(int bv) -> Integer.compare(qv, bv);
            case Value.LongVal(long qv) when bound instanceof Value.LongVal(long bv) -> Long.compare(qv, bv);
            // Int and long widen to each other for Iceberg's lossless int-to-long promotion.
            case Value.IntVal(int qv) when bound instanceof Value.LongVal(long bv) -> Long.compare(qv, bv);
            case Value.LongVal(long qv) when bound instanceof Value.IntVal(int bv) -> Long.compare(qv, bv);
            case Value.FloatVal(float qv) when bound instanceof Value.FloatVal(float bv) -> compareFloating(qv, bv);
            case Value.DoubleVal(double qv) when bound instanceof Value.DoubleVal(double bv) -> compareFloating(qv, bv);
            case Value.DoubleVal(double qv) when bound instanceof Value.FloatVal(float bv) -> compareFloating(qv, bv);
            case Value.FloatVal(float qv) when bound instanceof Value.DoubleVal(double bv) -> compareFloating(qv, bv);
            case Value.StringVal(String qv)
            when bound instanceof Value.BinaryVal(MemorySegment bv) ->
                compareBytes(MemorySegment.ofArray(qv.getBytes(StandardCharsets.UTF_8)), bv);
            // Same-typed pairs arise when both sides are produced as typed values (the Iceberg manifest-bound
            // path), not the Parquet-statistics path that decodes binary columns to BinaryVal. Unsigned byte
            // ordering keeps these in agreement with Parquet's default binary ColumnOrder and with file statistics.
            case Value.StringVal(String qv)
            when bound instanceof Value.StringVal(String bv) ->
                compareBytes(
                        MemorySegment.ofArray(qv.getBytes(StandardCharsets.UTF_8)),
                        MemorySegment.ofArray(bv.getBytes(StandardCharsets.UTF_8)));
            case Value.BinaryVal(MemorySegment qv)
            when bound instanceof Value.BinaryVal(MemorySegment bv) -> compareBytes(qv, bv);
            case Value.DateVal(LocalDate qv)
            when bound instanceof Value.IntVal(int bv) -> Integer.compare((int) qv.toEpochDay(), bv);
            case Value.DateVal(LocalDate qv) when bound instanceof Value.DateVal(LocalDate bv) -> qv.compareTo(bv);
            case Value.DecimalVal(BigDecimal qv)
            when bound instanceof Value.DecimalVal(BigDecimal bv) -> qv.compareTo(bv);
            case Value.TimeVal(LocalTime qv) when bound instanceof Value.TimeVal(LocalTime bv) -> qv.compareTo(bv);
            case Value.TimestampVal(LocalDateTime qv, boolean _)
            when bound instanceof Value.TimestampVal(LocalDateTime bv, boolean _) -> qv.compareTo(bv);
            case Value.UuidVal(UUID qv)
            when bound instanceof Value.BinaryVal(MemorySegment bv) -> -compareSegmentToUuidValue(bv, qv);
            case Value.UuidVal(UUID qv)
            when bound instanceof Value.UuidVal(UUID bv) ->
                compareSegmentToUuidValue(UuidConverter.toReadOnlySegment(qv), bv);
            default -> 0;
        };
    }

    /**
     * Whether {@code min} orders after {@code max}. Such a pair bounds nothing: it is left by a writer ordering the
     * values of the column unlike the column's type, and a value between the true bounds can lie outside it.
     */
    static boolean inverted(Value min, Value max) {
        return compareValues(min, max) > 0;
    }

    /**
     * Compares a primitive {@code int} actual value against a predicate-side {@link Value}, without boxing. Used by the
     * vectorized evaluator's typed scan. Agrees with the {@link Value.IntVal} and {@link Value.DateVal} arms of
     * {@link #compareBoxed}. Returns 0 for unknown bound kinds.
     */
    public static int compareInt(int actual, Value bound) {
        return switch (bound) {
            case Value.IntVal(int bv) -> Integer.compare(actual, bv);
            // A long-valued predicate reaches an INT32 column under Iceberg's lossless int-to-long promotion.
            case Value.LongVal(long bv) -> Long.compare(actual, bv);
            case Value.DateVal(LocalDate bv) -> Integer.compare(actual, (int) bv.toEpochDay());
            default -> 0;
        };
    }

    /** Compares a primitive {@code long} against a {@link Value} without boxing; agrees with {@link #compareBoxed}. */
    public static int compareLong(long actual, Value bound) {
        return switch (bound) {
            case Value.LongVal(long bv) -> Long.compare(actual, bv);
            // An int-valued predicate reaches an INT64 column under Iceberg's lossless int-to-long promotion.
            case Value.IntVal(int bv) -> Long.compare(actual, bv);
            default -> 0;
        };
    }

    /**
     * Compares a primitive {@code boolean} against a {@link Value} without boxing; agrees with {@link #compareBoxed}.
     */
    public static int compareBoolean(boolean actual, Value bound) {
        return bound instanceof Value.BoolVal(boolean bv) ? Boolean.compare(actual, bv) : 0;
    }

    /**
     * Compares a binary {@link MemorySegment} actual value against a predicate-side {@link Value}, mirroring the
     * {@link Value.BinaryVal}, {@link Value.StringVal}, {@link Value.UuidVal} and INT96 {@link Value.TimestampVal} arms
     * of {@link #compareBoxed}. Returns 0 for unknown bound kinds.
     */
    @SuppressWarnings("java:S7475") // palantirJavaFormat 2.90 cannot parse bare _ in nested record patterns
    public static int compareBinary(MemorySegment actual, Value bound) {
        return switch (bound) {
            case Value.BinaryVal(MemorySegment bv) -> compareBytes(actual, bv);
            case Value.StringVal(String bv) ->
                compareBytes(actual, MemorySegment.ofArray(bv.getBytes(StandardCharsets.UTF_8)));
            case Value.UuidVal(UUID bv) -> compareSegmentToUuidValue(actual, bv);
            case Value.TimestampVal(LocalDateTime bv, boolean _)
            when isInt96Cell(actual) -> Int96Timestamps.compare(actual, bv);
            default -> 0;
        };
    }

    /**
     * Whether a binary cell compared against a timestamp literal is an INT96 timestamp. A timestamp predicate reaches a
     * binary cell only on an INT96 column, and the width check keeps a malformed cell from being read past its end.
     */
    private static boolean isInt96Cell(MemorySegment cell) {
        return cell.byteSize() == Int96Timestamps.CELL_BYTES;
    }

    /**
     * Compares a binary segment against a UUID, tolerating a binary segment that is not exactly 16 bytes. The normal
     * case takes the allocation-free fast path; a short or oversized segment (such as a truncated FLBA row-group
     * statistic from a foreign writer) falls back to the same tolerant unsigned byte comparison the {@code BinaryVal}
     * path uses, instead of reading past the segment's end. A UUID's unsigned byte order equals {@link #compareBytes}
     * over its 16 bytes, hence the two paths agree on full-width segments.
     */
    private static int compareSegmentToUuidValue(MemorySegment segment, UUID uuid) {
        if (segment.byteSize() == UuidConverter.BYTES) {
            return UuidConverter.compareSegmentToUuid(segment, uuid);
        }
        return compareBytes(segment, UuidConverter.toReadOnlySegment(uuid));
    }

    /**
     * Orders two floating-point values numerically, with {@code -0.0} equal to {@code +0.0}. NaN sorts after the
     * numbers and equal to itself, keeping the order total; a predicate decides what NaN means through
     * {@link ComparisonOperator#holds(double, double)}.
     */
    static int compareFloating(double a, double b) {
        if (a == b) {
            return 0;
        }
        return Double.compare(a, b);
    }

    /** Lexicographic unsigned byte comparison, mirroring Parquet's default ColumnOrder for binary columns. */
    public static int compareBytes(MemorySegment a, MemorySegment b) {
        long aLen = a.byteSize();
        long bLen = b.byteSize();
        long common = Math.min(aLen, bLen);
        for (long i = 0; i < common; i++) {
            int diff = (a.get(JAVA_BYTE, i) & 0xff) - (b.get(JAVA_BYTE, i) & 0xff);
            if (diff != 0) {
                return diff;
            }
        }
        return Long.compare(aLen, bLen);
    }
}
