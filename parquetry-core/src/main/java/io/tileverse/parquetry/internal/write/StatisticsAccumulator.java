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
package io.tileverse.parquetry.internal.write;

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.FLOAT;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.OptionalLong;

import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.write.page.PageStatistics;
import io.tileverse.parquetry.schema.PrimitiveKind;

import lombok.NonNull;

/**
 * Per-column accumulator for the typed {@code min_value}, {@code max_value}, and {@code null_count} carried by
 * {@link Statistics} and by per-page {@link PageStatistics} snapshots.
 *
 * <p>Two snapshot operations cooperate on a single column-chunk pass: {@link #finishPage()} emits the per-page summary
 * for {@link ColumnIndexBuilder} without resetting the running state, while {@link #finishChunk()} produces the
 * column-chunk {@link Statistics} that lands in {@code ColumnMetaData}. Callers that drive the accumulator from several
 * batches in pieces can merge sibling accumulators with {@link #merge(StatisticsAccumulator)} before publishing the
 * chunk snapshot.
 *
 * <p>Ordering mirrors the read-side stats evaluator: signed numeric comparison for the integer and floating kinds and
 * unsigned lexicographic byte comparison for {@code BYTE_ARRAY} / {@code FIXED_LEN_BYTE_ARRAY}, matching Parquet's
 * default {@code ColumnOrder}. Floating-point {@code NaN} values participate in null-count tracking but are excluded
 * from min/max because the Parquet stats contract forbids representing them in the {@code min_value}/{@code max_value}
 * bytes. {@code INT96} is intentionally unsupported: the format itself does not carry typed stats for it.
 *
 * <p>Geometry and geography columns have no meaningful PLAIN-byte ordering, so {@link #forKind(PrimitiveKind,
 * LogicalType)} returns an accumulator that tracks {@code null_count} but never produces min/max; geometry-specific
 * bbox + type-set statistics live in {@link GeospatialStatisticsAccumulator}.
 *
 * <p>Distinct counts are intentionally not tracked: the Parquet spec marks {@code distinct_count} as optional and any
 * write-side estimate would have to choose between an HLL approximation or an exact hash set; both are deferred until a
 * concrete consumer needs them.
 */
public final class StatisticsAccumulator {

    private final PrimitiveKind kind;
    private final boolean tracksMinMax;
    private final boolean legacyOrderMatchesModern;

    private long nullCount;
    private long nonNullCount;
    private boolean hasMinMax;

    private boolean booleanMin;
    private boolean booleanMax;
    private int intMin;
    private int intMax;
    private long longMin;
    private long longMax;
    private float floatMin;
    private float floatMax;
    private double doubleMin;
    private double doubleMax;
    private byte[] binaryMin;
    private byte[] binaryMax;

    private StatisticsAccumulator(PrimitiveKind kind, boolean tracksMinMax, boolean legacyOrderMatchesModern) {
        this.kind = kind;
        this.tracksMinMax = tracksMinMax;
        this.legacyOrderMatchesModern = legacyOrderMatchesModern;
    }

    /**
     * Returns a fresh accumulator sized for {@code kind}. When {@code logicalType} is {@link LogicalType.Geometry} or
     * {@link LogicalType.Geography} the accumulator skips min/max tracking even though the physical kind would normally
     * support it; geometry stats live in {@link GeospatialStatisticsAccumulator}. Passing {@code null} for
     * {@code logicalType} keeps min/max tracking enabled.
     */
    public static StatisticsAccumulator forKind(@NonNull PrimitiveKind kind, LogicalType logicalType) {
        boolean tracksMinMax = supportsMinMax(kind) && !isGeometryLike(logicalType);
        boolean legacyOrderMatchesModern = legacyOrderMatchesModern(kind, logicalType);
        return new StatisticsAccumulator(kind, tracksMinMax, legacyOrderMatchesModern);
    }

    private static boolean supportsMinMax(PrimitiveKind kind) {
        return kind != PrimitiveKind.INT96;
    }

    private static boolean isGeometryLike(LogicalType logicalType) {
        return logicalType instanceof LogicalType.Geometry || logicalType instanceof LogicalType.Geography;
    }

    /**
     * Decides whether the deprecated {@code min}/{@code max} fields may mirror the modern {@code min_value}/
     * {@code max_value} bytes for this column. The legacy fields are spec-defined under SIGNED order for the binary
     * kinds and for unsigned-integer logical types, while parquetry computes those bounds under unsigned lexicographic
     * order. Mirroring is only sound when parquetry's computed order is signed and already matches the legacy contract:
     * the signed numeric kinds, and INT32/INT64 only when no unsigned-integer logical type retypes them.
     */
    private static boolean legacyOrderMatchesModern(PrimitiveKind kind, LogicalType logicalType) {
        return switch (kind) {
            case BOOLEAN, FLOAT, DOUBLE -> true;
            case INT32, INT64 -> !isUnsignedInteger(logicalType);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> false;
        };
    }

    private static boolean isUnsignedInteger(LogicalType logicalType) {
        return logicalType instanceof LogicalType.IntType intType && !intType.isSigned();
    }

    /**
     * Adds one cell to the accumulation window. {@code value} is the unboxed primitive for numeric / boolean kinds and
     * a {@link MemorySegment} for binary kinds ({@code BYTE_ARRAY}, {@code FIXED_LEN_BYTE_ARRAY}, {@code INT96}). The
     * binary path defensive-copies the segment bytes into the accumulator's internal scratch buffer so the caller can
     * reuse or release the source after this call returns. {@code value} is only consulted when {@code isNull} is
     * {@code false}.
     */
    public void update(Object value, boolean isNull) {
        if (isNull) {
            nullCount++;
            return;
        }
        nonNullCount++;
        if (!tracksMinMax) {
            return;
        }
        updateMinMax(value);
    }

    private void updateMinMax(Object value) {
        switch (kind) {
            case BOOLEAN -> updateBoolean((boolean) value);
            case INT32 -> updateInt32((int) value);
            case INT64 -> updateInt64((long) value);
            case FLOAT -> updateFloat((float) value);
            case DOUBLE -> updateDouble((double) value);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> updateBinary((MemorySegment) value);
            case INT96 -> {
                /* unreachable: tracksMinMax is false for INT96 */
            }
        }
    }

    private void updateBoolean(boolean v) {
        if (!hasMinMax) {
            booleanMin = v;
            booleanMax = v;
            hasMinMax = true;
            return;
        }
        if (Boolean.compare(v, booleanMin) < 0) {
            booleanMin = v;
        }
        if (Boolean.compare(v, booleanMax) > 0) {
            booleanMax = v;
        }
    }

    private void updateInt32(int v) {
        if (!hasMinMax) {
            intMin = v;
            intMax = v;
            hasMinMax = true;
            return;
        }
        if (v < intMin) {
            intMin = v;
        }
        if (v > intMax) {
            intMax = v;
        }
    }

    private void updateInt64(long v) {
        if (!hasMinMax) {
            longMin = v;
            longMax = v;
            hasMinMax = true;
            return;
        }
        if (v < longMin) {
            longMin = v;
        }
        if (v > longMax) {
            longMax = v;
        }
    }

    private void updateFloat(float v) {
        // NaN is excluded from min/max but already counted as a non-null observation.
        if (Float.isNaN(v)) {
            return;
        }
        if (!hasMinMax) {
            floatMin = v;
            floatMax = v;
            hasMinMax = true;
            return;
        }
        if (Float.compare(v, floatMin) < 0) {
            floatMin = v;
        }
        if (Float.compare(v, floatMax) > 0) {
            floatMax = v;
        }
    }

    private void updateDouble(double v) {
        if (Double.isNaN(v)) {
            return;
        }
        if (!hasMinMax) {
            doubleMin = v;
            doubleMax = v;
            hasMinMax = true;
            return;
        }
        if (Double.compare(v, doubleMin) < 0) {
            doubleMin = v;
        }
        if (Double.compare(v, doubleMax) > 0) {
            doubleMax = v;
        }
    }

    private void updateBinary(MemorySegment segment) {
        // toArray copies into a fresh byte[]; safe to retain without further cloning. binaryMin and binaryMax start
        // aliased to the same array on the first observation and diverge as min/max move independently afterwards.
        byte[] bytes = segment.toArray(ValueLayout.JAVA_BYTE);
        if (!hasMinMax) {
            binaryMin = bytes;
            binaryMax = bytes;
            hasMinMax = true;
            return;
        }
        if (compareBytes(bytes, binaryMin) < 0) {
            binaryMin = bytes;
        }
        if (compareBytes(bytes, binaryMax) > 0) {
            binaryMax = bytes;
        }
    }

    private static int compareBytes(byte[] a, byte[] b) {
        int common = Math.min(a.length, b.length);
        for (int i = 0; i < common; i++) {
            int diff = (a[i] & 0xff) - (b[i] & 0xff);
            if (diff != 0) {
                return diff;
            }
        }
        return Integer.compare(a.length, b.length);
    }

    /**
     * Merges {@code other} into this accumulator. Both accumulators must have been created for the same
     * {@link PrimitiveKind} and the same min/max-tracking policy; otherwise a {@link ParquetWriteException} is raised
     * because mixing kinds would corrupt the stored representation.
     */
    public void merge(@NonNull StatisticsAccumulator other) {
        if (other.kind != this.kind || other.tracksMinMax != this.tracksMinMax) {
            throw new ParquetWriteException(
                    "Cannot merge accumulators of different kinds: " + this.kind + " vs " + other.kind);
        }
        nullCount += other.nullCount;
        nonNullCount += other.nonNullCount;
        if (!other.hasMinMax) {
            return;
        }
        if (tracksMinMax) {
            mergeMinMax(other);
        }
    }

    private void mergeMinMax(StatisticsAccumulator other) {
        if (!hasMinMax) {
            copyMinMaxFrom(other);
            hasMinMax = true;
            return;
        }
        switch (kind) {
            case BOOLEAN -> mergeBooleanMinMax(other);
            case INT32 -> mergeIntMinMax(other);
            case INT64 -> mergeLongMinMax(other);
            case FLOAT -> mergeFloatMinMax(other);
            case DOUBLE -> mergeDoubleMinMax(other);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> mergeBinaryMinMax(other);
            case INT96 -> {
                /* unreachable */
            }
        }
    }

    private void mergeBooleanMinMax(StatisticsAccumulator other) {
        if (Boolean.compare(other.booleanMin, booleanMin) < 0) {
            booleanMin = other.booleanMin;
        }
        if (Boolean.compare(other.booleanMax, booleanMax) > 0) {
            booleanMax = other.booleanMax;
        }
    }

    private void mergeIntMinMax(StatisticsAccumulator other) {
        if (other.intMin < intMin) {
            intMin = other.intMin;
        }
        if (other.intMax > intMax) {
            intMax = other.intMax;
        }
    }

    private void mergeLongMinMax(StatisticsAccumulator other) {
        if (other.longMin < longMin) {
            longMin = other.longMin;
        }
        if (other.longMax > longMax) {
            longMax = other.longMax;
        }
    }

    private void mergeFloatMinMax(StatisticsAccumulator other) {
        if (Float.compare(other.floatMin, floatMin) < 0) {
            floatMin = other.floatMin;
        }
        if (Float.compare(other.floatMax, floatMax) > 0) {
            floatMax = other.floatMax;
        }
    }

    private void mergeDoubleMinMax(StatisticsAccumulator other) {
        if (Double.compare(other.doubleMin, doubleMin) < 0) {
            doubleMin = other.doubleMin;
        }
        if (Double.compare(other.doubleMax, doubleMax) > 0) {
            doubleMax = other.doubleMax;
        }
    }

    private void mergeBinaryMinMax(StatisticsAccumulator other) {
        if (compareBytes(other.binaryMin, binaryMin) < 0) {
            binaryMin = other.binaryMin.clone();
        }
        if (compareBytes(other.binaryMax, binaryMax) > 0) {
            binaryMax = other.binaryMax.clone();
        }
    }

    private void copyMinMaxFrom(StatisticsAccumulator other) {
        switch (kind) {
            case BOOLEAN -> {
                booleanMin = other.booleanMin;
                booleanMax = other.booleanMax;
            }
            case INT32 -> {
                intMin = other.intMin;
                intMax = other.intMax;
            }
            case INT64 -> {
                longMin = other.longMin;
                longMax = other.longMax;
            }
            case FLOAT -> {
                floatMin = other.floatMin;
                floatMax = other.floatMax;
            }
            case DOUBLE -> {
                doubleMin = other.doubleMin;
                doubleMax = other.doubleMax;
            }
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
                binaryMin = other.binaryMin.clone();
                binaryMax = other.binaryMax.clone();
            }
            case INT96 -> {
                /* unreachable */
            }
        }
    }

    /**
     * Returns the chunk-level {@link Statistics} for the current accumulation window. Subsequent {@link #update} calls
     * keep accumulating; {@link #reset()} starts a fresh window.
     */
    public Statistics finishChunk() {
        MemorySegment minSegment = encodedMin();
        MemorySegment maxSegment = encodedMax();
        return Statistics.builder()
                .nullCount(OptionalLong.of(nullCount))
                .distinctCount(OptionalLong.empty())
                .minValue(minSegment)
                .maxValue(maxSegment)
                .isMinValueExact(minSegment != MemorySegment.NULL)
                .isMaxValueExact(maxSegment != MemorySegment.NULL)
                .min(legacyMinSegment(minSegment))
                .max(legacyMaxSegment(maxSegment))
                .build();
    }

    /**
     * Mirrors the modern {@code min_value} bytes into the deprecated {@code min} field only when parquetry's computed
     * order matches the legacy SIGNED contract for this column. A legacy reader that ignores {@code column_orders}
     * reads the deprecated fields under signed order; for the binary kinds and unsigned-integer logical types parquetry
     * computes the bound under unsigned order, which a legacy reader would invert and mis-prune. Omitting the legacy
     * field for those columns keeps the modern {@code min_value} as the only published bound.
     */
    private MemorySegment legacyMinSegment(MemorySegment modernMin) {
        if (legacyOrderMatchesModern) {
            return modernMin;
        }
        return MemorySegment.NULL;
    }

    private MemorySegment legacyMaxSegment(MemorySegment modernMax) {
        if (legacyOrderMatchesModern) {
            return modernMax;
        }
        return MemorySegment.NULL;
    }

    /**
     * Returns the per-page snapshot used by {@link ColumnIndexBuilder}. Like {@link #finishChunk()} this method does
     * not modify the accumulator -- to start a new page window call {@link #reset()}.
     */
    public PageStatistics finishPage() {
        MemorySegment minSegment = encodedMin();
        MemorySegment maxSegment = encodedMax();
        boolean isNullPage = nonNullCount == 0 && nullCount > 0;
        return new PageStatistics(minSegment, maxSegment, nullCount, isNullPage);
    }

    /** Returns the accumulator to its initial state. */
    public void reset() {
        nullCount = 0;
        nonNullCount = 0;
        hasMinMax = false;
        booleanMin = false;
        booleanMax = false;
        intMin = 0;
        intMax = 0;
        longMin = 0L;
        longMax = 0L;
        floatMin = 0f;
        floatMax = 0f;
        doubleMin = 0d;
        doubleMax = 0d;
        binaryMin = null;
        binaryMax = null;
    }

    private MemorySegment encodedMin() {
        if (!tracksMinMax || !hasMinMax) {
            return MemorySegment.NULL;
        }
        return switch (kind) {
            case BOOLEAN -> readOnlyHeap(new byte[] {(byte) (booleanMin ? 1 : 0)});
            case INT32 -> readOnlyHeap(encodeInt32(intMin));
            case INT64 -> readOnlyHeap(encodeInt64(longMin));
            case FLOAT -> readOnlyHeap(encodeFloat(floatMin));
            case DOUBLE -> readOnlyHeap(encodeDouble(doubleMin));
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> readOnlyHeap(binaryMin.clone());
            case INT96 -> MemorySegment.NULL;
        };
    }

    private MemorySegment encodedMax() {
        if (!tracksMinMax || !hasMinMax) {
            return MemorySegment.NULL;
        }
        return switch (kind) {
            case BOOLEAN -> readOnlyHeap(new byte[] {(byte) (booleanMax ? 1 : 0)});
            case INT32 -> readOnlyHeap(encodeInt32(intMax));
            case INT64 -> readOnlyHeap(encodeInt64(longMax));
            case FLOAT -> readOnlyHeap(encodeFloat(floatMax));
            case DOUBLE -> readOnlyHeap(encodeDouble(doubleMax));
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> readOnlyHeap(binaryMax.clone());
            case INT96 -> MemorySegment.NULL;
        };
    }

    private static MemorySegment readOnlyHeap(byte[] bytes) {
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    private static byte[] encodeInt32(int value) {
        byte[] out = new byte[4];
        MemorySegment.ofArray(out).set(INT32, 0L, value);
        return out;
    }

    private static byte[] encodeInt64(long value) {
        byte[] out = new byte[8];
        MemorySegment.ofArray(out).set(INT64, 0L, value);
        return out;
    }

    private static byte[] encodeFloat(float value) {
        byte[] out = new byte[4];
        MemorySegment.ofArray(out).set(FLOAT, 0L, value);
        return out;
    }

    private static byte[] encodeDouble(double value) {
        byte[] out = new byte[8];
        MemorySegment.ofArray(out).set(DOUBLE, 0L, value);
        return out;
    }
}
