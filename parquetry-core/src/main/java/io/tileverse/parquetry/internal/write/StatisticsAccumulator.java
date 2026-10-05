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
package io.tileverse.parquetry.internal.write;

import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.OptionalLong;

import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.write.page.PageStatistics;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

import lombok.NonNull;

/**
 * Per-column accumulator for the {@code min_value}, {@code max_value}, {@code null_count} and {@code nan_count} of
 * {@link Statistics} and of the per-page {@link PageStatistics} snapshots.
 *
 * <p>Two snapshot operations cooperate on a single column-chunk pass: {@link #finishPage()} emits the per-page summary
 * for {@link ColumnIndexBuilder} without resetting the running state, while {@link #finishChunk()} produces the
 * column-chunk {@link Statistics} written to {@code ColumnMetaData}. Callers that drive the accumulator from several
 * batches in pieces can merge sibling accumulators with {@link #merge(StatisticsAccumulator)} before publishing the
 * chunk snapshot.
 *
 * <p>Each non-null cell enters through its kind's typed update method ({@link #updateInt}, {@link #updateLong},
 * {@link #updateFloat}, {@link #updateDouble}, {@link #updateBoolean}, {@link #updateBinary}); {@link #updateNull}
 * records a null and {@link #updateNonNull} records a non-null observation for the kinds without min/max
 * ({@code INT96}). Binary retention copies the segment bytes only on the first observation or a strict min/max
 * improvement, never per cell, and the caller may reuse or release its source segment after the call returns.
 *
 * <p>The bounds follow the {@link BoundsOrder} of the column. A column with an {@link BoundsOrder#UNDEFINED undefined}
 * order tracks its null count and no bounds; geometry and geography columns are among them, and their bounding box and
 * geometry types are kept by {@link GeospatialStatisticsAccumulator}.
 *
 * <p>A FLOAT, DOUBLE or FLOAT16 column counts its NaN cells in {@code nan_count}, and as non-null cells too. Its bounds
 * are its smallest and largest number, and a window holding no number has none. A zero bound is recorded as
 * {@code -0.0} for a minimum and as {@code +0.0} for a maximum, as required by the format: readers disagree on the
 * order of the two zeros, and a bound of the other sign would hide one of them.
 *
 * <p>Distinct counts are intentionally not tracked: the Parquet spec marks {@code distinct_count} as optional and any
 * write-side estimate would have to choose between an HLL approximation or an exact hash set; both are deferred until a
 * concrete consumer needs them.
 */
public final class StatisticsAccumulator {

    /** The order key of {@code +0.0} in a floating-point column: its bits are zero, and their key too. */
    private static final long POSITIVE_ZERO_KEY = 0L;

    private final PrimitiveKind kind;
    private final BoundsOrder order;
    private final boolean tracksMinMax;
    private final boolean legacyOrderMatchesModern;

    /**
     * XORed into an INT32 cell to obtain its order key, and into a key to obtain the cell back. Zero for a signed
     * column. The sign bit for an unsigned column: with it flipped, signed comparison orders the cells as unsigned
     * numbers.
     */
    private final int intSignFlip;

    /** The INT64 counterpart of {@link #intSignFlip}. */
    private final long longSignFlip;

    /** The order key of {@code -0.0} in a floating-point column; unused by the other columns. */
    private final long negativeZeroKey;

    private long nullCount;
    private long nonNullCount;
    private boolean hasMinMax;

    /**
     * Order keys of the smallest and largest cell of the window, for the orders comparing numbers: signed comparison of
     * two keys orders their cells. A boolean, an integer, or the {@link TotalOrder} key of a floating-point number.
     */
    private long minKey;

    private long maxKey;

    /** The smallest and largest cell of the window, for the orders comparing bytes. */
    private MemorySegment binaryMin;

    private MemorySegment binaryMax;

    private long nanCount;

    private StatisticsAccumulator(PrimitiveKind kind, BoundsOrder order) {
        this.kind = kind;
        this.order = order;
        this.tracksMinMax = order.isDefined();
        this.legacyOrderMatchesModern = legacyOrderMatchesModern(order);
        this.intSignFlip = order == BoundsOrder.UNSIGNED_INT32 ? Integer.MIN_VALUE : 0;
        this.longSignFlip = order == BoundsOrder.UNSIGNED_INT64 ? Long.MIN_VALUE : 0L;
        this.negativeZeroKey = negativeZeroKey(order);
    }

    /** Returns a fresh accumulator for the cells of {@code leaf}, with bounds in the leaf's {@link BoundsOrder}. */
    static StatisticsAccumulator forColumn(@NonNull SchemaNode.Primitive leaf) {
        return new StatisticsAccumulator(leaf.kind(), BoundsOrder.of(leaf));
    }

    private static long negativeZeroKey(BoundsOrder order) {
        return switch (order) {
            case FLOAT -> TotalOrder.key(Float.floatToRawIntBits(-0.0f));
            case DOUBLE -> TotalOrder.key(Double.doubleToRawLongBits(-0.0));
            case HALF_FLOAT -> HalfFloats.key(Float.floatToFloat16(-0.0f));
            case BOOLEAN,
                    SIGNED_INT32,
                    UNSIGNED_INT32,
                    SIGNED_INT64,
                    UNSIGNED_INT64,
                    UNSIGNED_BYTES,
                    SIGNED_BYTES,
                    UNDEFINED -> 0L;
        };
    }

    /**
     * Decides whether the deprecated {@code min}/{@code max} fields may mirror the modern {@code min_value}/
     * {@code max_value} bytes for a column in {@code order}. A legacy reader ignoring {@code column_orders} reads the
     * deprecated fields as signed numbers, and binary values as signed bytes. Mirroring is sound only for the orders
     * agreeing with that reading, FLOAT and DOUBLE numbers among them.
     */
    private static boolean legacyOrderMatchesModern(BoundsOrder order) {
        return switch (order) {
            case BOOLEAN, SIGNED_INT32, SIGNED_INT64, FLOAT, DOUBLE -> true;
            case UNSIGNED_INT32, UNSIGNED_INT64, HALF_FLOAT, UNSIGNED_BYTES, SIGNED_BYTES, UNDEFINED -> false;
        };
    }

    /** Adds one non-null INT32 cell: counts the observation and folds it into min/max. */
    public void updateInt(int value) {
        requireValueKind(PrimitiveKind.INT32);
        nonNullCount++;
        if (tracksMinMax) {
            updateKeyMinMax(value ^ intSignFlip);
        }
    }

    /** Adds one non-null INT64 cell: counts the observation and folds it into min/max. */
    public void updateLong(long value) {
        requireValueKind(PrimitiveKind.INT64);
        nonNullCount++;
        if (tracksMinMax) {
            updateKeyMinMax(value ^ longSignFlip);
        }
    }

    /** Adds one non-null FLOAT cell: counts the observation, and folds a number into min/max or counts a NaN. */
    public void updateFloat(float value) {
        requireValueKind(PrimitiveKind.FLOAT);
        nonNullCount++;
        if (Float.isNaN(value)) {
            nanCount++;
        } else if (tracksMinMax) {
            int key = TotalOrder.key(Float.floatToRawIntBits(value));
            updateKeyMinMax(key);
        }
    }

    /** Adds one non-null DOUBLE cell: counts the observation, and folds a number into min/max or counts a NaN. */
    public void updateDouble(double value) {
        requireValueKind(PrimitiveKind.DOUBLE);
        nonNullCount++;
        if (Double.isNaN(value)) {
            nanCount++;
        } else if (tracksMinMax) {
            long key = TotalOrder.key(Double.doubleToRawLongBits(value));
            updateKeyMinMax(key);
        }
    }

    /** Adds one non-null BOOLEAN cell: counts the observation and folds it into min/max. */
    public void updateBoolean(boolean value) {
        requireValueKind(PrimitiveKind.BOOLEAN);
        nonNullCount++;
        if (tracksMinMax) {
            updateKeyMinMax(value ? 1L : 0L);
        }
    }

    /** Adds one null cell. */
    public void updateNull() {
        nullCount++;
    }

    /**
     * Adds one non-null cell for a kind that tracks no min/max (INT96). Kinds with min/max tracking must go through
     * their typed update; that is how the observation reaches the bounds.
     */
    public void updateNonNull() {
        if (tracksMinMax) {
            throw new ParquetWriteException("updateNonNull is only for kinds without min/max tracking; " + kind
                    + " must use its typed update method");
        }
        nonNullCount++;
    }

    private void requireValueKind(PrimitiveKind valueKind) {
        if (kind != valueKind) {
            throw new ParquetWriteException("Accumulator for " + kind + " received a " + valueKind + " value");
        }
    }

    /**
     * Adds one non-null binary cell. A FLOAT16 cell folds its number into min/max or counts as a NaN. Any other cell is
     * compared in place against the retained bounds; its bytes are copied only on the first observation or a strict
     * min/max improvement, never per cell.
     */
    public void updateBinary(@NonNull MemorySegment value) {
        requireBinaryKind();
        nonNullCount++;
        if (order == BoundsOrder.HALF_FLOAT) {
            updateHalfFloat(value);
        } else if (tracksMinMax) {
            updateBinaryMinMax(value);
        }
    }

    private void updateHalfFloat(MemorySegment cell) {
        if (HalfFloats.isNaN(cell)) {
            nanCount++;
        } else {
            updateKeyMinMax(HalfFloats.key(cell));
        }
    }

    private void requireBinaryKind() {
        if (kind != PrimitiveKind.BYTE_ARRAY && kind != PrimitiveKind.FIXED_LEN_BYTE_ARRAY) {
            throw new ParquetWriteException("Accumulator for " + kind + " received a binary value");
        }
    }

    private void updateKeyMinMax(long key) {
        if (!hasMinMax) {
            minKey = key;
            maxKey = key;
            hasMinMax = true;
            return;
        }
        if (key < minKey) {
            minKey = key;
        } else if (key > maxKey) {
            maxKey = key;
        }
    }

    private void updateBinaryMinMax(MemorySegment value) {
        if (!hasMinMax) {
            MemorySegment retained = retainCopy(value);
            binaryMin = retained;
            binaryMax = retained;
            hasMinMax = true;
            return;
        }
        if (order.compare(value, binaryMin) < 0) {
            binaryMin = retainCopy(value);
        }
        if (order.compare(value, binaryMax) > 0) {
            binaryMax = retainCopy(value);
        }
    }

    private static MemorySegment retainCopy(MemorySegment value) {
        return MemorySegment.ofArray(value.toArray(ValueLayout.JAVA_BYTE)).asReadOnly();
    }

    /**
     * Merges {@code other} into this accumulator. Both accumulators must have been created for the same
     * {@link PrimitiveKind} and the same {@link BoundsOrder}; otherwise a {@link ParquetWriteException} is raised
     * because mixing them would corrupt the stored representation.
     */
    public void merge(@NonNull StatisticsAccumulator other) {
        requireSameColumnType(other);
        nullCount += other.nullCount;
        nonNullCount += other.nonNullCount;
        nanCount += other.nanCount;
        if (other.hasMinMax) {
            mergeMinMax(other);
        }
    }

    private void requireSameColumnType(StatisticsAccumulator other) {
        if (other.kind != this.kind) {
            throw new ParquetWriteException(
                    "Cannot merge accumulators of different kinds: " + this.kind + " vs " + other.kind);
        }
        if (other.order != this.order) {
            throw new ParquetWriteException(
                    "Cannot merge accumulators of different orders: " + this.order + " vs " + other.order);
        }
    }

    private void mergeMinMax(StatisticsAccumulator other) {
        if (!hasMinMax) {
            copyMinMaxFrom(other);
            return;
        }
        if (order.comparesBytes()) {
            mergeBinaryMinMax(other);
        } else {
            minKey = Math.min(minKey, other.minKey);
            maxKey = Math.max(maxKey, other.maxKey);
        }
    }

    private void copyMinMaxFrom(StatisticsAccumulator other) {
        minKey = other.minKey;
        maxKey = other.maxKey;
        binaryMin = other.binaryMin;
        binaryMax = other.binaryMax;
        hasMinMax = true;
    }

    private void mergeBinaryMinMax(StatisticsAccumulator other) {
        if (order.compare(other.binaryMin, binaryMin) < 0) {
            binaryMin = other.binaryMin;
        }
        if (order.compare(other.binaryMax, binaryMax) > 0) {
            binaryMax = other.binaryMax;
        }
    }

    /**
     * Returns the chunk-level {@link Statistics} for the current accumulation window. Subsequent typed update calls
     * keep accumulating; {@link #reset()} starts a fresh window.
     */
    public Statistics finishChunk() {
        MemorySegment minSegment = encodedLowerBound();
        MemorySegment maxSegment = encodedUpperBound();
        return Statistics.builder()
                .nullCount(OptionalLong.of(nullCount))
                .distinctCount(OptionalLong.empty())
                .minValue(minSegment)
                .maxValue(maxSegment)
                .isMinValueExact(lowerBoundIsExact())
                .isMaxValueExact(upperBoundIsExact())
                .min(legacyBound(minSegment))
                .max(legacyBound(maxSegment))
                .nanCount(recordedNaNCount())
                .build();
    }

    /**
     * Returns the per-page snapshot used by {@link ColumnIndexBuilder}. Like {@link #finishChunk()} this method does
     * not modify the accumulator -- to start a new page window call {@link #reset()}.
     */
    public PageStatistics finishPage() {
        MemorySegment minSegment = encodedLowerBound();
        MemorySegment maxSegment = encodedUpperBound();
        boolean isNullPage = nonNullCount == 0 && nullCount > 0;
        return new PageStatistics(
                minSegment,
                maxSegment,
                nullCount,
                isNullPage,
                recordedNaNCount(),
                lowerBoundIsExact(),
                upperBoundIsExact());
    }

    /** Returns the accumulator to its initial state. */
    public void reset() {
        nullCount = 0;
        nonNullCount = 0;
        hasMinMax = false;
        minKey = 0L;
        maxKey = 0L;
        binaryMin = null;
        binaryMax = null;
        nanCount = 0;
    }

    /** The NaN count of the window, recorded for a floating-point column alone. */
    private OptionalLong recordedNaNCount() {
        if (order.isFloatingPoint()) {
            return OptionalLong.of(nanCount);
        }
        return OptionalLong.empty();
    }

    /**
     * The bytes of a modern bound to publish in its deprecated field too: the bound itself when the column's order
     * agrees with the signed reading of a legacy reader, nothing otherwise; see
     * {@link #legacyOrderMatchesModern(BoundsOrder)}.
     */
    private MemorySegment legacyBound(MemorySegment modernBound) {
        if (legacyOrderMatchesModern) {
            return modernBound;
        }
        return MemorySegment.NULL;
    }

    /** The smallest cell of the window, nothing for a window holding no ordered cell. */
    private MemorySegment encodedLowerBound() {
        if (!hasMinMax) {
            return MemorySegment.NULL;
        }
        return order.comparesBytes() ? binaryMin : encoded(lowerBoundKey());
    }

    /** The largest cell of the window, nothing for a window holding no ordered cell. */
    private MemorySegment encodedUpperBound() {
        if (!hasMinMax) {
            return MemorySegment.NULL;
        }
        return order.comparesBytes() ? binaryMax : encoded(upperBoundKey());
    }

    /** Whether the lower bound is held by a cell of the window: not so for a {@code -0.0} written over {@code +0.0}. */
    private boolean lowerBoundIsExact() {
        return hasMinMax && lowerBoundKey() == minKey;
    }

    /** Whether the upper bound is held by a cell of the window: not so for a {@code +0.0} written over {@code -0.0}. */
    private boolean upperBoundIsExact() {
        return hasMinMax && upperBoundKey() == maxKey;
    }

    /**
     * A floating-point minimum of zero is recorded as {@code -0.0}, regardless of the sign of the zeros in the window.
     */
    private long lowerBoundKey() {
        boolean positiveZero = order.isFloatingPoint() && minKey == POSITIVE_ZERO_KEY;
        return positiveZero ? negativeZeroKey : minKey;
    }

    /**
     * A floating-point maximum of zero is recorded as {@code +0.0}, regardless of the sign of the zeros in the window.
     */
    private long upperBoundKey() {
        boolean negativeZero = order.isFloatingPoint() && maxKey == negativeZeroKey;
        return negativeZero ? POSITIVE_ZERO_KEY : maxKey;
    }

    /** The PLAIN bytes of the cell with the given order key. */
    private MemorySegment encoded(long key) {
        return switch (order) {
            case BOOLEAN -> readOnlyHeap(new byte[] {(byte) key});
            case SIGNED_INT32, UNSIGNED_INT32 -> encodedInt32((int) key ^ intSignFlip);
            case SIGNED_INT64, UNSIGNED_INT64 -> encodedInt64(key ^ longSignFlip);
            case FLOAT -> encodedInt32(TotalOrder.bits((int) key));
            case DOUBLE -> encodedInt64(TotalOrder.bits(key));
            case HALF_FLOAT -> encodedHalfFloat(HalfFloats.bits((int) key));
            case UNSIGNED_BYTES, SIGNED_BYTES, UNDEFINED ->
                throw new IllegalStateException("a column in " + order + " order has no order keys");
        };
    }

    private static MemorySegment encodedInt32(int value) {
        byte[] out = new byte[Integer.BYTES];
        MemorySegment.ofArray(out).set(INT32, 0L, value);
        return readOnlyHeap(out);
    }

    private static MemorySegment encodedInt64(long value) {
        byte[] out = new byte[Long.BYTES];
        MemorySegment.ofArray(out).set(INT64, 0L, value);
        return readOnlyHeap(out);
    }

    private static MemorySegment encodedHalfFloat(short bits) {
        return readOnlyHeap(HalfFloats.encode(bits));
    }

    private static MemorySegment readOnlyHeap(byte[] bytes) {
        return MemorySegment.ofArray(bytes).asReadOnly();
    }
}
