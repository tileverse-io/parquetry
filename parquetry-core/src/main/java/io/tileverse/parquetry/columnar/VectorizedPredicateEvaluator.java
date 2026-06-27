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
package io.tileverse.parquetry.columnar;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.function.IntPredicate;

import io.tileverse.parquetry.filter.GeometryFilter;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.internal.filter.RecordAccessors;
import io.tileverse.parquetry.internal.filter.RecordLevelEvaluator;
import io.tileverse.parquetry.internal.filter.ValueComparison;
import io.tileverse.parquetry.internal.filter.spatial.WkbEnvelope;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Evaluates a normalized predicate against one {@link ParquetRecordBatch} columnar-style, returning the {@link BitSet}
 * of matching rows. Calling {@code cardinality()} on the result yields the match count with no row materialization.
 * Null-only predicates resolve to validity-mask popcounts; comparisons scan a typed array and intersect with validity.
 *
 * <p>Semantics match {@link io.tileverse.parquetry.internal.filter.RecordLevelEvaluator} exactly: a value comparison
 * against a null row never matches, {@link Predicate.IsNull} / {@link Predicate.IsNotNull} are the only predicates
 * whose result depends on null presence, and a null geometry has no spatial truth value (excluded from both a spatial
 * predicate and its negation). The predicate must already be normalized (Not pushed to leaves, Always folded, And/Or
 * flattened).
 */
public final class VectorizedPredicateEvaluator {

    private VectorizedPredicateEvaluator() {}

    /** Returns the bitset of rows in {@code batch} that satisfy {@code predicate}. */
    @SuppressWarnings({"java:S3776", "java:S7475"})
    public static BitSet eval(Predicate predicate, ParquetRecordBatch batch) {
        int rowCount = batch.rowCount();
        return switch (predicate) {
            case Predicate.Always(boolean value) -> value ? all(rowCount) : new BitSet(rowCount);
            case Predicate.And(List<Predicate> children) -> and(children, batch, rowCount);
            case Predicate.Or(List<Predicate> children) -> or(children, batch);
            // A null geometry has no spatial truth value and is excluded from both a spatial predicate and its
            // negation; the masks scan valid rows only. A plain bit-flip over [0,rowCount) would wrongly turn
            // null-geometry rows into matches, hence these dedicated negated arms ahead of the generic Not.
            case Predicate.Not(Predicate.Spatial spatial) -> spatialMask(batch, spatial, rowCount, true);
            case Predicate.Not(Predicate.GeometryFilterPredicate(GeometryFilter<?> filter)) ->
                geometryMask(batch, filter, rowCount, true);
            case Predicate.Not(Predicate child) -> negate(eval(child, batch), rowCount);
            case Predicate.IsNull(ColumnPath col) -> nulls(batch, col, rowCount);
            case Predicate.IsNotNull(ColumnPath col) -> validityOf(batch, col);
            case Predicate.Eq(ColumnPath col, Value v) -> compareMask(batch, col, v, c -> c == 0);
            case Predicate.NotEq(ColumnPath col, Value v) -> compareMask(batch, col, v, c -> c != 0);
            case Predicate.Lt(ColumnPath col, Value v) -> compareMask(batch, col, v, c -> c < 0);
            case Predicate.LtEq(ColumnPath col, Value v) -> compareMask(batch, col, v, c -> c <= 0);
            case Predicate.Gt(ColumnPath col, Value v) -> compareMask(batch, col, v, c -> c > 0);
            case Predicate.GtEq(ColumnPath col, Value v) -> compareMask(batch, col, v, c -> c >= 0);
            case Predicate.In(ColumnPath col, List<Value> values) -> inMask(batch, col, values);
            case Predicate.Spatial spatial -> spatialMask(batch, spatial, rowCount, false);
            case Predicate.GeometryFilterPredicate(GeometryFilter<?> filter) ->
                geometryMask(batch, filter, rowCount, false);
            case Predicate.Quantified q -> quantifiedMask(q, batch, rowCount);
        };
    }

    // A list/map leaf has no flat scalar vector to compare columnar-style; the existential/universal/exactly-one
    // semantics only make sense over a row's element values. Materialize each row and reuse the record-level
    // evaluator. Quantified is the multi-valued case, not the scalar hot path, hence a per-row fallback is acceptable.
    private static BitSet quantifiedMask(Predicate.Quantified quantified, ParquetRecordBatch batch, int rowCount) {
        return quantifiedMask(quantified, batch, rowCount, null);
    }

    private static BitSet quantifiedMask(
            Predicate.Quantified quantified, ParquetRecordBatch batch, int rowCount, BitSet restriction) {
        BitSet matches = new BitSet(rowCount);
        int row = (restriction == null) ? 0 : restriction.nextSetBit(0);
        while (row >= 0 && row < rowCount) {
            ParquetRecord rec = batch.materialize(row);
            if (RecordLevelEvaluator.test(quantified, RecordAccessors.of(rec))) {
                matches.set(row);
            }
            row = (restriction == null) ? row + 1 : restriction.nextSetBit(row + 1);
        }
        return matches;
    }

    private static BitSet all(int rowCount) {
        BitSet bits = new BitSet(rowCount);
        bits.set(0, rowCount);
        return bits;
    }

    // Evaluate the cheap, columnar children first to build the candidate set, then run the expensive per-row gates
    // (geometry/spatial WKB, quantified) only over rows that survived. Without this an `attribute AND intersects(...)`
    // would build a JTS geometry for every row in the batch rather than the few the attribute filter keeps. AND is
    // commutative, hence the result is identical; only the wasted work on already-excluded rows is removed.
    private static BitSet and(List<Predicate> children, ParquetRecordBatch batch, int rowCount) {
        BitSet acc = all(rowCount);
        List<Predicate> expensive = null;
        for (Predicate child : children) {
            if (isExpensive(child)) {
                if (expensive == null) {
                    expensive = new ArrayList<>();
                }
                expensive.add(child);
                continue;
            }
            acc.and(eval(child, batch));
            if (acc.isEmpty()) {
                return acc;
            }
        }
        if (expensive != null) {
            for (Predicate child : expensive) {
                acc.and(evalRestricted(child, batch, acc));
                if (acc.isEmpty()) {
                    return acc;
                }
            }
        }
        return acc;
    }

    // The per-row geometry/spatial gates and the quantified per-row fallback build or materialize per row; the columnar
    // leaves only scan a typed array. Deferring the former behind the latter inside an AND is the whole point of the
    // short-circuit above.
    private static boolean isExpensive(Predicate predicate) {
        return switch (predicate) {
            case Predicate.Spatial _ -> true;
            case Predicate.GeometryFilterPredicate _ -> true;
            case Predicate.Quantified _ -> true;
            case Predicate.Not(Predicate.Spatial _) -> true;
            case Predicate.Not(Predicate.GeometryFilterPredicate _) -> true;
            default -> false;
        };
    }

    // Evaluates an expensive child only over the rows still set in {@code restriction}, mirroring the matching arms of
    // {@link #eval}. Only the expensive leaves {@link #isExpensive} defers are restriction-aware.
    private static BitSet evalRestricted(Predicate child, ParquetRecordBatch batch, BitSet restriction) {
        int rowCount = batch.rowCount();
        return switch (child) {
            case Predicate.Spatial spatial -> spatialMask(batch, spatial, rowCount, false, restriction);
            case Predicate.Not(Predicate.Spatial spatial) -> spatialMask(batch, spatial, rowCount, true, restriction);
            case Predicate.GeometryFilterPredicate(GeometryFilter<?> filter) ->
                geometryMask(batch, filter, rowCount, false, restriction);
            case Predicate.Not(Predicate.GeometryFilterPredicate(GeometryFilter<?> filter)) ->
                geometryMask(batch, filter, rowCount, true, restriction);
            case Predicate.Quantified q -> quantifiedMask(q, batch, rowCount, restriction);
            default -> eval(child, batch);
        };
    }

    // Valid rows the AND short-circuit has not already excluded. A null restriction means no upstream filter (an
    // expensive leaf evaluated standalone); then every valid row is a candidate.
    private static BitSet candidateRows(Validity validity, BitSet restriction) {
        BitSet rows = validity.copy();
        if (restriction != null) {
            rows.and(restriction);
        }
        return rows;
    }

    private static BitSet or(List<Predicate> children, ParquetRecordBatch batch) {
        BitSet acc = new BitSet(batch.rowCount());
        for (Predicate child : children) {
            acc.or(eval(child, batch));
        }
        return acc;
    }

    private static BitSet negate(BitSet mask, int rowCount) {
        BitSet flipped = new BitSet(rowCount);
        flipped.set(0, rowCount);
        flipped.andNot(mask);
        return flipped;
    }

    private static BitSet validityOf(ParquetRecordBatch batch, ColumnPath col) {
        return batch.columns().get(col).validity().copy();
    }

    private static BitSet nulls(ParquetRecordBatch batch, ColumnPath col, int rowCount) {
        BitSet nullMask = batch.columns().get(col).validity().copy();
        nullMask.flip(0, rowCount);
        return nullMask;
    }

    // S6541 (Brain Method): the per-vector-type loops are an intentional dispatch table on the hot count path;
    // collapsing them to lower the metric would reintroduce per-row megamorphic dispatch (see the in-body note).
    @SuppressWarnings({"java:S3776", "java:S6541"})
    private static BitSet compareMask(ParquetRecordBatch batch, ColumnPath col, Value v, IntPredicate accept) {
        ColumnVector vec = batch.columns().get(col);
        Validity validity = vec.validity();
        BitSet out = new BitSet(batch.rowCount());
        // The per-vector-type loops below are intentional: this is the hot row-counting path, and monomorphic loops
        // keep each comparison boxing-free and avoid a per-row megamorphic functional-interface call. Do not collapse
        // them into a single loop driven by a shared lambda.
        switch (vec) {
            case IntVector iv -> {
                for (int r = validity.nextSetBit(0); r >= 0; r = validity.nextSetBit(r + 1)) {
                    if (accept.test(ValueComparison.compareInt(iv.getInt(r), v))) {
                        out.set(r);
                    }
                }
            }
            case LongVector lv -> {
                for (int r = validity.nextSetBit(0); r >= 0; r = validity.nextSetBit(r + 1)) {
                    if (accept.test(ValueComparison.compareLong(lv.getLong(r), v))) {
                        out.set(r);
                    }
                }
            }
            case DoubleVector dv -> {
                for (int r = validity.nextSetBit(0); r >= 0; r = validity.nextSetBit(r + 1)) {
                    if (accept.test(ValueComparison.compareDouble(dv.getDouble(r), v))) {
                        out.set(r);
                    }
                }
            }
            case FloatVector fv -> {
                for (int r = validity.nextSetBit(0); r >= 0; r = validity.nextSetBit(r + 1)) {
                    if (accept.test(ValueComparison.compareFloat(fv.getFloat(r), v))) {
                        out.set(r);
                    }
                }
            }
            case BooleanVector bvec -> {
                for (int r = validity.nextSetBit(0); r >= 0; r = validity.nextSetBit(r + 1)) {
                    if (accept.test(ValueComparison.compareBoolean(bvec.getBoolean(r), v))) {
                        out.set(r);
                    }
                }
            }
            case BinaryVector bin -> {
                for (int r = validity.nextSetBit(0); r >= 0; r = validity.nextSetBit(r + 1)) {
                    if (accept.test(ValueComparison.compareBinary(bin.get(r), v))) {
                        out.set(r);
                    }
                }
            }
            case FixedLenBinaryVector fixed -> {
                for (int r = validity.nextSetBit(0); r >= 0; r = validity.nextSetBit(r + 1)) {
                    if (accept.test(ValueComparison.compareBinary(fixed.get(r), v))) {
                        out.set(r);
                    }
                }
            }
            case Int96Vector int96 -> {
                for (int r = validity.nextSetBit(0); r >= 0; r = validity.nextSetBit(r + 1)) {
                    if (accept.test(ValueComparison.compareBinary(int96.get(r), v))) {
                        out.set(r);
                    }
                }
            }
            default -> {
                /* nested leaf kind (list/map/struct/variant): no scalar comparison */
            }
        }
        return out;
    }

    private static BitSet inMask(ParquetRecordBatch batch, ColumnPath col, List<Value> values) {
        BitSet acc = new BitSet(batch.rowCount());
        for (Value v : values) {
            acc.or(compareMask(batch, col, v, c -> c == 0));
        }
        return acc;
    }

    private static BitSet spatialMask(
            ParquetRecordBatch batch, Predicate.Spatial spatial, int rowCount, boolean negated) {
        return spatialMask(batch, spatial, rowCount, negated, null);
    }

    private static BitSet spatialMask(
            ParquetRecordBatch batch, Predicate.Spatial spatial, int rowCount, boolean negated, BitSet restriction) {
        ColumnVector vec = batch.columns().get(spatial.col());
        BitSet out = new BitSet(rowCount);
        if (vec instanceof BinaryVector wkb) {
            BitSet candidates = candidateRows(vec.validity(), restriction);
            for (int r = candidates.nextSetBit(0); r >= 0; r = candidates.nextSetBit(r + 1)) {
                boolean hit = WkbEnvelope.matches(spatial, wkb.get(r));
                if (hit != negated) {
                    out.set(r);
                }
            }
        }
        return out;
    }

    private static BitSet geometryMask(
            ParquetRecordBatch batch, GeometryFilter<?> filter, int rowCount, boolean negated) {
        return geometryMask(batch, filter, rowCount, negated, null);
    }

    private static BitSet geometryMask(
            ParquetRecordBatch batch, GeometryFilter<?> filter, int rowCount, boolean negated, BitSet restriction) {
        ColumnVector vec = batch.columns().get(filter.column());
        BitSet out = new BitSet(rowCount);
        if (vec instanceof BinaryVector wkb) {
            BitSet candidates = candidateRows(vec.validity(), restriction);
            for (int r = candidates.nextSetBit(0); r >= 0; r = candidates.nextSetBit(r + 1)) {
                MemorySegment seg = wkb.get(r);
                boolean hit = filter.gate(seg).isPresent();
                if (hit != negated) {
                    out.set(r);
                }
            }
        }
        return out;
    }
}
