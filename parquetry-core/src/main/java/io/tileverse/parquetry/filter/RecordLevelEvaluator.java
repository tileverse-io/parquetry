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
package io.tileverse.parquetry.filter;

import java.lang.foreign.MemorySegment;
import java.util.List;

import io.tileverse.parquetry.filter.spatial.WkbEnvelope;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Tier-5 (RECORD_LEVEL) evaluator. Tests a single assembled record against a normalized {@link Predicate}, returning
 * {@code true} if the record passes.
 *
 * <p>NULL comparisons follow SQL WHERE semantics collapsed to boolean: a value comparison against NULL evaluates to
 * {@code false} (not NULL). {@link Predicate.IsNull} and {@link Predicate.IsNotNull} are the only predicates whose
 * truth value depends on null presence.
 *
 * <p>Assumes the predicate has been normalized (Not pushed to leaves, Always folded, And/Or flattened).
 */
public final class RecordLevelEvaluator {

    private RecordLevelEvaluator() {}

    /**
     * Accessor for a single assembled record's column values, as seen by the record-level filter evaluator. The boxed
     * return is one of: Boolean / Integer / Long / Float / Double / String / {@link MemorySegment} (for binary / INT96)
     * / java.time.LocalDate / java.time.LocalDateTime, or {@code null} when the column is NULL for this row.
     */
    @FunctionalInterface
    public interface RecordAccessor {

        Object value(ColumnPath path);
    }

    /** Returns {@code true} if {@code row} satisfies {@code predicate}. */
    public static boolean test(Predicate predicate, RecordAccessor row) {
        return switch (predicate) {
            case Predicate.Always(boolean value) -> value;
            case Predicate.And(List<Predicate> children) -> testAnd(children, row);
            case Predicate.Or(List<Predicate> children) -> testOr(children, row);
            case Predicate.Not(Predicate.Spatial spatial) -> negatedSpatialHolds(spatial, row);
            case Predicate.Not(Predicate.GeometryFilterPredicate(GeometryFilter<?> filter)) ->
                negatedGeometryFilterHolds(filter, row);
            case Predicate.Not(Predicate child) -> !test(child, row);
            case Predicate.Eq(ColumnPath col, Value v) -> ValueComparison.compareBoxed(row.value(col), v) == 0;
            case Predicate.NotEq(ColumnPath col, Value v) -> {
                Object got = row.value(col);
                yield got != null && ValueComparison.compareBoxed(got, v) != 0;
            }
            case Predicate.Lt(ColumnPath col, Value v) -> {
                Object got = row.value(col);
                yield got != null && ValueComparison.compareBoxed(got, v) < 0;
            }
            case Predicate.LtEq(ColumnPath col, Value v) -> {
                Object got = row.value(col);
                yield got != null && ValueComparison.compareBoxed(got, v) <= 0;
            }
            case Predicate.Gt(ColumnPath col, Value v) -> {
                Object got = row.value(col);
                yield got != null && ValueComparison.compareBoxed(got, v) > 0;
            }
            case Predicate.GtEq(ColumnPath col, Value v) -> {
                Object got = row.value(col);
                yield got != null && ValueComparison.compareBoxed(got, v) >= 0;
            }
            case Predicate.In(ColumnPath col, List<Value> values) -> {
                Object got = row.value(col);
                yield got != null && values.stream().anyMatch(v -> ValueComparison.compareBoxed(got, v) == 0);
            }
            case Predicate.IsNull(ColumnPath col) -> row.value(col) == null;
            case Predicate.IsNotNull(ColumnPath col) -> row.value(col) != null;
            case Predicate.Spatial spatial -> spatialHolds(spatial, row);
            case Predicate.GeometryFilterPredicate(GeometryFilter<?> filter) -> geometryFilterHolds(filter, row);
        };
    }

    private static boolean testAnd(List<Predicate> children, RecordAccessor row) {
        for (Predicate child : children) {
            if (!test(child, row)) {
                return false;
            }
        }
        return true;
    }

    private static boolean testOr(List<Predicate> children, RecordAccessor row) {
        for (Predicate child : children) {
            if (test(child, row)) {
                return true;
            }
        }
        return false;
    }

    private static boolean spatialHolds(Predicate.Spatial spatial, RecordAccessor row) {
        Object geometry = row.value(spatial.col());
        return geometry != null && WkbEnvelope.matches(spatial, (MemorySegment) geometry);
    }

    /**
     * Negation of a spatial relation under OGC simple-feature semantics: a null geometry has no spatial truth value; it
     * is excluded from both a spatial predicate and its negation rather than being flipped into a match.
     */
    private static boolean negatedSpatialHolds(Predicate.Spatial spatial, RecordAccessor row) {
        Object geometry = row.value(spatial.col());
        return geometry != null && !WkbEnvelope.matches(spatial, (MemorySegment) geometry);
    }

    private static boolean geometryFilterHolds(GeometryFilter<?> filter, RecordAccessor row) {
        MemorySegment wkb = wkbValue(filter, row);
        if (wkb == null) {
            return false;
        }
        return filter.gate(wkb).isPresent();
    }

    /**
     * Negation of a geometry filter under OGC simple-feature semantics: a null geometry has no spatial truth value and
     * is excluded from both the filter and its negation. An empty geometry whose exact test is a definite false is
     * dropped by the filter and kept by its negation - that follows from the filter's own
     * {@link GeometryFilter#matches} result.
     */
    private static boolean negatedGeometryFilterHolds(GeometryFilter<?> filter, RecordAccessor row) {
        MemorySegment wkb = wkbValue(filter, row);
        if (wkb == null) {
            return false;
        }
        return filter.gate(wkb).isEmpty();
    }

    private static MemorySegment wkbValue(GeometryFilter<?> filter, RecordAccessor row) {
        Object value = row.value(filter.column());
        if (value instanceof MemorySegment wkb) {
            return wkb;
        }
        return null;
    }
}
