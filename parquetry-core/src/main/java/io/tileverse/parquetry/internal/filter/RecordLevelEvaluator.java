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

import java.lang.foreign.MemorySegment;
import java.util.List;

import io.tileverse.parquetry.filter.GeometryFilter;
import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowPositionSet;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.internal.filter.spatial.WkbEnvelope;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Tier-5 (RECORD_LEVEL) evaluator. Tests a single assembled record against a normalized {@link Predicate}, returning
 * {@code true} if the record passes.
 *
 * <p>NULL comparisons follow SQL WHERE semantics collapsed to boolean: a value comparison against NULL evaluates to
 * {@code false} (not NULL). {@link Predicate.IsNull} and {@link Predicate.IsNotNull} are the only predicates whose
 * truth value depends on null presence. Floating-point comparisons follow IEEE 754, as documented on {@link Predicate}.
 *
 * <p>Assumes the predicate has been normalized (Not pushed to leaves, Always folded, And/Or flattened).
 */
public final class RecordLevelEvaluator {

    private RecordLevelEvaluator() {}

    /**
     * Accessor for a single assembled record's column values, as seen by the record-level filter evaluator. The boxed
     * return is the RAW physical value: Boolean / Integer / Long / Float / Double / {@link MemorySegment} (for binary,
     * FIXED_LEN_BYTE_ARRAY, and INT96), or {@code null} when the column is NULL for this row. Logical interpretation -
     * reading a DATE's INT32 as a calendar date, or a FIXED_LEN_BYTE_ARRAY UUID as a {@link java.util.UUID} - is the
     * comparison layer's job in {@link ValueComparison}, not this accessor's.
     */
    public interface RecordAccessor {

        Object value(ColumnPath path);

        /**
         * Flattened, NULL-FREE element values at a repeated leaf, for {@link Predicate.Quantified}. Empty when none.
         * Null elements are excluded; a quantified evaluator that needs the full universe size uses
         * {@link #multiValueSize(ColumnPath)}.
         */
        List<Object> multiValue(ColumnPath leafPath);

        /**
         * The size of the repeated leaf's universe INCLUDING null elements. A null element is a present member of the
         * universe under ANY, ALL and ONE: it matches {@code IS NULL} and no comparison. The default falls back to the
         * null-free count from {@link #multiValue(ColumnPath)}; accessors that can see null elements override this to
         * include them.
         */
        default int multiValueSize(ColumnPath leafPath) {
            return multiValue(leafPath).size();
        }
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
            case Predicate.Eq(ColumnPath col, Value v) ->
                ValueComparison.holds(ComparisonOperator.EQ, row.value(col), v);
            case Predicate.NotEq(ColumnPath col, Value v) ->
                ValueComparison.holds(ComparisonOperator.NOT_EQ, row.value(col), v);
            case Predicate.Lt(ColumnPath col, Value v) ->
                ValueComparison.holds(ComparisonOperator.LT, row.value(col), v);
            case Predicate.LtEq(ColumnPath col, Value v) ->
                ValueComparison.holds(ComparisonOperator.LT_EQ, row.value(col), v);
            case Predicate.Gt(ColumnPath col, Value v) ->
                ValueComparison.holds(ComparisonOperator.GT, row.value(col), v);
            case Predicate.GtEq(ColumnPath col, Value v) ->
                ValueComparison.holds(ComparisonOperator.GT_EQ, row.value(col), v);
            case Predicate.In(ColumnPath col, List<Value> values) -> {
                Object got = row.value(col);
                yield values.stream().anyMatch(v -> ValueComparison.holds(ComparisonOperator.EQ, got, v));
            }
            case Predicate.IsNull(ColumnPath col) -> row.value(col) == null;
            case Predicate.IsNotNull(ColumnPath col) -> row.value(col) != null;
            case Predicate.Spatial spatial -> spatialHolds(spatial, row);
            case Predicate.GeometryFilterPredicate(GeometryFilter<?> filter) -> geometryFilterHolds(filter, row);
            case Predicate.RowIndexExcluded(ColumnPath col, RowPositionSet deleted) ->
                rowPositionSurvives(col, deleted, row);
            case Predicate.Quantified(MatchAction match, Predicate leaf) -> testQuantified(match, leaf, row);
        };
    }

    /**
     * Keeps a row only when its synthesized absolute position is not a deleted position. The position is a non-null
     * {@code long} the reader materializes at the predicate's {@code col} path, the synthesized row-position column the
     * caller named; an absent value means the read did not synthesize the column the predicate requires.
     */
    private static boolean rowPositionSurvives(ColumnPath col, RowPositionSet deleted, RecordAccessor row) {
        Object position = row.value(col);
        if (!(position instanceof Long absolutePosition)) {
            throw new IllegalStateException(
                    "row-position column " + col.dot() + " is not present as a long for a RowIndexExcluded predicate");
        }
        return !deleted.contains(absolutePosition);
    }

    private static boolean testQuantified(MatchAction match, Predicate leaf, RecordAccessor row) {
        ColumnPath leafColumn = Predicate.columns(leaf).iterator().next();
        List<Object> elements = row.multiValue(leafColumn);
        int universeSize = row.multiValueSize(leafColumn);
        int matches = matchingNullElements(leaf, universeSize - elements.size());
        for (Object element : elements) {
            if (test(leaf, singletonAccessor(leafColumn, element))) {
                matches++;
            }
        }
        return switch (match) {
            case ANY -> matches > 0;
            case ALL -> matches == universeSize;
            case ONE -> matches == 1;
        };
    }

    /**
     * The null elements matching {@code leaf}: all of them or none. A null element is tested in the three-valued logic
     * of SQL: it matches {@code IS NULL}, it fails {@code IS NOT NULL}, and a comparison with it is unknown.
     */
    private static int matchingNullElements(Predicate leaf, int nullElements) {
        if (truthForNull(leaf) == Truth.TRUE) {
            return nullElements;
        }
        return 0;
    }

    /** The truth of {@code leaf} for a null element. An unknown truth matches no element. */
    private static Truth truthForNull(Predicate leaf) {
        return switch (leaf) {
            case Predicate.IsNull _ -> Truth.TRUE;
            case Predicate.IsNotNull _ -> Truth.FALSE;
            case Predicate.Always(boolean value) -> Truth.of(value);
            case Predicate.Not(Predicate child) -> truthForNull(child).negated();
            case Predicate.And(List<Predicate> children) -> Truth.allOf(truthsForNull(children));
            case Predicate.Or(List<Predicate> children) -> Truth.anyOf(truthsForNull(children));
            default -> Truth.UNKNOWN;
        };
    }

    private static List<Truth> truthsForNull(List<Predicate> children) {
        return children.stream().map(RecordLevelEvaluator::truthForNull).toList();
    }

    /** A truth value of SQL. A comparison with a null is {@link #UNKNOWN}. */
    private enum Truth {
        TRUE,
        FALSE,
        UNKNOWN;

        static Truth of(boolean value) {
            return value ? TRUE : FALSE;
        }

        Truth negated() {
            return switch (this) {
                case TRUE -> FALSE;
                case FALSE -> TRUE;
                case UNKNOWN -> UNKNOWN;
            };
        }

        /** FALSE when one truth is FALSE, otherwise UNKNOWN when one is UNKNOWN, otherwise TRUE. */
        static Truth allOf(List<Truth> truths) {
            if (truths.contains(FALSE)) {
                return FALSE;
            }
            if (truths.contains(UNKNOWN)) {
                return UNKNOWN;
            }
            return TRUE;
        }

        /** TRUE when one truth is TRUE, otherwise UNKNOWN when one is UNKNOWN, otherwise FALSE. */
        static Truth anyOf(List<Truth> truths) {
            if (truths.contains(TRUE)) {
                return TRUE;
            }
            if (truths.contains(UNKNOWN)) {
                return UNKNOWN;
            }
            return FALSE;
        }
    }

    private static RecordAccessor singletonAccessor(ColumnPath leafColumn, Object element) {
        return new RecordAccessor() {
            @Override
            public Object value(ColumnPath path) {
                return path.equals(leafColumn) ? element : null;
            }

            @Override
            public List<Object> multiValue(ColumnPath path) {
                return List.of();
            }
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
