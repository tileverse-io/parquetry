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
package io.tileverse.parquetry.filter;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Sealed predicate ADT for filter pushdown.
 *
 * <p>Use pattern matching on the cases to evaluate or transform. The {@code Pred} fluent builder emits these records;
 * the normalizer and the 5-tier evaluator pipeline consume them.
 */
public sealed interface Predicate {

    Predicate ALWAYS_TRUE = new Always(true);
    Predicate ALWAYS_FALSE = new Always(false);

    /** Returns {@code this AND other} as a new {@link And} predicate. */
    default Predicate and(Predicate other) {
        return new And(List.of(this, other));
    }

    /** Returns {@code this OR other} as a new {@link Or} predicate. */
    default Predicate or(Predicate other) {
        return new Or(List.of(this, other));
    }

    /** Returns {@code NOT this} as a new {@link Not} predicate. */
    default Predicate negate() {
        return new Not(this);
    }

    /**
     * Returns a predicate that applies an exact {@link GeometryFilter} at record level after coarse bbox pruning. The
     * filter supplies its own sound bbox lowering through {@link GeometryFilter#pruningPredicate()}; the reader prunes
     * row groups and pages with that lowering, then runs the exact {@link GeometryFilter#gate} on each surviving row.
     */
    static Predicate geometryFilter(GeometryFilter<?> filter) {
        return new GeometryFilterPredicate(filter);
    }

    /**
     * Returns the leaf column paths referenced by {@code predicate}, in encounter order. The read path uses this to
     * decode those columns for record-level evaluation even when they fall outside the caller's projection.
     */
    static Set<ColumnPath> columns(Predicate predicate) {
        Set<ColumnPath> columns = new LinkedHashSet<>();
        collectColumns(predicate, columns);
        return columns;
    }

    /*
     * palantirJavaFormat 2.90.0 doesn't support unnamed record patterns
     * in records (e.g. case Eq(ColumnPath col, _) ->
     */
    @SuppressWarnings("java:S6878")
    private static void collectColumns(Predicate predicate, Set<ColumnPath> columns) {
        switch (predicate) {
            case Always _ -> {
                /* no column */
            }
            case And and -> and.children().forEach(child -> collectColumns(child, columns));
            case Or or -> or.children().forEach(child -> collectColumns(child, columns));
            case Not not -> collectColumns(not.child(), columns);
            case Eq eq -> columns.add(eq.col());
            case NotEq notEq -> columns.add(notEq.col());
            case Lt lt -> columns.add(lt.col());
            case LtEq ltEq -> columns.add(ltEq.col());
            case Gt gt -> columns.add(gt.col());
            case GtEq gtEq -> columns.add(gtEq.col());
            case In in -> columns.add(in.col());
            case IsNull isNull -> columns.add(isNull.col());
            case IsNotNull isNotNull -> columns.add(isNotNull.col());
            case Spatial s -> columns.add(s.col());
            case GeometryFilterPredicate gfp -> columns.add(gfp.filter().column());
            case Quantified q -> collectColumns(q.leaf(), columns);
            case RowIndexExcluded rowIndexExcluded -> columns.add(rowIndexExcluded.column());
        }
    }

    /**
     * The distinct columns named by {@link RowIndexExcluded} leaves: the synthesized row-position columns this
     * predicate filters on, in encounter order. Empty when the predicate has no row-position delete. The reader
     * produces a column at each of these paths rather than decoding it from the file, and the caller chooses the name
     * when building the predicate (Iceberg uses {@code _pos}).
     */
    static List<ColumnPath> rowPositionColumns(Predicate predicate) {
        List<ColumnPath> columns = new ArrayList<>();
        collectRowPositionColumns(predicate, columns);
        return columns;
    }

    @SuppressWarnings("java:S6878") // palantirJavaFormat 2.90.0 rejects unnamed record-pattern components
    private static void collectRowPositionColumns(Predicate predicate, List<ColumnPath> columns) {
        switch (predicate) {
            case And and -> and.children().forEach(child -> collectRowPositionColumns(child, columns));
            case Or or -> or.children().forEach(child -> collectRowPositionColumns(child, columns));
            case Not not -> collectRowPositionColumns(not.child(), columns);
            case Quantified q -> collectRowPositionColumns(q.leaf(), columns);
            case RowIndexExcluded rowIndexExcluded -> {
                if (!columns.contains(rowIndexExcluded.column())) {
                    columns.add(rowIndexExcluded.column());
                }
            }
            case Always _,
                    Eq _,
                    NotEq _,
                    Lt _,
                    LtEq _,
                    Gt _,
                    GtEq _,
                    In _,
                    IsNull _,
                    IsNotNull _,
                    Spatial _,
                    GeometryFilterPredicate _ -> {
                /* not a row-position column */
            }
        }
    }

    record Always(boolean value) implements Predicate {}

    record Eq(ColumnPath col, Value v) implements Predicate {}

    record NotEq(ColumnPath col, Value v) implements Predicate {}

    record Lt(ColumnPath col, Value v) implements Predicate {}

    record LtEq(ColumnPath col, Value v) implements Predicate {}

    record Gt(ColumnPath col, Value v) implements Predicate {}

    record GtEq(ColumnPath col, Value v) implements Predicate {}

    record In(ColumnPath col, List<Value> values) implements Predicate {
        public In {
            values = List.copyOf(values);
        }
    }

    record IsNull(ColumnPath col) implements Predicate {}

    record IsNotNull(ColumnPath col) implements Predicate {}

    /**
     * A comparison over a repeated (list/map) leaf, aggregated by {@link MatchAction}. The wrapped {@code leaf} is one
     * of the scalar comparison records whose column path is a physical repeated leaf. Single-valued leaves use the bare
     * comparison and are never wrapped.
     */
    record Quantified(MatchAction match, Predicate leaf) implements Predicate {}

    record And(List<Predicate> children) implements Predicate {
        public And {
            children = List.copyOf(children);
        }
    }

    record Or(List<Predicate> children) implements Predicate {
        public Or {
            children = List.copyOf(children);
        }
    }

    record Not(Predicate child) implements Predicate {}

    /**
     * A coordinate-free bbox relation between a geometry column and a query box.
     *
     * <p>Each relation is evaluated in 2D only (X/Y); the Z ordinate is ignored even when the geometry includes it. All
     * edges are treated as inclusive. The evaluation computes the geometry's 2D bounding box from either per-row-group
     * statistics (row-group pruning) or the WKB byte stream (record-level confirmation), and then tests that box
     * against {@link #bbox()}. A row is retained only when both checks agree it satisfies the relation.
     *
     * <p>These predicates filter on the geometry's bounding box, not on the exact geometry shape. True spatial
     * predicates - such as an exact polygon intersection or a topological "within" - belong in the caller: push the
     * envelope as one of these bbox relations for coarse row elimination, then re-apply the exact test against the
     * matched rows using a geometry engine.
     *
     * <p>Pruning is conservative: when no usable bounds exist for a row group, the row group is kept in full and
     * record-level WKB evaluation confirms every row. Wrapping a spatial leaf in {@link Not} disables pruning for that
     * relation (the row group is always kept) and inverts the record-level decision.
     */
    sealed interface Spatial extends Predicate {
        ColumnPath col();

        Bbox bbox();

        /**
         * The geometry's 2D bounding box overlaps the query box. Overlap is true when the two boxes share at least one
         * point (edges inclusive), and false when they are completely disjoint. This is the weakest of the four
         * relations and the most common starting point for GIS query filtering.
         */
        record BboxIntersects(ColumnPath col, Bbox bbox) implements Spatial {}

        /**
         * The geometry's 2D bounding box encloses the query box. That is, every point of the query box lies inside (or
         * on the edge of) the geometry's envelope. Equivalent to {@code geomBbox.contains(queryBbox)} with edges
         * inclusive. A geometry whose bounding box exactly equals the query box satisfies this relation.
         */
        record BboxContains(ColumnPath col, Bbox bbox) implements Spatial {}

        /**
         * The geometry's 2D bounding box is enclosed by the query box. That is, every point of the geometry's envelope
         * lies inside (or on the edge of) the query box. Equivalent to {@code geomBbox.coveredBy(queryBbox)} with edges
         * inclusive. A geometry whose bounding box exactly equals the query box satisfies this relation.
         */
        record BboxCoveredBy(ColumnPath col, Bbox bbox) implements Spatial {}

        /**
         * The geometry's 2D bounding box matches the query box exactly on all four edges: minX, minY, maxX, maxY. Z is
         * ignored. This is the strictest of the four relations; it implies both {@link BboxContains} and
         * {@link BboxCoveredBy}.
         */
        record BboxEquals(ColumnPath col, Bbox bbox) implements Spatial {}
    }

    /**
     * A leaf predicate that delegates exact geometry testing to a {@link GeometryFilter}. The bbox lowering from
     * {@link GeometryFilter#pruningPredicate()} is used for coarse row-group and page pruning;
     * {@link GeometryFilter#gate} performs the exact per-row test.
     */
    record GeometryFilterPredicate(GeometryFilter<?> filter) implements Predicate {}

    /**
     * A leaf that keeps a row only when its absolute file row position is NOT a member of {@code deleted}. This is the
     * already-negated form: the row survives iff {@code deleted.contains(position)} is {@code false}; no {@link Not}
     * ever wraps it. Its {@code column} names a synthesized row-position column the reader produces (the absolute
     * 0-based file position) rather than a physical Parquet leaf; the caller picks the name (Iceberg uses
     * {@code _pos}). The schema-validation pass leaves the column alone and the statistics tiers cannot prune it. The
     * merge-on-read delete path of an Iceberg table lowers its positional deletes into this leaf.
     */
    record RowIndexExcluded(ColumnPath column, RowPositionSet deleted) implements Predicate {
        public RowIndexExcluded {
            Objects.requireNonNull(column, "column");
            Objects.requireNonNull(deleted, "deleted");
        }
    }
}
