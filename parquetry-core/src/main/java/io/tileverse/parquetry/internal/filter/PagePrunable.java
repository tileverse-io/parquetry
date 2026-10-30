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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * The part of a normalized predicate evaluable from the statistics of each page: comparisons, set membership and null
 * tests of a column, combined by And and Or. A conjunct out of reach is dropped, which widens the match and keeps the
 * pruning sound. A disjunction with a branch out of reach can match any row, and has no prunable part.
 *
 * <p>Out of reach: a negation, because the complement of the page ranges of its child would drop matching rows; a
 * spatial relation and a geometry filter, with no page statistics of their own; a row-position delete, whose column has
 * no column index; a quantified leaf, because the elements of a list do not line up with the row ranges of the pages.
 */
public final class PagePrunable {

    private PagePrunable() {}

    /** The part of {@code normalized} evaluable from page statistics, empty when there is none. */
    public static Optional<Predicate> part(Predicate normalized) {
        return switch (normalized) {
            case Predicate.And(List<Predicate> children) -> conjunctionPart(children);
            case Predicate.Or(List<Predicate> children) -> disjunctionPart(children);
            case Predicate.Always _,
                    Predicate.Eq _,
                    Predicate.NotEq _,
                    Predicate.Lt _,
                    Predicate.LtEq _,
                    Predicate.Gt _,
                    Predicate.GtEq _,
                    Predicate.In _,
                    Predicate.IsNull _,
                    Predicate.IsNotNull _ -> Optional.of(normalized);
            case Predicate.Not _,
                    Predicate.Spatial _,
                    Predicate.GeometryFilterPredicate _,
                    Predicate.RowIndexExcluded _,
                    Predicate.Quantified _ -> Optional.empty();
        };
    }

    /** The column of each leaf of {@code prunable}, in encounter order. */
    public static List<ColumnPath> columns(Predicate prunable) {
        List<ColumnPath> columns = new ArrayList<>();
        collectColumns(prunable, columns);
        return columns;
    }

    private static Optional<Predicate> conjunctionPart(List<Predicate> children) {
        List<Predicate> parts = new ArrayList<>(children.size());
        for (Predicate child : children) {
            part(child).ifPresent(parts::add);
        }
        if (parts.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new Predicate.And(parts));
    }

    private static Optional<Predicate> disjunctionPart(List<Predicate> children) {
        List<Predicate> parts = new ArrayList<>(children.size());
        for (Predicate child : children) {
            Optional<Predicate> part = part(child);
            if (part.isEmpty()) {
                return Optional.empty();
            }
            parts.add(part.orElseThrow());
        }
        return Optional.of(new Predicate.Or(parts));
    }

    private static void collectColumns(Predicate predicate, List<ColumnPath> columns) {
        switch (predicate) {
            case Predicate.And and -> and.children().forEach(child -> collectColumns(child, columns));
            case Predicate.Or or -> or.children().forEach(child -> collectColumns(child, columns));
            case Predicate.Eq eq -> columns.add(eq.col());
            case Predicate.NotEq notEq -> columns.add(notEq.col());
            case Predicate.Lt lt -> columns.add(lt.col());
            case Predicate.LtEq ltEq -> columns.add(ltEq.col());
            case Predicate.Gt gt -> columns.add(gt.col());
            case Predicate.GtEq gtEq -> columns.add(gtEq.col());
            case Predicate.In in -> columns.add(in.col());
            case Predicate.IsNull isNull -> columns.add(isNull.col());
            case Predicate.IsNotNull isNotNull -> columns.add(isNotNull.col());
            case Predicate.Always _,
                    Predicate.Not _,
                    Predicate.Spatial _,
                    Predicate.GeometryFilterPredicate _,
                    Predicate.RowIndexExcluded _,
                    Predicate.Quantified _ -> {
                // no column: a constant, or a leaf out of reach
            }
        }
    }
}
