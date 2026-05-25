/*
 * Copyright (c) 2026 Tileverse.io
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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Sealed predicate ADT for filter pushdown.
 *
 * <p>Use pattern matching on the variants to evaluate or transform. The {@code Pred} fluent builder emits these
 * records; the normalizer and the 5-tier evaluator pipeline consume them.
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
     * Returns the leaf column paths referenced by {@code predicate}, in encounter order. The read path uses this to
     * decode those columns for record-level evaluation even when they fall outside the caller's projection.
     */
    static Set<ColumnPath> columns(Predicate predicate) {
        Set<ColumnPath> columns = new LinkedHashSet<>();
        collectColumns(predicate, columns);
        return columns;
    }

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
            case BboxIntersects bbox -> columns.add(bbox.col());
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

    record BboxIntersects(ColumnPath col, Bbox bbox) implements Predicate {}
}
