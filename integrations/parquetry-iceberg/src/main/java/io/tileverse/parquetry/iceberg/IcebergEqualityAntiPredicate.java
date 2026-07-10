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
package io.tileverse.parquetry.iceberg;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Builds the predicate that keeps every data row an Iceberg equality-delete file does NOT match. An equality-delete
 * file lists tuples to drop, one value per equality field; a data row is deleted when it matches a tuple on every
 * equality field. The keep predicate is the conjunction over tuples of "this row does not match this tuple":
 *
 * <pre>AND over tuples of ( OR over equality fields of keep-component )</pre>
 *
 * <p>The keep predicate is built directly in already-negated form rather than as {@code NOT(matches-some-tuple)}.
 * Iceberg equality is null-safe (IS NOT DISTINCT FROM) while parquetry comparison leaves are SQL-null, and the
 * normalizer's Not-pushdown would not preserve the null-safe meaning. A row keeps a tuple when at least one field fails
 * to match it:
 *
 * <ul>
 *   <li>against a non-null tuple value {@code v}, the row's field fails to match when its cell IS DISTINCT FROM
 *       {@code v}: the cell is null, or the cell differs from {@code v}. That is {@code IsNull(col) OR NotEq(col, v)}.
 *       A bare {@code NotEq} would be wrong, because under SQL-null {@code NotEq} is not true for a null cell, yet a
 *       null cell does fail to match a non-null tuple value.
 *   <li>against a null tuple value, the row's field fails to match when its cell is not null: {@code IsNotNull(col)}.
 * </ul>
 *
 * <p>Conjoining the per-tuple keep clauses leaves the row only when it fails to match every tuple, which is exactly the
 * set of live rows.
 */
final class IcebergEqualityAntiPredicate {

    private IcebergEqualityAntiPredicate() {}

    /**
     * The keep predicate for {@code tuples} over {@code columns}, or empty when the delete file lists no tuples
     * (nothing to drop).
     *
     * @param columns the table column path of each equality field, in equality-id order
     * @param tuples one delete tuple per row; each tuple holds one value per column, in {@code columns} order, with a
     *     null element meaning the field's value is null in that tuple
     */
    static Optional<Predicate> build(List<ColumnPath> columns, List<List<Value>> tuples) {
        if (tuples.isEmpty()) {
            return Optional.empty();
        }
        List<Predicate> keepClauses = new ArrayList<>(tuples.size());
        for (List<Value> tuple : tuples) {
            keepClauses.add(keepUnlessMatches(columns, tuple));
        }
        return Optional.of(allOf(keepClauses));
    }

    private static Predicate keepUnlessMatches(List<ColumnPath> columns, List<Value> tuple) {
        List<Predicate> fieldMismatches = new ArrayList<>(columns.size());
        for (int field = 0; field < columns.size(); field++) {
            fieldMismatches.add(distinctFrom(columns.get(field), tuple.get(field)));
        }
        return anyOf(fieldMismatches);
    }

    private static Predicate distinctFrom(ColumnPath column, Value value) {
        if (value == null) {
            return new Predicate.IsNotNull(column);
        }
        return new Predicate.IsNull(column).or(new Predicate.NotEq(column, value));
    }

    private static Predicate allOf(List<Predicate> clauses) {
        if (clauses.size() == 1) {
            return clauses.get(0);
        }
        return new Predicate.And(clauses);
    }

    private static Predicate anyOf(List<Predicate> clauses) {
        if (clauses.size() == 1) {
            return clauses.get(0);
        }
        return new Predicate.Or(clauses);
    }
}
