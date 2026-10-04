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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import io.tileverse.parquetry.internal.filter.ComparisonOperator;
import io.tileverse.parquetry.internal.filter.PredicateNormalizer;
import io.tileverse.parquetry.internal.filter.ValueComparison;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Replaces predicate leaves on synthesized (constant) columns with their truth value for one file, then simplifies the
 * boolean tree. A synthesized column holds the same value in every row of a file, which makes a leaf on it a constant
 * true or false; the engine then never validates or filters a column it cannot see.
 */
public final class ConstantFolding {

    private ConstantFolding() {}

    /**
     * Folds the normalized form of {@code predicate}, the form evaluated by the read path. A negated comparison then
     * folds as it filters a physical column: {@code NOT (c < 3)} becomes {@code c >= 3}, false for a null or NaN value,
     * rather than the negation of a false leaf.
     *
     * @param constants synthesized column path -&gt; its constant value for this file
     * @param nulls synthesized columns whose value is null for this file, such as a column added to an Iceberg table
     *     after the file was written
     */
    public static Predicate fold(Predicate predicate, Map<ColumnPath, Value> constants, Set<ColumnPath> nulls) {
        return foldNormalized(PredicateNormalizer.normalize(predicate), constants, nulls);
    }

    @SuppressWarnings("java:S6878") // comparison arms reuse the whole leaf value, not just its components
    private static Predicate foldNormalized(
            Predicate predicate, Map<ColumnPath, Value> constants, Set<ColumnPath> nulls) {
        return switch (predicate) {
            case Predicate.And(List<Predicate> children) -> foldAnd(children, constants, nulls);
            case Predicate.Or(List<Predicate> children) -> foldOr(children, constants, nulls);
            case Predicate.Not(Predicate child) -> negate(foldNormalized(child, constants, nulls));
            case Predicate.Eq leaf -> foldLeaf(leaf, leaf.col(), constants, nulls, ComparisonOperator.EQ);
            case Predicate.NotEq leaf -> foldLeaf(leaf, leaf.col(), constants, nulls, ComparisonOperator.NOT_EQ);
            case Predicate.Lt leaf -> foldLeaf(leaf, leaf.col(), constants, nulls, ComparisonOperator.LT);
            case Predicate.LtEq leaf -> foldLeaf(leaf, leaf.col(), constants, nulls, ComparisonOperator.LT_EQ);
            case Predicate.Gt leaf -> foldLeaf(leaf, leaf.col(), constants, nulls, ComparisonOperator.GT);
            case Predicate.GtEq leaf -> foldLeaf(leaf, leaf.col(), constants, nulls, ComparisonOperator.GT_EQ);
            case Predicate.In in -> foldIn(in, constants, nulls);
            case Predicate.IsNull isNull -> foldIsNull(isNull.col(), constants, nulls, true);
            case Predicate.IsNotNull isNotNull -> foldIsNull(isNotNull.col(), constants, nulls, false);
            default -> predicate;
        };
    }

    private static Predicate foldLeaf(
            Predicate leaf,
            ColumnPath col,
            Map<ColumnPath, Value> constants,
            Set<ColumnPath> nulls,
            ComparisonOperator op) {
        if (nulls.contains(col)) {
            return Predicate.ALWAYS_FALSE;
        }
        Value constant = constants.get(col);
        if (constant == null) {
            return leaf;
        }
        Value bound = boundOf(leaf);
        return holdsWidening(op, constant, bound) ? Predicate.ALWAYS_TRUE : Predicate.ALWAYS_FALSE;
    }

    /**
     * Whether {@code op} holds between a synthetic column's constant and a predicate literal, widening across numeric
     * subtypes not bridged by {@link ValueComparison#compareValues}. A constant inferred as {@code LONG} compared
     * against an {@code INT} literal (or the reverse) would otherwise fall to the {@code default -> 0} arm and
     * mis-fold. When both sides are numeric they compare as a common widened type: {@code double} under the IEEE 754
     * rules of the record level if either side is floating, else {@code long}. Non-numeric pairs
     * (String/Date/Bool/Uuid/...) defer to {@code compareValues}.
     */
    private static boolean holdsWidening(ComparisonOperator op, Value constant, Value bound) {
        if (isNumeric(constant) && isNumeric(bound)) {
            if (isFloating(constant) || isFloating(bound)) {
                return op.holds(asDouble(constant), asDouble(bound));
            }
            return op.holds(Long.compare(asLong(constant), asLong(bound)));
        }
        return op.holds(ValueComparison.compareValues(constant, bound));
    }

    private static boolean isNumeric(Value value) {
        return value instanceof Value.IntVal
                || value instanceof Value.LongVal
                || value instanceof Value.FloatVal
                || value instanceof Value.DoubleVal;
    }

    private static boolean isFloating(Value value) {
        return value instanceof Value.FloatVal || value instanceof Value.DoubleVal;
    }

    private static long asLong(Value value) {
        return switch (value) {
            case Value.IntVal(int v) -> v;
            case Value.LongVal(long v) -> v;
            default -> throw new IllegalStateException("not an integral value: " + value);
        };
    }

    private static double asDouble(Value value) {
        return switch (value) {
            case Value.IntVal(int v) -> v;
            case Value.LongVal(long v) -> v;
            case Value.FloatVal(float v) -> v;
            case Value.DoubleVal(double v) -> v;
            default -> throw new IllegalStateException("not a numeric value: " + value);
        };
    }

    private static Value boundOf(Predicate leaf) {
        return switch (leaf) {
            case Predicate.Eq p -> p.v();
            case Predicate.NotEq p -> p.v();
            case Predicate.Lt p -> p.v();
            case Predicate.LtEq p -> p.v();
            case Predicate.Gt p -> p.v();
            case Predicate.GtEq p -> p.v();
            default -> throw new IllegalStateException("not a comparison leaf: " + leaf);
        };
    }

    private static Predicate foldIn(Predicate.In in, Map<ColumnPath, Value> constants, Set<ColumnPath> nulls) {
        if (nulls.contains(in.col())) {
            return Predicate.ALWAYS_FALSE;
        }
        Value constant = constants.get(in.col());
        if (constant == null) {
            return in;
        }
        for (Value candidate : in.values()) {
            if (holdsWidening(ComparisonOperator.EQ, constant, candidate)) {
                return Predicate.ALWAYS_TRUE;
            }
        }
        return Predicate.ALWAYS_FALSE;
    }

    private static Predicate foldIsNull(
            ColumnPath col, Map<ColumnPath, Value> constants, Set<ColumnPath> nulls, boolean wantNull) {
        if (nulls.contains(col)) {
            return wantNull ? Predicate.ALWAYS_TRUE : Predicate.ALWAYS_FALSE;
        }
        if (constants.containsKey(col)) {
            return wantNull ? Predicate.ALWAYS_FALSE : Predicate.ALWAYS_TRUE;
        }
        return wantNull ? new Predicate.IsNull(col) : new Predicate.IsNotNull(col);
    }

    private static Predicate foldAnd(
            List<Predicate> children, Map<ColumnPath, Value> constants, Set<ColumnPath> nulls) {
        List<Predicate> kept = new ArrayList<>();
        for (Predicate child : children) {
            Predicate folded = foldNormalized(child, constants, nulls);
            if (folded.equals(Predicate.ALWAYS_FALSE)) {
                return Predicate.ALWAYS_FALSE;
            }
            if (!folded.equals(Predicate.ALWAYS_TRUE)) {
                kept.add(folded);
            }
        }
        return collapse(kept, Predicate.ALWAYS_TRUE, Predicate.And::new);
    }

    private static Predicate foldOr(List<Predicate> children, Map<ColumnPath, Value> constants, Set<ColumnPath> nulls) {
        List<Predicate> kept = new ArrayList<>();
        for (Predicate child : children) {
            Predicate folded = foldNormalized(child, constants, nulls);
            if (folded.equals(Predicate.ALWAYS_TRUE)) {
                return Predicate.ALWAYS_TRUE;
            }
            if (!folded.equals(Predicate.ALWAYS_FALSE)) {
                kept.add(folded);
            }
        }
        return collapse(kept, Predicate.ALWAYS_FALSE, Predicate.Or::new);
    }

    private static Predicate collapse(
            List<Predicate> kept, Predicate identity, Function<List<Predicate>, Predicate> wrap) {
        if (kept.isEmpty()) {
            return identity;
        }
        if (kept.size() == 1) {
            return kept.get(0);
        }
        return wrap.apply(kept);
    }

    private static Predicate negate(Predicate p) {
        if (p.equals(Predicate.ALWAYS_TRUE)) {
            return Predicate.ALWAYS_FALSE;
        }
        if (p.equals(Predicate.ALWAYS_FALSE)) {
            return Predicate.ALWAYS_TRUE;
        }
        return new Predicate.Not(p);
    }
}
