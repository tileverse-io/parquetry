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

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import io.tileverse.parquetry.internal.filter.ComparisonOperator;
import io.tileverse.parquetry.internal.filter.PredicateNormalizer;
import io.tileverse.parquetry.internal.filter.ValueComparison;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchemaException;

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
     * @throws ParquetSchemaException when a literal has no order against the constant of its column, such as a string
     *     against a number
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
        return holdsWidening(op, col, constant, bound) ? Predicate.ALWAYS_TRUE : Predicate.ALWAYS_FALSE;
    }

    /**
     * Whether {@code op} holds between the constant of the synthesized column {@code col} and a predicate literal. Two
     * numbers compare as a common widened type: {@code double} under the IEEE 754 rules of the record level if either
     * side is floating, else {@code long}. The other pairs follow {@link #compareOrdered}.
     */
    private static boolean holdsWidening(ComparisonOperator op, ColumnPath col, Value constant, Value bound) {
        if (isNumeric(constant) && isNumeric(bound)) {
            if (isFloating(constant) || isFloating(bound)) {
                return op.holds(asDouble(constant), asDouble(bound));
            }
            return op.holds(Long.compare(asLong(constant), asLong(bound)));
        }
        return op.holds(compareOrdered(col, constant, bound));
    }

    /**
     * The sign of {@code constant - literal}, ordered from either side as a scan orders a stored cell against the
     * literal: epoch days against a date, bytes against a string.
     *
     * @throws ParquetSchemaException when the two types have no order between them. A constant has no stored form to
     *     compare with a literal of its physical type, such as a timestamp against an integer.
     */
    private static int compareOrdered(ColumnPath col, Value constant, Value literal) {
        if (constant instanceof Value.DateVal(LocalDate day) && literal instanceof Value.LongVal(long days)) {
            // An integer literal beyond INT32 reaches a DATE column as a long, and a scan compares it with the days.
            return Long.compare(day.toEpochDay(), days);
        }
        if (ValueComparison.ordered(constant, literal)) {
            return ValueComparison.compareValues(constant, literal);
        }
        if (ValueComparison.ordered(literal, constant)) {
            return -Integer.signum(ValueComparison.compareValues(literal, constant));
        }
        throw new ParquetSchemaException("Value of type " + typeName(literal) + " is not comparable with column "
                + col.dot() + ", a constant of type " + typeName(constant));
    }

    private static String typeName(Value value) {
        return value.getClass().getSimpleName();
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
        // Each value is compared before answering: a value without an order against the constant is rejected wherever
        // it stands in the list.
        boolean listed = false;
        for (Value candidate : in.values()) {
            listed |= holdsWidening(ComparisonOperator.EQ, in.col(), constant, candidate);
        }
        return listed ? Predicate.ALWAYS_TRUE : Predicate.ALWAYS_FALSE;
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
