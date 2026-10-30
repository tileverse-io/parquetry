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

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Judges a leaf of a normalized predicate from the statistics of one unit of its column. A unit is a column chunk, a
 * page, or a whole file. A leaf compares the column with literals, or tests its nulls. The STATS tier and the page tier
 * judge their units with these rules.
 *
 * <p>The rules follow the semantics of the scan. A null cell matches no comparison. An ordered comparison with NaN
 * matches no cell, and a NaN literal equals the NaN cells. The bounds of a FLOAT or DOUBLE column cover its numbers,
 * and its NaN cells lie outside them. The NaN count describes those cells.
 */
final class LeafVerdicts {

    private LeafVerdicts() {}

    /**
     * The verdict on {@code leaf} for a unit described by {@code unit}.
     *
     * @throws IllegalArgumentException when {@code leaf} is not a comparison, a set membership or a null test
     */
    static LeafVerdict of(Predicate leaf, ColumnSummary unit) {
        return switch (leaf) {
            case Predicate.Eq(ColumnPath _, Value literal) -> equalTo(literal, unit);
            case Predicate.NotEq(ColumnPath _, Value literal) -> differentFrom(literal, unit);
            case Predicate.Lt(ColumnPath _, Value literal) -> ordered(ComparisonOperator.LT, literal, unit);
            case Predicate.LtEq(ColumnPath _, Value literal) -> ordered(ComparisonOperator.LT_EQ, literal, unit);
            case Predicate.Gt(ColumnPath _, Value literal) -> ordered(ComparisonOperator.GT, literal, unit);
            case Predicate.GtEq(ColumnPath _, Value literal) -> ordered(ComparisonOperator.GT_EQ, literal, unit);
            case Predicate.In(ColumnPath _, List<Value> literals) -> in(literals, unit);
            case Predicate.IsNull _ -> isNull(unit);
            case Predicate.IsNotNull _ -> isNotNull(unit);
            case Predicate.Always _,
                    Predicate.And _,
                    Predicate.Or _,
                    Predicate.Not _,
                    Predicate.Spatial _,
                    Predicate.GeometryFilterPredicate _,
                    Predicate.RowIndexExcluded _,
                    Predicate.Quantified _ -> throw new IllegalArgumentException("not a leaf of one column: " + leaf);
        };
    }

    /**
     * The verdict on {@code leaf} for a unit without statistics. An empty list matches no row, whatever the unit holds.
     */
    static LeafVerdict withoutStatistics(Predicate leaf) {
        if (leaf instanceof Predicate.In(ColumnPath _, List<Value> literals) && literals.isEmpty()) {
            return LeafVerdict.NOTHING_LISTED;
        }
        return LeafVerdict.NO_STATISTICS;
    }

    /** A number literal can match only a cell within the bounds. A NaN literal matches the NaN cells alone. */
    private static LeafVerdict equalTo(Value literal, ColumnSummary unit) {
        if (unit.onlyNulls()) {
            return LeafVerdict.ONLY_NULLS;
        }
        if (ValueComparison.isNaN(literal)) {
            return matchedByTheNaNCells(unit);
        }
        if (unit.nans() == NaNCells.ALL) {
            return LeafVerdict.ONLY_NAN;
        }
        Optional<LeafVerdict> unjudged = boundsCannotJudge(literal, unit);
        if (unjudged.isPresent()) {
            return unjudged.orElseThrow();
        }
        int toMin = ValueComparison.compareValues(literal, unit.min().orElseThrow());
        int toMax = ValueComparison.compareValues(literal, unit.max().orElseThrow());
        if (toMin < 0 || toMax > 0) {
            return LeafVerdict.OUTSIDE_BOUNDS;
        }
        if (toMin == 0 && toMax == 0 && boundsCoverEachRow(unit)) {
            return LeafVerdict.SINGLE_VALUE_MATCHES;
        }
        return LeafVerdict.WITHIN_BOUNDS;
    }

    /**
     * A literal differs from the non-null cells holding another value. NaN differs from the numbers, and a number
     * differs from the NaN cells. Bounds equal to a number at both ends leave no other number in the unit. Only NaN
     * cells can then differ from that number.
     */
    private static LeafVerdict differentFrom(Value literal, ColumnSummary unit) {
        if (unit.onlyNulls()) {
            return LeafVerdict.ONLY_NULLS;
        }
        boolean nanLiteral = ValueComparison.isNaN(literal);
        if (unit.nans() == NaNCells.ALL) {
            return nanLiteral ? LeafVerdict.ONLY_NAN : onlyNaNCellsMatch(unit);
        }
        if (nanLiteral) {
            return differentFromNaN(unit);
        }
        Optional<LeafVerdict> unjudged = boundsCannotJudge(literal, unit);
        if (unjudged.isPresent()) {
            return unjudged.orElseThrow();
        }
        int toMin = ValueComparison.compareValues(literal, unit.min().orElseThrow());
        int toMax = ValueComparison.compareValues(literal, unit.max().orElseThrow());
        if (toMin < 0 || toMax > 0) {
            return boundsCoverEachRow(unit) ? LeafVerdict.BOUNDS_MATCH : LeafVerdict.BOUNDS_MATCH_PARTLY;
        }
        if (toMin == 0 && toMax == 0 && !mayHoldNaN(unit)) {
            return LeafVerdict.EACH_CELL_EQUAL;
        }
        return LeafVerdict.WITHIN_BOUNDS;
    }

    /** NaN differs from each number. A unit counting no NaN cell holds numbers alone in its non-null cells. */
    private static LeafVerdict differentFromNaN(ColumnSummary unit) {
        if (unit.nans() == NaNCells.ABSENT) {
            return unit.noNulls() ? LeafVerdict.NO_NAN_NOR_NULL : LeafVerdict.NO_NAN_BESIDE_NULLS;
        }
        return unit.bounded() ? LeafVerdict.NAN_OUTSIDE_BOUNDS : LeafVerdict.NO_BOUNDS;
    }

    /**
     * An ordered comparison matches no NaN cell, and a NaN literal orders no cell of a FLOAT or DOUBLE column. The
     * minimum decides whether a number can match {@code Lt} or {@code LtEq}, and the maximum whether each number does.
     * {@code Gt} and {@code GtEq} use the two bounds the other way round.
     */
    private static LeafVerdict ordered(ComparisonOperator operator, Value literal, ColumnSummary unit) {
        if (ValueComparison.isNaN(literal) && NaNCells.comparesAsNumbers(unit.kind())) {
            return LeafVerdict.NAN_ORDERS_NOTHING;
        }
        if (unit.onlyNulls()) {
            return LeafVerdict.ONLY_NULLS;
        }
        if (unit.nans() == NaNCells.ALL) {
            return LeafVerdict.ONLY_NAN;
        }
        Optional<LeafVerdict> unjudged = boundsCannotJudge(literal, unit);
        if (unjudged.isPresent()) {
            return unjudged.orElseThrow();
        }
        boolean below = operator == ComparisonOperator.LT || operator == ComparisonOperator.LT_EQ;
        Value likeliest = below ? unit.min().orElseThrow() : unit.max().orElseThrow();
        Value unlikeliest = below ? unit.max().orElseThrow() : unit.min().orElseThrow();
        if (!holdsAt(operator, likeliest, literal)) {
            return LeafVerdict.OUTSIDE_BOUNDS;
        }
        if (holdsAt(operator, unlikeliest, literal)) {
            return boundsCoverEachRow(unit) ? LeafVerdict.BOUNDS_MATCH : LeafVerdict.BOUNDS_MATCH_PARTLY;
        }
        return LeafVerdict.WITHIN_BOUNDS;
    }

    /** Whether {@code operator} holds between a cell equal to {@code bound} and {@code literal}. */
    private static boolean holdsAt(ComparisonOperator operator, Value bound, Value literal) {
        int literalToBound = ValueComparison.compareValues(literal, bound);
        return operator.holds(-Integer.signum(literalToBound));
    }

    /**
     * A list matches the cells equal to one of its literals. A NaN literal keeps a unit with possible NaN cells,
     * whatever its bounds.
     */
    private static LeafVerdict in(List<Value> literals, ColumnSummary unit) {
        if (literals.isEmpty()) {
            return LeafVerdict.NOTHING_LISTED;
        }
        if (unit.onlyNulls()) {
            return LeafVerdict.ONLY_NULLS;
        }
        boolean listsNaN = literals.stream().anyMatch(ValueComparison::isNaN);
        if (listsNaN && unit.nans() != NaNCells.ABSENT) {
            return matchedByTheNaNCells(unit);
        }
        return matchedByTheNumbers(literals, unit);
    }

    /**
     * Judges the number literals of a list against the bounds. A list naming NaN reaches this rule only when the unit
     * counts no NaN cell. Its NaN literal then matches nothing.
     */
    private static LeafVerdict matchedByTheNumbers(List<Value> literals, ColumnSummary unit) {
        boolean listsNumber = literals.stream().anyMatch(literal -> !ValueComparison.isNaN(literal));
        if (!listsNumber) {
            return LeafVerdict.NO_NAN;
        }
        if (unit.nans() == NaNCells.ALL) {
            return LeafVerdict.ONLY_NAN;
        }
        if (!unit.bounded()) {
            return LeafVerdict.NO_BOUNDS;
        }
        Value min = unit.min().orElseThrow();
        Value max = unit.max().orElseThrow();
        for (Value literal : literals) {
            if (ValueComparison.isNaN(literal)) {
                continue;
            }
            if (!ValueComparison.ordered(literal, min)) {
                return LeafVerdict.LITERAL_OF_ANOTHER_TYPE;
            }
            boolean withinBounds = ValueComparison.compareValues(literal, min) >= 0
                    && ValueComparison.compareValues(literal, max) <= 0;
            if (withinBounds) {
                return LeafVerdict.WITHIN_BOUNDS;
            }
        }
        return LeafVerdict.OUTSIDE_BOUNDS;
    }

    private static LeafVerdict isNull(ColumnSummary unit) {
        OptionalLong nullCount = unit.nullCount();
        if (nullCount.isEmpty()) {
            return LeafVerdict.NO_NULL_COUNT;
        }
        if (nullCount.getAsLong() == 0L) {
            return LeafVerdict.NO_NULLS;
        }
        if (unit.onlyNulls()) {
            return LeafVerdict.ONLY_NULLS_MATCH;
        }
        return LeafVerdict.SOME_NULLS;
    }

    private static LeafVerdict isNotNull(ColumnSummary unit) {
        OptionalLong nullCount = unit.nullCount();
        if (nullCount.isEmpty()) {
            return LeafVerdict.NO_NULL_COUNT;
        }
        if (unit.onlyNulls()) {
            return LeafVerdict.ONLY_NULLS;
        }
        if (nullCount.getAsLong() == 0L) {
            return LeafVerdict.NO_NULLS_MATCH;
        }
        return LeafVerdict.SOME_NULLS;
    }

    /** A NaN literal matches the NaN cells alone. The NaN count of the unit describes them, and its bounds do not. */
    private static LeafVerdict matchedByTheNaNCells(ColumnSummary unit) {
        return switch (unit.nans()) {
            case ABSENT -> LeafVerdict.NO_NAN;
            case ALL -> onlyNaNCellsMatch(unit);
            case POSSIBLE -> unit.bounded() ? LeafVerdict.NAN_OUTSIDE_BOUNDS : LeafVerdict.NO_BOUNDS;
        };
    }

    /**
     * Judges a unit whose non-null cells are NaN, for a literal matching the NaN cells. Each row matches when the unit
     * counts no null.
     */
    private static LeafVerdict onlyNaNCellsMatch(ColumnSummary unit) {
        return unit.noNulls() ? LeafVerdict.EACH_CELL_NAN : LeafVerdict.NAN_BESIDE_NULLS;
    }

    /**
     * Why the bounds of {@code unit} cannot judge a comparison with {@code literal}, empty when they can. The unit may
     * have no usable bounds. A literal of the physical type of a column has no order against bounds decoded to its
     * logical type, such as an integer against a timestamp. The scan then compares the literal with the stored cells.
     */
    private static Optional<LeafVerdict> boundsCannotJudge(Value literal, ColumnSummary unit) {
        if (!unit.bounded()) {
            return Optional.of(LeafVerdict.NO_BOUNDS);
        }
        if (!ValueComparison.ordered(literal, unit.min().orElseThrow())) {
            return Optional.of(LeafVerdict.LITERAL_OF_ANOTHER_TYPE);
        }
        return Optional.empty();
    }

    /**
     * Whether the bounds of {@code unit} cover each of its rows. A comparison holding over the whole range then matches
     * each row. Null cells lie outside the bounds, and the NaN cells of a FLOAT or DOUBLE column too. The unit must
     * count no null, and no NaN cell for those kinds. Binary bounds may be truncated, and INT96 has no statistics path.
     */
    private static boolean boundsCoverEachRow(ColumnSummary unit) {
        if (!unit.noNulls()) {
            return false;
        }
        return switch (unit.kind()) {
            case BOOLEAN, INT32, INT64 -> true;
            case FLOAT, DOUBLE -> unit.nans() == NaNCells.ABSENT;
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> false;
        };
    }

    /** Whether a FLOAT or DOUBLE unit may hold NaN cells. NaN cells lie outside the bounds. */
    private static boolean mayHoldNaN(ColumnSummary unit) {
        return NaNCells.comparesAsNumbers(unit.kind()) && unit.nans() != NaNCells.ABSENT;
    }
}
