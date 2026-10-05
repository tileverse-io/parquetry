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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.internal.filter.PredicateNormalizer;
import io.tileverse.parquetry.internal.filter.RecordLevelEvaluator;
import io.tileverse.parquetry.schema.ColumnPath;

class ConstantFoldingTest {

    private static final ColumnPath YEAR = ColumnPath.of("year");
    private static final ColumnPath POP = ColumnPath.of("pop");
    private static final ColumnPath VALUE = ColumnPath.of("value");
    private static final Map<ColumnPath, Value> YEAR_2024 = Map.of(YEAR, new Value.IntVal(2024));

    @Test
    void matchingEqualityFoldsToTrueAndDropsFromAnd() {
        Predicate input = new Predicate.And(
                List.of(new Predicate.Eq(YEAR, new Value.IntVal(2024)), new Predicate.Gt(POP, new Value.IntVal(100))));
        assertThat(ConstantFolding.fold(input, YEAR_2024, Set.of()))
                .isEqualTo(new Predicate.Gt(POP, new Value.IntVal(100)));
    }

    @Test
    void nonMatchingEqualityFoldsToFalseAndCollapsesAndToFalse() {
        Predicate input = new Predicate.And(
                List.of(new Predicate.Eq(YEAR, new Value.IntVal(2025)), new Predicate.Gt(POP, new Value.IntVal(100))));
        assertThat(ConstantFolding.fold(input, YEAR_2024, Set.of())).isEqualTo(Predicate.ALWAYS_FALSE);
    }

    @Test
    void orWithFalseSyntheticLeafKeepsPhysicalLeaf() {
        Predicate input = new Predicate.Or(
                List.of(new Predicate.Eq(YEAR, new Value.IntVal(2025)), new Predicate.Gt(POP, new Value.IntVal(100))));
        assertThat(ConstantFolding.fold(input, YEAR_2024, Set.of()))
                .isEqualTo(new Predicate.Gt(POP, new Value.IntVal(100)));
    }

    @Test
    void physicalOnlyPredicateIsUnchanged() {
        Predicate input = new Predicate.Gt(POP, new Value.IntVal(100));
        assertThat(ConstantFolding.fold(input, YEAR_2024, Set.of())).isEqualTo(input);
    }

    @Test
    void rangeOnConstantEvaluates() {
        Predicate input = new Predicate.GtEq(YEAR, new Value.IntVal(2020));
        assertThat(ConstantFolding.fold(input, YEAR_2024, Set.of())).isEqualTo(Predicate.ALWAYS_TRUE);
    }

    @Test
    void longConstantFoldsAgainstIntLiterals() {
        Map<ColumnPath, Value> longYear = Map.of(YEAR, new Value.LongVal(2024L));

        assertThat(ConstantFolding.fold(new Predicate.Eq(YEAR, new Value.IntVal(2024)), longYear, Set.of()))
                .isEqualTo(Predicate.ALWAYS_TRUE);
        assertThat(ConstantFolding.fold(new Predicate.Eq(YEAR, new Value.IntVal(2025)), longYear, Set.of()))
                .isEqualTo(Predicate.ALWAYS_FALSE);
        assertThat(ConstantFolding.fold(new Predicate.Lt(YEAR, new Value.IntVal(2025)), longYear, Set.of()))
                .isEqualTo(Predicate.ALWAYS_TRUE);
        assertThat(ConstantFolding.fold(new Predicate.Gt(YEAR, new Value.IntVal(2025)), longYear, Set.of()))
                .isEqualTo(Predicate.ALWAYS_FALSE);
        assertThat(ConstantFolding.fold(new Predicate.GtEq(YEAR, new Value.IntVal(2024)), longYear, Set.of()))
                .isEqualTo(Predicate.ALWAYS_TRUE);
    }

    @Test
    void longConstantFoldsAgainstIntInList() {
        Map<ColumnPath, Value> longYear = Map.of(YEAR, new Value.LongVal(2024L));

        assertThat(ConstantFolding.fold(
                        new Predicate.In(YEAR, List.of(new Value.IntVal(2023), new Value.IntVal(2024))),
                        longYear,
                        Set.of()))
                .isEqualTo(Predicate.ALWAYS_TRUE);
        assertThat(ConstantFolding.fold(
                        new Predicate.In(YEAR, List.of(new Value.IntVal(2023), new Value.IntVal(2025))),
                        longYear,
                        Set.of()))
                .isEqualTo(Predicate.ALWAYS_FALSE);
    }

    @Test
    void doubleConstantFoldsAgainstIntLiteral() {
        Map<ColumnPath, Value> doublePop = Map.of(POP, new Value.DoubleVal(100.0));

        assertThat(ConstantFolding.fold(new Predicate.Eq(POP, new Value.IntVal(100)), doublePop, Set.of()))
                .isEqualTo(Predicate.ALWAYS_TRUE);
        assertThat(ConstantFolding.fold(new Predicate.Gt(POP, new Value.IntVal(100)), doublePop, Set.of()))
                .isEqualTo(Predicate.ALWAYS_FALSE);
    }

    @Test
    void nullPartitionFoldsIsNullTrueAndEqualityFalse() {
        Set<ColumnPath> nulls = Set.of(YEAR);
        assertThat(ConstantFolding.fold(new Predicate.IsNull(YEAR), Map.of(), nulls))
                .isEqualTo(Predicate.ALWAYS_TRUE);
        assertThat(ConstantFolding.fold(new Predicate.Eq(YEAR, new Value.IntVal(2024)), Map.of(), nulls))
                .isEqualTo(Predicate.ALWAYS_FALSE);
    }

    @Test
    void negatedOrderedComparisonFoldsAsItsFlippedOperator() {
        Predicate notBefore2025 = new Predicate.Lt(YEAR, new Value.IntVal(2025)).negate();
        Predicate notAfter2025 = new Predicate.Gt(YEAR, new Value.IntVal(2025)).negate();

        assertThat(ConstantFolding.fold(notBefore2025, YEAR_2024, Set.of())).isEqualTo(Predicate.ALWAYS_FALSE);
        assertThat(ConstantFolding.fold(notAfter2025, YEAR_2024, Set.of())).isEqualTo(Predicate.ALWAYS_TRUE);
    }

    @Test
    void nanPartitionValueSatisfiesNoNegatedOrderedComparison() {
        Map<ColumnPath, Value> nanValue = Map.of(VALUE, new Value.DoubleVal(Double.NaN));
        Predicate notBelowTwo = new Predicate.Lt(VALUE, new Value.DoubleVal(2.0)).negate();

        assertThat(ConstantFolding.fold(notBelowTwo, nanValue, Set.of())).isEqualTo(Predicate.ALWAYS_FALSE);
    }

    @Test
    void addedNullColumnSatisfiesNoNegatedOrderedComparison() {
        Predicate notBelowThree = new Predicate.Lt(VALUE, new Value.IntVal(3)).negate();

        assertThat(ConstantFolding.fold(notBelowThree, Map.of(), Set.of(VALUE))).isEqualTo(Predicate.ALWAYS_FALSE);
    }

    @Test
    void addedNullColumnSatisfiesNoNegatedIn() {
        Predicate in = new Predicate.In(VALUE, List.of(new Value.IntVal(3), new Value.IntVal(4)));

        assertThat(ConstantFolding.fold(in.negate(), Map.of(), Set.of(VALUE))).isEqualTo(Predicate.ALWAYS_FALSE);
    }

    @Test
    void longZeroConstantEqualsANegativeZeroLiteral() {
        Map<ColumnPath, Value> zero = Map.of(VALUE, new Value.LongVal(0L));

        assertThat(ConstantFolding.fold(new Predicate.Eq(VALUE, new Value.DoubleVal(-0.0)), zero, Set.of()))
                .isEqualTo(Predicate.ALWAYS_TRUE);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("predicatesOverValue")
    void nullColumnFoldsAsThePredicateFiltersANullCell(Predicate predicate) {
        Predicate folded = ConstantFolding.fold(predicate, Map.of(), Set.of(VALUE));

        assertThat(folded).isEqualTo(verdictOverACellHolding(null, predicate));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("predicatesOverValue")
    void nanConstantFoldsAsThePredicateFiltersANaNCell(Predicate predicate) {
        Map<ColumnPath, Value> nanValue = Map.of(VALUE, new Value.DoubleVal(Double.NaN));

        Predicate folded = ConstantFolding.fold(predicate, nanValue, Set.of());

        assertThat(folded).isEqualTo(verdictOverACellHolding(Double.NaN, predicate));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("predicatesOverValue")
    void negativeZeroConstantFoldsAsThePredicateFiltersANegativeZeroCell(Predicate predicate) {
        Map<ColumnPath, Value> negativeZero = Map.of(VALUE, new Value.DoubleVal(-0.0));

        Predicate folded = ConstantFolding.fold(predicate, negativeZero, Set.of());

        assertThat(folded).isEqualTo(verdictOverACellHolding(-0.0, predicate));
    }

    /**
     * Comparisons of {@code value} against a number, NaN and both zeros, an IN list, and the null checks, each also
     * negated.
     */
    static Stream<Predicate> predicatesOverValue() {
        List<Predicate> leaves = new ArrayList<>();
        for (double literal : new double[] {2.0, Double.NaN, 0.0, -0.0}) {
            Value value = new Value.DoubleVal(literal);
            leaves.add(new Predicate.Eq(VALUE, value));
            leaves.add(new Predicate.NotEq(VALUE, value));
            leaves.add(new Predicate.Lt(VALUE, value));
            leaves.add(new Predicate.LtEq(VALUE, value));
            leaves.add(new Predicate.Gt(VALUE, value));
            leaves.add(new Predicate.GtEq(VALUE, value));
            leaves.add(new Predicate.In(VALUE, List.of(value, new Value.DoubleVal(7.0))));
        }
        leaves.add(new Predicate.IsNull(VALUE));
        leaves.add(new Predicate.IsNotNull(VALUE));
        List<Predicate> predicates = new ArrayList<>(leaves.size() * 2);
        for (Predicate leaf : leaves) {
            predicates.add(leaf);
            predicates.add(leaf.negate());
        }
        return predicates.stream();
    }

    /**
     * {@link Predicate#ALWAYS_TRUE} when the read path keeps a row with {@code cell} in {@code value}, else
     * {@link Predicate#ALWAYS_FALSE}: the record level evaluates the normalized predicate.
     */
    private static Predicate verdictOverACellHolding(Object cell, Predicate predicate) {
        Predicate normalized = PredicateNormalizer.normalize(predicate);
        boolean kept = RecordLevelEvaluator.test(normalized, rowWithValue(cell));
        return kept ? Predicate.ALWAYS_TRUE : Predicate.ALWAYS_FALSE;
    }

    private static RecordLevelEvaluator.RecordAccessor rowWithValue(Object cell) {
        return new RecordLevelEvaluator.RecordAccessor() {
            @Override
            public Object value(ColumnPath path) {
                return path.equals(VALUE) ? cell : null;
            }

            @Override
            public List<Object> multiValue(ColumnPath leafPath) {
                return List.of();
            }
        };
    }
}
