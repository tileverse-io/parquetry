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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ColumnPath;

class ConstantFoldingTest {

    private static final ColumnPath YEAR = ColumnPath.of("year");
    private static final ColumnPath POP = ColumnPath.of("pop");
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
}
