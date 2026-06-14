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
package io.tileverse.parquetry.internal.filter;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Pins the null-element semantics of {@link Predicate.Quantified} at record level. A null element of a repeated leaf is
 * a present, non-matching member of the universe: it never makes ANY true, and it counts against ALL and ONE the same
 * way any non-matching element does. This mirrors GeoTools and SQL {@code = ALL} / {@code = ANY} behavior.
 */
class QuantifiedNullSemanticsTest {

    private static final ColumnPath LEAF = ColumnPath.of("tags", "list", "element");

    @Test
    void anyMatchesWhenSomeElementMatches() {
        assertThat(evaluate(MatchAction.ANY, "a", "a", "b", "c")).isTrue();
    }

    @Test
    void anyFailsWhenNoElementMatches() {
        assertThat(evaluate(MatchAction.ANY, "z", "a", "b", "c")).isFalse();
    }

    @Test
    void allFailsWhenSomeElementDiffers() {
        assertThat(evaluate(MatchAction.ALL, "a", "a", "b", "c")).isFalse();
    }

    @Test
    void allMatchesWhenEveryElementMatches() {
        assertThat(evaluate(MatchAction.ALL, "a", "a", "a")).isTrue();
    }

    @Test
    void oneMatchesWhenExactlyOneElementMatches() {
        assertThat(evaluate(MatchAction.ONE, "a", "a", "b", "c")).isTrue();
    }

    @Test
    void oneFailsWhenTwoElementsMatch() {
        assertThat(evaluate(MatchAction.ONE, "b", "a", "b", "b")).isFalse();
    }

    @Test
    void allFailsWhenANullElementIsPresent() {
        assertThat(evaluate(MatchAction.ALL, "a", "a", null))
                .as("a null list element is a present, non-matching member of the universe")
                .isFalse();
    }

    @Test
    void anyIsUnaffectedByANullElement() {
        assertThat(evaluate(MatchAction.ANY, "a", "a", null)).isTrue();
    }

    @Test
    void oneCountsOnlyNonNullMatches() {
        assertThat(evaluate(MatchAction.ONE, "a", "a", null))
                .as("a null element does not match; exactly one non-null match satisfies ONE")
                .isTrue();
    }

    @Test
    void anyFailsOnEmptyUniverse() {
        assertThat(evaluate(MatchAction.ANY, "a")).isFalse();
    }

    @Test
    void allIsVacuouslyTrueOnEmptyUniverse() {
        assertThat(evaluate(MatchAction.ALL, "a")).isTrue();
    }

    @Test
    void oneFailsOnEmptyUniverse() {
        assertThat(evaluate(MatchAction.ONE, "a")).isFalse();
    }

    private static boolean evaluate(MatchAction match, String wanted, Object... universe) {
        Predicate leaf = new Predicate.Eq(LEAF, new Value.StringVal(wanted));
        Predicate quantified = new Predicate.Quantified(match, leaf);
        return RecordLevelEvaluator.test(quantified, universeRow(universe));
    }

    /**
     * A {@link RecordLevelEvaluator.RecordAccessor} whose repeated leaf holds {@code universe} (which may include null
     * elements). {@code multiValue} returns the null-free flattening per the public contract; {@code multiValueSize}
     * reports the full universe size including nulls.
     */
    private static RecordLevelEvaluator.RecordAccessor universeRow(Object... universe) {
        List<Object> all = Arrays.asList(universe);
        List<Object> nonNull = new ArrayList<>();
        for (Object element : all) {
            if (element != null) {
                nonNull.add(element);
            }
        }
        return new RecordLevelEvaluator.RecordAccessor() {
            @Override
            public Object value(ColumnPath path) {
                return null;
            }

            @Override
            public List<Object> multiValue(ColumnPath leafPath) {
                return List.copyOf(nonNull);
            }

            @Override
            public int multiValueSize(ColumnPath leafPath) {
                return all.size();
            }
        };
    }
}
