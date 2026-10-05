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
package io.tileverse.parquetry.cli.expr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ParquetSchema;

/** Parentheses group the conditions of a filter, as in SQL. */
class ParenthesizedConditionTest {

    private static final ParquetSchema SCHEMA = Fixtures.citiesSchema();

    @ParameterizedTest(name = "{0}")
    @MethodSource("conditionsWithRedundantParentheses")
    void parenthesesAroundAConditionKeepItsMeaning(String parenthesized, String plain) {
        assertThat(parse(parenthesized)).isEqualTo(parse(plain));
    }

    static Stream<Arguments> conditionsWithRedundantParentheses() {
        return Stream.of(
                Arguments.of("(pop > 1000)", "pop > 1000"),
                Arguments.of("((pop > 1000))", "pop > 1000"),
                Arguments.of("NOT (pop > 1000)", "NOT pop > 1000"),
                Arguments.of("(name IN ('Rosario', 'Cordoba'))", "name IN ('Rosario', 'Cordoba')"),
                Arguments.of("(pop BETWEEN 1 AND 2) AND (capital = true)", "pop BETWEEN 1 AND 2 AND capital = true"),
                Arguments.of(
                        "pop > 1000 OR (pop < 10 AND capital = true)", "pop > 1000 OR pop < 10 AND capital = true"));
    }

    @Test
    void parenthesesGroupAnOrUnderAnAnd() {
        Predicate grouped = parse("(pop > 1000 OR pop < 10) AND capital = true");

        assertThat(grouped).isInstanceOf(Predicate.And.class);
        Predicate.And and = (Predicate.And) grouped;
        assertThat(and.children().get(0)).isInstanceOf(Predicate.Or.class);
        assertThat(and.children().get(1)).isEqualTo(parse("capital = true"));
    }

    @Test
    void parenthesizedListOfSeveralExpressionsIsNotACondition() {
        assertThatThrownBy(() -> parse("(pop > 1000, capital = true)"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("unsupported in --filter");
    }

    @Test
    void filterReferenceListsParenthesesWithTheLogicalOperators() {
        assertThat(FilterSyntax.reference()).contains("AND, OR, NOT, ( ) to group conditions");
    }

    private static Predicate parse(String filter) {
        return FilterParser.parse(filter, SCHEMA, Set.of());
    }
}
