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
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/** Parentheses around an operand of a filter keep its meaning, as in SQL. */
class ParenthesizedOperandTest {

    private static final ParquetSchema CITIES = Fixtures.citiesSchema();
    private static final ParquetSchema GEO_CITIES = Fixtures.geoCitiesSchema();
    private static final Set<ColumnPath> GEOMETRY = Set.of(ColumnPath.of("geometry"));

    // A filter with an operand in parentheses parses as the same filter without them.
    @ParameterizedTest(name = "{0}")
    @MethodSource("filtersWithParenthesizedOperands")
    void readsParenthesizedOperands(String parenthesized, String plain) {
        assertThat(parse(parenthesized)).isEqualTo(parse(plain));
    }

    static Stream<Arguments> filtersWithParenthesizedOperands() {
        return Stream.of(
                Arguments.of("(pop) > 1000", "pop > 1000"),
                Arguments.of("pop > (1000)", "pop > 1000"),
                Arguments.of("((pop)) >= ((1000))", "pop >= 1000"),
                Arguments.of("(id) = (2)", "id = 2"),
                Arguments.of("pop <> (-5)", "pop <> -5"),
                Arguments.of("pop > -(5)", "pop > -5"),
                Arguments.of("name = ('Rosario')", "name = 'Rosario'"),
                Arguments.of("(name) <> ('Rosario')", "name <> 'Rosario'"),
                Arguments.of("capital = (true)", "capital = true"),
                Arguments.of("pop IN ((1500))", "pop IN (1500)"),
                Arguments.of("pop IN ((1500), 2000)", "pop IN (1500, 2000)"),
                Arguments.of("(pop) IN (1500, 2000)", "pop IN (1500, 2000)"),
                Arguments.of("(pop) NOT IN (1500)", "pop NOT IN (1500)"),
                Arguments.of("(pop) IS NULL", "pop IS NULL"),
                Arguments.of("(pop) IS NOT NULL", "pop IS NOT NULL"),
                Arguments.of("(pop) BETWEEN (1) AND (2)", "pop BETWEEN 1 AND 2"));
    }

    // A spatial function with an argument in parentheses parses as the same call without them.
    @ParameterizedTest(name = "{0}")
    @MethodSource("spatialFiltersWithParenthesizedArguments")
    void readsParenthesizedSpatialArgs(String parenthesized, String plain) {
        assertThat(parseSpatial(parenthesized)).isEqualTo(parseSpatial(plain));
    }

    static Stream<Arguments> spatialFiltersWithParenthesizedArguments() {
        String plain = "ST_Intersects(ST_Extent(geometry), ST_MakeEnvelope(0, 0, 10, 5))";
        return Stream.of(
                Arguments.of("ST_Intersects(ST_Extent((geometry)), ST_MakeEnvelope(0, 0, 10, 5))", plain),
                Arguments.of("ST_Intersects((ST_Extent(geometry)), (ST_MakeEnvelope(0, 0, 10, 5)))", plain),
                Arguments.of("ST_Intersects(ST_Extent(geometry), ST_MakeEnvelope((0), 0, (10), 5))", plain),
                Arguments.of("ST_Intersects(ST_Extent(geometry), ST_Extent((ST_MakeEnvelope(0, 0, 10, 5))))", plain));
    }

    // An exact relation with its geometry column in parentheses still filters that column.
    @Test
    void parenthesizedGeometryColumn() {
        Predicate parsed = parseSpatial("ST_Intersects((geometry), ST_GeomFromText(('POINT(1 1)')))");

        assertThat(parsed).isInstanceOf(Predicate.GeometryFilterPredicate.class);
        assertThat(Predicate.columns(parsed)).containsExactlyElementsOf(GEOMETRY);
    }

    // Parentheses around several expressions are a list, not an operand.
    @Test
    void rejectsParenthesizedList() {
        assertThatThrownBy(() -> parse("(pop, id) > 1000"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("expected a column on the left of the comparison, got: (pop, id)");
    }

    // A rejection quotes the text of the filter and names no class of the SQL parser.
    @ParameterizedTest(name = "{0}")
    @MethodSource("rejectedFilters")
    void rejectionQuotesTheFilterText(String filter, String expectedMessage) {
        assertThatThrownBy(() -> parse(filter))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining(expectedMessage)
                .hasMessageNotContaining("ParenthesedExpressionList")
                .hasMessageNotContaining("LongValue")
                .hasMessageNotContaining("Addition");
    }

    static Stream<Arguments> rejectedFilters() {
        return Stream.of(
                Arguments.of("5 > pop", "expected a column on the left of the comparison, got: 5"),
                Arguments.of("pop + 1 > 5", "expected a column on the left of the comparison, got: pop + 1"),
                Arguments.of("(pop > 1000, capital = true)", "unsupported in --filter: (pop > 1000, capital = true)"),
                Arguments.of("pop + 1", "unsupported in --filter: pop + 1"));
    }

    private static Predicate parse(String filter) {
        return FilterParser.parse(filter, CITIES, Set.of());
    }

    private static Predicate parseSpatial(String filter) {
        return FilterParser.parse(filter, GEO_CITIES, GEOMETRY);
    }
}
