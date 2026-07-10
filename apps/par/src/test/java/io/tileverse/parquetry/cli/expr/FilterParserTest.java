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

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ParquetSchema;

class FilterParserTest {

    private final ParquetSchema schema = Fixtures.citiesSchema();

    private Predicate parse(String filter) {
        return FilterParser.parse(filter, schema, Set.of());
    }

    @Test
    void parsesIntComparisonToTypedPredicate() {
        assertThat(parse("pop > 1000")).isInstanceOf(Predicate.Gt.class);
    }

    @Test
    void parsesStringEquality() {
        assertThat(parse("name = 'Rosario'")).isInstanceOf(Predicate.Eq.class);
    }

    @Test
    void parsesAndOrPrecedence() {
        Predicate p = parse("pop > 1000 OR pop < 10 AND capital = true");
        assertThat(p).isInstanceOf(Predicate.Or.class);
        Predicate.Or or = (Predicate.Or) p;
        assertThat(or.children().get(1)).isInstanceOf(Predicate.And.class);
    }

    @Test
    void parsesBooleanNotEquals() {
        assertThat(parse("capital != true")).isInstanceOf(Predicate.Not.class);
    }

    @Test
    void parsesIsNotNull() {
        assertThat(parse("name IS NOT NULL")).isInstanceOf(Predicate.IsNotNull.class);
    }

    @Test
    void rejectsUnknownColumn() {
        assertThatThrownBy(() -> parse("nope = 1"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("no such column");
    }

    @Test
    void rejectsInvalidSyntax() {
        assertThatThrownBy(() -> parse("pop > 1 AND"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("invalid filter syntax");
    }
}
