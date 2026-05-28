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
package io.tileverse.parquetry.cli.expr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ParquetSchema;

class FilterParserTest {

    private final ParquetSchema schema = Fixtures.citiesSchema();

    @Test
    void parsesIntComparisonToTypedPredicate() {
        Predicate p = FilterParser.parse("pop > 1000", schema);
        assertThat(p).isInstanceOf(Predicate.Gt.class);
    }

    @Test
    void parsesStringEquality() {
        Predicate p = FilterParser.parse("name = 'Rosario'", schema);
        assertThat(p).isInstanceOf(Predicate.Eq.class);
    }

    @Test
    void parsesAndOrPrecedence() {
        Predicate p = FilterParser.parse("pop > 1000 OR pop < 10 AND capital = true", schema);
        assertThat(p).isInstanceOf(Predicate.Or.class);
        Predicate.Or or = (Predicate.Or) p;
        assertThat(or.children().get(1)).isInstanceOf(Predicate.And.class);
    }

    @Test
    void parsesBooleanNotEquals() {
        Predicate p = FilterParser.parse("capital != true", schema);
        assertThat(p).isInstanceOf(Predicate.Not.class);
    }

    @Test
    void parsesIsNotNull() {
        Predicate p = FilterParser.parse("name IS NOT NULL", schema);
        assertThat(p).isInstanceOf(Predicate.IsNotNull.class);
    }

    @Test
    void rejectsUnknownColumn() {
        assertThatThrownBy(() -> FilterParser.parse("nope = 1", schema))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("unknown column");
    }

    @Test
    void rejectsTrailingTokens() {
        assertThatThrownBy(() -> FilterParser.parse("pop > 1 2", schema)).isInstanceOf(FilterParseException.class);
    }
}
