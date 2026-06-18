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

import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ParquetSchema;

class SqlPredicateTranslatorTest {

    private final ParquetSchema schema = Fixtures.citiesSchema();

    private Predicate parse(String filter) {
        return FilterParser.parse(filter, schema, Set.of());
    }

    private static final ParquetSchema NUMERIC_KINDS = Fixtures.numericKindsSchema();

    private static Predicate parseNumeric(String filter) {
        return FilterParser.parse(filter, NUMERIC_KINDS, Set.of());
    }

    @Test
    void intComparison() {
        assertThat(parse("pop > 1000")).isInstanceOf(Predicate.Gt.class);
        assertThat(parse("pop >= 1000")).isInstanceOf(Predicate.GtEq.class);
        assertThat(parse("pop < 1000")).isInstanceOf(Predicate.Lt.class);
        assertThat(parse("pop <= 1000")).isInstanceOf(Predicate.LtEq.class);
        assertThat(parse("pop = 1000")).isInstanceOf(Predicate.Eq.class);
    }

    @Test
    void negativeNumberLiteral() {
        assertThat(parse("pop > -5")).isInstanceOf(Predicate.Gt.class);
    }

    @Test
    void stringEquality() {
        assertThat(parse("name = 'Rosario'")).isInstanceOf(Predicate.Eq.class);
    }

    @Test
    void notEqualsBothSpellings() {
        assertThat(parse("pop != 5")).isInstanceOf(Predicate.NotEq.class);
        assertThat(parse("pop <> 5")).isInstanceOf(Predicate.NotEq.class);
    }

    @Test
    void booleanEqualityAndInequality() {
        assertThat(parse("capital = true")).isInstanceOf(Predicate.Eq.class);
        assertThat(parse("capital != true")).isInstanceOf(Predicate.Not.class);
    }

    @Test
    void andOrPrecedence() {
        Predicate p = parse("pop > 1000 OR pop < 10 AND capital = true");
        assertThat(p).isInstanceOf(Predicate.Or.class);
        Predicate.Or or = (Predicate.Or) p;
        assertThat(or.children().get(1)).isInstanceOf(Predicate.And.class);
    }

    @Test
    void notDistributes() {
        assertThat(parse("NOT pop > 1000")).isInstanceOf(Predicate.Not.class);
    }

    @Test
    void isNullAndIsNotNull() {
        assertThat(parse("name IS NULL")).isInstanceOf(Predicate.IsNull.class);
        assertThat(parse("name IS NOT NULL")).isInstanceOf(Predicate.IsNotNull.class);
    }

    @Test
    void unknownColumnRejected() {
        assertThatThrownBy(() -> parse("nope = 1"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("no such column");
    }

    @Test
    void invalidSyntaxRejected() {
        assertThatThrownBy(() -> parse("pop >"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("invalid filter syntax");
    }

    @Test
    void typeMismatchRejected() {
        assertThatThrownBy(() -> parse("pop = 'abc'")).isInstanceOf(FilterParseException.class);
    }

    @Test
    void functionRejectedForNow() {
        assertThatThrownBy(() -> parse("UPPER(name) = 'X'"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("unsupported");
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("numericKindComparisons")
    void numericKindComparison(String filter, Class<? extends Predicate> expected) {
        assertThat(parseNumeric(filter)).isInstanceOf(expected);
    }

    private static Stream<Arguments> numericKindComparisons() {
        return Stream.of(
                Arguments.of("i32 > 5", Predicate.Gt.class),
                Arguments.of("i32 = 5", Predicate.Eq.class),
                Arguments.of("f32 < 1.5", Predicate.Lt.class),
                Arguments.of("f32 = 1.5", Predicate.Eq.class),
                Arguments.of("f64 >= 2.5", Predicate.GtEq.class),
                Arguments.of("f32 != 1.5", Predicate.NotEq.class));
    }

    @Test
    void int32EqualityNarrowsLiteralToInt() {
        Predicate.Eq eq = (Predicate.Eq) parseNumeric("i32 = 5");
        assertThat(eq.v()).isEqualTo(new Value.IntVal(5));
    }

    @Test
    void int32ComparisonNarrowsLiteralToInt() {
        Predicate.Gt gt = (Predicate.Gt) parseNumeric("i32 > 5");
        assertThat(gt.v()).isEqualTo(new Value.IntVal(5));
    }

    @Test
    void floatEqualityNarrowsLiteralToFloat() {
        Predicate.Eq eq = (Predicate.Eq) parseNumeric("f32 = 1.5");
        assertThat(eq.v()).isEqualTo(new Value.FloatVal(1.5f));
    }

    @Test
    void floatComparisonComparesAsDouble() {
        Predicate.Lt lt = (Predicate.Lt) parseNumeric("f32 < 1.5");
        assertThat(lt.v()).isEqualTo(new Value.DoubleVal(1.5d));
    }

    @Test
    void floatNotEqualsComparesAsDouble() {
        Predicate.NotEq notEq = (Predicate.NotEq) parseNumeric("f32 != 1.5");
        assertThat(notEq.v()).isEqualTo(new Value.DoubleVal(1.5d));
    }

    @Test
    void doubleComparisonKeepsLiteralAsDouble() {
        Predicate.GtEq gtEq = (Predicate.GtEq) parseNumeric("f64 >= 2.5");
        assertThat(gtEq.v()).isEqualTo(new Value.DoubleVal(2.5d));
    }

    @Test
    void inListOfInts() {
        Predicate p = parse("pop IN (1000, 2000, 3000)");
        assertThat(p).isInstanceOf(Predicate.In.class);
        assertThat(((Predicate.In) p).values()).hasSize(3);
    }

    @Test
    void inListOfStrings() {
        Predicate p = parse("name IN ('Rosario', 'Cordoba')");
        assertThat(p).isInstanceOf(Predicate.In.class);
    }

    @Test
    void notInNegatesIn() {
        Predicate p = parse("pop NOT IN (1000, 2000)");
        assertThat(p).isInstanceOf(Predicate.Not.class);
        assertThat(((Predicate.Not) p).child()).isInstanceOf(Predicate.In.class);
    }

    @Test
    void betweenLowersToBoundedAnd() {
        Predicate p = parse("pop BETWEEN 1000 AND 2000");
        assertThat(p).isInstanceOf(Predicate.And.class);
        Predicate.And and = (Predicate.And) p;
        assertThat(and.children().get(0)).isInstanceOf(Predicate.GtEq.class);
        assertThat(and.children().get(1)).isInstanceOf(Predicate.LtEq.class);
    }

    @Test
    void notBetweenNegatesBoundedAnd() {
        Predicate p = parse("pop NOT BETWEEN 1000 AND 2000");
        assertThat(p).isInstanceOf(Predicate.Not.class);
        assertThat(((Predicate.Not) p).child()).isInstanceOf(Predicate.And.class);
    }

    @Test
    void inListOnLongColumnCoercesToLongVal() {
        Predicate.In in = (Predicate.In) parse("pop IN (1, 2)");
        assertThat(in.values()).containsExactly(new Value.LongVal(1L), new Value.LongVal(2L));
    }

    @Test
    void inListOnIntColumnCoercesToIntVal() {
        Predicate.In in = (Predicate.In) parseNumeric("i32 IN (1, 2)");
        assertThat(in.values()).containsExactly(new Value.IntVal(1), new Value.IntVal(2));
    }

    private static final ParquetSchema UUID_COLUMN = Fixtures.uuidColumnSchema();

    private static Predicate parseUuid(String filter) {
        return FilterParser.parse(filter, UUID_COLUMN, Set.of());
    }

    @Test
    void uuidEqualityCoercesLiteralToUuidVal() {
        UUID uuid = UUID.fromString("12345678-1234-1234-1234-123456789abc");
        Predicate.Eq eq = (Predicate.Eq) parseUuid("id = '12345678-1234-1234-1234-123456789abc'");
        assertThat(eq.v()).isEqualTo(new Value.UuidVal(uuid));
    }

    @Test
    void uuidInListCoercesLiteralsToUuidVal() {
        UUID first = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID second = UUID.fromString("00000000-0000-0000-0000-000000000002");
        Predicate.In in = (Predicate.In)
                parseUuid("id IN ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002')");
        assertThat(in.values()).containsExactly(new Value.UuidVal(first), new Value.UuidVal(second));
    }

    @Test
    void uuidNotEqualsNegatesUuidEquality() {
        UUID uuid = UUID.fromString("12345678-1234-1234-1234-123456789abc");
        Predicate.Not not = (Predicate.Not) parseUuid("id <> '12345678-1234-1234-1234-123456789abc'");
        Predicate.Eq eq = (Predicate.Eq) not.child();
        assertThat(eq.v()).isEqualTo(new Value.UuidVal(uuid));
    }

    @Test
    void uuidRangeRejected() {
        assertThatThrownBy(() -> parseUuid("id > '12345678-1234-1234-1234-123456789abc'"))
                .isInstanceOf(FilterParseException.class);
    }
}
