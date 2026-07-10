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

import org.junit.jupiter.api.Test;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;

/**
 * Pins the jsqlparser 5.3 AST node classes and accessors the filter translator relies on. A failure here means the
 * parser version exposes a different shape than the translator expects, and the translator must be reconciled with the
 * actual API.
 */
class JsqlParserApiProbeTest {

    private static Expression parse(String text) throws Exception {
        return CCJSqlParserUtil.parseCondExpression(text);
    }

    @Test
    void comparisonShapes() throws Exception {
        assertThat(parse("a = 1")).isInstanceOf(EqualsTo.class);
        assertThat(parse("a != 1")).isInstanceOf(NotEqualsTo.class);
        assertThat(parse("a <> 1")).isInstanceOf(NotEqualsTo.class);
        assertThat(parse("a > 1")).isInstanceOf(GreaterThan.class);
        EqualsTo eq = (EqualsTo) parse("a = 1");
        assertThat(eq.getLeftExpression()).isInstanceOf(Column.class);
        assertThat(eq.getRightExpression()).isInstanceOf(LongValue.class);
        assertThat(((Column) eq.getLeftExpression()).getColumnName()).isEqualTo("a");
        assertThat(((LongValue) eq.getRightExpression()).getValue()).isEqualTo(1L);
    }

    @Test
    void booleanAndConjunctionShapes() throws Exception {
        assertThat(parse("a = 1 AND b = 2")).isInstanceOf(AndExpression.class);
        assertThat(parse("a = 1 OR b = 2")).isInstanceOf(OrExpression.class);
        assertThat(parse("NOT a = 1")).isInstanceOf(NotExpression.class);
        AndExpression and = (AndExpression) parse("a = 1 AND b = 2");
        assertThat(and.getLeftExpression()).isInstanceOf(EqualsTo.class);
        assertThat(and.getRightExpression()).isInstanceOf(EqualsTo.class);
    }

    @Test
    void precedencePutsAndUnderOr() throws Exception {
        OrExpression or = (OrExpression) parse("a = 1 OR b = 2 AND c = 3");
        assertThat(or.getRightExpression()).isInstanceOf(AndExpression.class);
    }

    @Test
    void stringLiteralUnquoted() throws Exception {
        EqualsTo eq = (EqualsTo) parse("name = 'Rosario'");
        assertThat(eq.getRightExpression()).isInstanceOf(StringValue.class);
        assertThat(((StringValue) eq.getRightExpression()).getValue()).isEqualTo("Rosario");
    }

    @Test
    void inBetweenIsNullShapes() throws Exception {
        InExpression in = (InExpression) parse("a IN (1, 2, 3)");
        assertThat(in.getLeftExpression()).isInstanceOf(Column.class);
        assertThat(in.isNot()).isFalse();
        assertThat(((InExpression) parse("a NOT IN (1, 2)")).isNot()).isTrue();

        // The value list lives on the right side as an ExpressionList (an ArrayList of Expression).
        assertThat(in.getRightExpression()).isInstanceOf(ExpressionList.class);
        ExpressionList<?> inValues = (ExpressionList<?>) in.getRightExpression();
        assertThat(inValues).hasSize(3);
        assertThat(inValues.get(0)).isInstanceOf(LongValue.class);

        Between between = (Between) parse("a BETWEEN 1 AND 10");
        assertThat(between.getLeftExpression()).isInstanceOf(Column.class);
        assertThat(between.getBetweenExpressionStart()).isInstanceOf(LongValue.class);
        assertThat(between.getBetweenExpressionEnd()).isInstanceOf(LongValue.class);
        assertThat(between.isNot()).isFalse();

        IsNullExpression isNull = (IsNullExpression) parse("a IS NULL");
        assertThat(isNull.getLeftExpression()).isInstanceOf(Column.class);
        assertThat(isNull.isNot()).isFalse();
        assertThat(((IsNullExpression) parse("a IS NOT NULL")).isNot()).isTrue();
    }

    @Test
    void functionExposesNameAndOrderedParameters() throws Exception {
        Function fn = (Function) parse("ST_Intersects(geom, ST_GeomFromText('POINT(0 0)'))");
        assertThat(fn.getName()).isEqualToIgnoringCase("ST_Intersects");
        assertThat(fn.getParameters()).hasSize(2);
        assertThat(fn.getParameters().get(0)).isInstanceOf(Column.class);
        assertThat(fn.getParameters().get(1)).isInstanceOf(Function.class);
    }

    @Test
    void negativeNumberAndBooleanLiteralRepresentation() throws Exception {
        // Document how 5.3 represents a negative number and a boolean literal; the translator coercion handles whatever
        // shapes this produces.
        Expression negative = ((GreaterThan) parse("a > -5")).getRightExpression();
        assertThat(negative).hasToString("-5");
        Expression bool = ((EqualsTo) parse("flag = true")).getRightExpression();
        assertThat(bool.toString()).isEqualToIgnoringCase("true");
    }
}
