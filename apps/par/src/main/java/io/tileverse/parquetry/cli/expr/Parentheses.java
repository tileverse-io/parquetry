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

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;

/** Redundant parentheses around an operand of a filter: {@code (pop)} stands for {@code pop}, as in SQL. */
final class Parentheses {

    private Parentheses() {}

    /**
     * The expression inside the parentheses around {@code expression}, at any depth. A parenthesized list of several
     * expressions is no single operand and is returned as it is.
     */
    static Expression strip(Expression expression) {
        Expression inner = expression;
        while (inner instanceof ParenthesedExpressionList<?> parenthesized && parenthesized.size() == 1) {
            inner = parenthesized.getFirst();
        }
        return inner;
    }
}
