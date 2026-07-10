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

import java.util.Set;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

/** Parses a SQL {@code --filter} condition and types its literals against the schema. */
public final class FilterParser {

    private FilterParser() {}

    public static Predicate parse(String filterText, ParquetSchema schema, Set<ColumnPath> geometryColumns) {
        Expression expression = parseCondition(filterText);
        return new SqlPredicateTranslator(schema, geometryColumns).translate(expression);
    }

    private static Expression parseCondition(String filterText) {
        try {
            boolean allowPartialParse = false;
            return CCJSqlParserUtil.parseCondExpression(filterText, allowPartialParse);
        } catch (JSQLParserException e) {
            throw new FilterParseException("invalid filter syntax: " + e.getMessage());
        }
    }
}
