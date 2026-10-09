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

import java.util.ArrayList;
import java.util.List;

import io.tileverse.parquetry.schema.ColumnPath;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * The column paths named by the column references of a filter. A reference is a sequence of identifiers separated by
 * dots; a quoted identifier, as required by a SQL keyword or a name with a space, names its segment without the quotes.
 * A doubled quote inside a quoted identifier stands for one quote of the name.
 */
final class ColumnReferences {

    private ColumnReferences() {}

    /**
     * The path of {@code column}: the identifiers of its qualifier, outermost first, then its own.
     *
     * @throws FilterParseException when an identifier of the reference is blank
     */
    static ColumnPath pathOf(Column column) {
        List<String> segments = new ArrayList<>(qualifierSegments(column));
        segments.add(nameOf(column.getColumnName()));
        requireNames(segments, column);
        return ColumnPath.of(segments);
    }

    private static void requireNames(List<String> segments, Column column) {
        for (String segment : segments) {
            if (segment == null || segment.isBlank()) {
                throw new FilterParseException("blank name in column reference: " + column.getFullyQualifiedName());
            }
        }
    }

    private static List<String> qualifierSegments(Column column) {
        Table qualifier = column.getTable();
        if (qualifier == null) {
            return List.of();
        }
        // The parser keeps the identifiers of a qualifier from the innermost outwards.
        List<String> outermostFirst = qualifier.getNameParts().reversed();
        return outermostFirst.stream().map(ColumnReferences::nameOf).toList();
    }

    /**
     * The name written by {@code identifier}: a quoted identifier without its quotes, a doubled quote inside it
     * standing for one.
     */
    private static String nameOf(String identifier) {
        if (identifier == null || !isQuoted(identifier)) {
            return identifier;
        }
        String quote = identifier.substring(0, 1);
        String quoted = identifier.substring(1, identifier.length() - 1);
        return quoted.replace(quote + quote, quote);
    }

    /** Whether {@code identifier} sits between two double quotes or two backticks, the quotes read by the parser. */
    private static boolean isQuoted(String identifier) {
        if (identifier.length() < 2) {
            return false;
        }
        char opening = identifier.charAt(0);
        char closing = identifier.charAt(identifier.length() - 1);
        boolean quote = opening == '"' || opening == '`';
        return quote && closing == opening;
    }
}
