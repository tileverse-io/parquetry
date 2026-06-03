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

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;

/** Coerces jsqlparser literal nodes into the Java values parquetry predicates expect. */
final class Literals {

    private Literals() {}

    static long asLong(Expression value) {
        return switch (value) {
            case LongValue longValue -> longValue.getValue();
            case SignedExpression signed -> signedLong(signed);
            default -> throw new FilterParseException("expected an integer literal, got: " + value);
        };
    }

    static double asDouble(Expression value) {
        return switch (value) {
            case LongValue longValue -> longValue.getValue();
            case DoubleValue doubleValue -> doubleValue.getValue();
            case SignedExpression signed -> signedDouble(signed);
            default -> throw new FilterParseException("expected a numeric literal, got: " + value);
        };
    }

    static String asString(Expression value) {
        if (value instanceof StringValue stringValue) {
            return stringValue.getValue();
        }
        throw new FilterParseException("expected a quoted string literal, got: " + value);
    }

    static boolean asBool(Expression value) {
        if (value instanceof BooleanValue booleanValue) {
            return booleanValue.getValue();
        }
        if (value instanceof StringValue stringValue) {
            return parseBooleanText(stringValue.getValue(), value);
        }
        throw new FilterParseException("expected true or false, got: " + value);
    }

    private static long signedLong(SignedExpression signed) {
        long magnitude = asLong(signed.getExpression());
        return isNegative(signed) ? -magnitude : magnitude;
    }

    private static double signedDouble(SignedExpression signed) {
        double magnitude = asDouble(signed.getExpression());
        return isNegative(signed) ? -magnitude : magnitude;
    }

    private static boolean isNegative(SignedExpression signed) {
        return signed.getSign() == '-';
    }

    private static boolean parseBooleanText(String text, Expression value) {
        if ("true".equalsIgnoreCase(text)) {
            return true;
        }
        if ("false".equalsIgnoreCase(text)) {
            return false;
        }
        throw new FilterParseException("expected true or false, got: " + value);
    }
}
