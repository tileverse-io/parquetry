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

import java.math.BigInteger;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;

/**
 * Coerces jsqlparser literal nodes into the Java values expected by parquetry predicates. A literal in parentheses is
 * read as the literal.
 */
final class Literals {

    private Literals() {}

    /**
     * The integer written by {@code value}.
     *
     * @throws FilterParseException when it is no integer literal, or lies beyond the range of INT64
     */
    static long asLong(Expression value) {
        BigInteger integer = asInteger(value);
        boolean fitsInt64 = integer.bitLength() < Long.SIZE;
        if (!fitsInt64) {
            throw new FilterParseException("integer literal beyond the range of INT64: " + value);
        }
        return integer.longValue();
    }

    /**
     * The integer written by {@code value}, of any magnitude. The smallest INT64 is written as a sign over a magnitude
     * beyond INT64.
     */
    private static BigInteger asInteger(Expression value) {
        return switch (Parentheses.strip(value)) {
            case LongValue longValue -> longValue.getBigIntegerValue();
            case SignedExpression signed -> signedInteger(signed);
            default -> throw new FilterParseException("expected an integer literal, got: " + value);
        };
    }

    static double asDouble(Expression value) {
        return switch (Parentheses.strip(value)) {
            case LongValue longValue -> longValue.getBigIntegerValue().doubleValue();
            case DoubleValue doubleValue -> doubleValue.getValue();
            case SignedExpression signed -> signedDouble(signed);
            default -> throw new FilterParseException("expected a numeric literal, got: " + value);
        };
    }

    static String asString(Expression value) {
        if (Parentheses.strip(value) instanceof StringValue stringValue) {
            return stringValue.getValue();
        }
        throw new FilterParseException("expected a quoted string literal, got: " + value);
    }

    static boolean asBool(Expression value) {
        Expression literal = Parentheses.strip(value);
        if (literal instanceof BooleanValue booleanValue) {
            return booleanValue.getValue();
        }
        if (literal instanceof StringValue stringValue) {
            return parseBooleanText(stringValue.getValue(), value);
        }
        throw new FilterParseException("expected true or false, got: " + value);
    }

    private static BigInteger signedInteger(SignedExpression signed) {
        BigInteger magnitude = asInteger(signed.getExpression());
        return isNegative(signed) ? magnitude.negate() : magnitude;
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
