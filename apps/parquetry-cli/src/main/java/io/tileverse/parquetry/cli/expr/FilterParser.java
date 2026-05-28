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

import java.util.List;

import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

/** Recursive-descent parser for the reduced filter DSL; types literals against the schema. */
public final class FilterParser {

    private final List<Token> tokens;
    private final ParquetSchema schema;
    private int index;

    private FilterParser(List<Token> tokens, ParquetSchema schema) {
        this.tokens = tokens;
        this.schema = schema;
    }

    public static Predicate parse(String expr, ParquetSchema schema) {
        FilterParser parser = new FilterParser(FilterLexer.lex(expr), schema);
        Predicate predicate = parser.orExpr();
        parser.expect(Token.Type.EOF);
        return predicate;
    }

    private Predicate orExpr() {
        Predicate left = andExpr();
        while (peek().type() == Token.Type.OR) {
            advance();
            left = Pred.or(left, andExpr());
        }
        return left;
    }

    private Predicate andExpr() {
        Predicate left = notExpr();
        while (peek().type() == Token.Type.AND) {
            advance();
            left = Pred.and(left, notExpr());
        }
        return left;
    }

    private Predicate notExpr() {
        if (peek().type() == Token.Type.NOT) {
            advance();
            return Pred.not(notExpr());
        }
        return atom();
    }

    private Predicate atom() {
        if (peek().type() == Token.Type.LPAREN) {
            advance();
            Predicate inner = orExpr();
            expect(Token.Type.RPAREN);
            return inner;
        }
        return predicate();
    }

    private Predicate predicate() {
        Token columnToken = expect(Token.Type.IDENT);
        ColumnPath column = column(columnToken);
        Pred.ColumnRef ref = Pred.col(column);
        if (peek().type() == Token.Type.IS) {
            return isNullPredicate(ref);
        }
        Token op = expect(Token.Type.OP);
        Token value = advance();
        return comparison(column, ref, op.text(), value);
    }

    private Predicate isNullPredicate(Pred.ColumnRef ref) {
        advance(); // IS
        if (peek().type() == Token.Type.NOT) {
            advance();
            expect(Token.Type.NULL);
            return ref.isNotNull();
        }
        expect(Token.Type.NULL);
        return ref.isNull();
    }

    private Predicate comparison(ColumnPath column, Pred.ColumnRef ref, String op, Token value) {
        PrimitiveKind kind = kindOf(column);
        return switch (op) {
            case "=" -> eq(ref, kind, value);
            case "!=" -> notEq(ref, kind, value);
            case "<" -> lt(ref, kind, value);
            case "<=" -> ltEq(ref, kind, value);
            case ">" -> gt(ref, kind, value);
            case ">=" -> gtEq(ref, kind, value);
            default -> throw new FilterParseException("unsupported operator '" + op + "' at " + value.pos());
        };
    }

    private Predicate eq(Pred.ColumnRef ref, PrimitiveKind kind, Token value) {
        return switch (kind) {
            case BOOLEAN -> ref.eq(asBool(value));
            case INT32 -> ref.eq((int) asLong(value));
            case INT64 -> ref.eq(asLong(value));
            case FLOAT -> ref.eq((float) asDouble(value));
            case DOUBLE -> ref.eq(asDouble(value));
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> ref.eq(asString(value));
            default -> throw unsupported(kind, value);
        };
    }

    private Predicate notEq(Pred.ColumnRef ref, PrimitiveKind kind, Token value) {
        return switch (kind) {
            case BOOLEAN -> Pred.not(ref.eq(asBool(value)));
            case INT32 -> ref.notEq((int) asLong(value));
            case INT64 -> ref.notEq(asLong(value));
            case DOUBLE, FLOAT -> ref.notEq(asDouble(value));
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> ref.notEq(asString(value));
            default -> throw unsupported(kind, value);
        };
    }

    private Predicate lt(Pred.ColumnRef ref, PrimitiveKind kind, Token value) {
        return switch (kind) {
            case INT32 -> ref.lt((int) asLong(value));
            case INT64 -> ref.lt(asLong(value));
            case DOUBLE, FLOAT -> ref.lt(asDouble(value));
            default -> throw unsupported(kind, value);
        };
    }

    private Predicate ltEq(Pred.ColumnRef ref, PrimitiveKind kind, Token value) {
        return switch (kind) {
            case INT32 -> ref.ltEq((int) asLong(value));
            case INT64 -> ref.ltEq(asLong(value));
            case DOUBLE, FLOAT -> ref.ltEq(asDouble(value));
            default -> throw unsupported(kind, value);
        };
    }

    private Predicate gt(Pred.ColumnRef ref, PrimitiveKind kind, Token value) {
        return switch (kind) {
            case INT32 -> ref.gt((int) asLong(value));
            case INT64 -> ref.gt(asLong(value));
            case DOUBLE, FLOAT -> ref.gt(asDouble(value));
            default -> throw unsupported(kind, value);
        };
    }

    private Predicate gtEq(Pred.ColumnRef ref, PrimitiveKind kind, Token value) {
        return switch (kind) {
            case INT32 -> ref.gtEq((int) asLong(value));
            case INT64 -> ref.gtEq(asLong(value));
            case DOUBLE, FLOAT -> ref.gtEq(asDouble(value));
            default -> throw unsupported(kind, value);
        };
    }

    private ColumnPath column(Token first) {
        return ColumnPath.of(first.text().split("\\."));
    }

    private PrimitiveKind kindOf(ColumnPath column) {
        SchemaNode node = schema.find(column)
                .orElseThrow(() -> new FilterParseException("unknown column '" + column.dot() + "'"));
        if (node instanceof SchemaNode.Primitive prim) {
            return prim.kind();
        }
        throw new FilterParseException("column '" + column.dot() + "' is not a leaf");
    }

    private boolean asBool(Token value) {
        if (value.type() != Token.Type.BOOL) {
            throw new FilterParseException("expected true/false at " + value.pos());
        }
        return Boolean.parseBoolean(value.text());
    }

    private long asLong(Token value) {
        requireNumber(value);
        try {
            return Long.parseLong(value.text());
        } catch (NumberFormatException e) {
            throw new FilterParseException("expected an integer at " + value.pos());
        }
    }

    private double asDouble(Token value) {
        requireNumber(value);
        try {
            return Double.parseDouble(value.text());
        } catch (NumberFormatException e) {
            throw new FilterParseException("expected a number at " + value.pos());
        }
    }

    private String asString(Token value) {
        if (value.type() != Token.Type.STRING) {
            throw new FilterParseException("expected a quoted string at " + value.pos());
        }
        return value.text();
    }

    private void requireNumber(Token value) {
        if (value.type() != Token.Type.NUMBER) {
            throw new FilterParseException("expected a number at " + value.pos());
        }
    }

    private FilterParseException unsupported(PrimitiveKind kind, Token value) {
        return new FilterParseException("operator not supported for " + kind + " at " + value.pos());
    }

    private Token peek() {
        return tokens.get(index);
    }

    private Token advance() {
        return tokens.get(index++);
    }

    private Token expect(Token.Type type) {
        Token token = peek();
        if (token.type() != type) {
            throw new FilterParseException("expected " + type + " but found '" + token.text() + "' at " + token.pos());
        }
        return advance();
    }
}
