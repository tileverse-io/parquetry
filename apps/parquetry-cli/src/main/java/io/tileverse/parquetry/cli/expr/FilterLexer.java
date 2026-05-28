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

import java.util.ArrayList;
import java.util.List;

/** Hand-rolled lexer for the reduced filter DSL. */
public final class FilterLexer {

    private final String src;
    private int pos;

    private FilterLexer(String src) {
        this.src = src;
    }

    public static List<Token> lex(String src) {
        return new FilterLexer(src).run();
    }

    private List<Token> run() {
        List<Token> out = new ArrayList<>();
        while (pos < src.length()) {
            char c = src.charAt(pos);
            if (Character.isWhitespace(c)) {
                pos++;
                continue;
            }
            if (c == '(') {
                out.add(new Token(Token.Type.LPAREN, "(", pos++));
            } else if (c == ')') {
                out.add(new Token(Token.Type.RPAREN, ")", pos++));
            } else if (c == '\'') {
                out.add(stringLiteral());
            } else if (isOpStart(c)) {
                out.add(operator());
            } else if (c == '-' || c == '.' || Character.isDigit(c)) {
                out.add(number());
            } else if (Character.isLetter(c) || c == '_') {
                out.add(word());
            } else {
                throw new FilterParseException("unexpected character '" + c + "' at " + pos);
            }
        }
        out.add(new Token(Token.Type.EOF, "", pos));
        return out;
    }

    private Token stringLiteral() {
        int start = pos;
        pos++;
        StringBuilder sb = new StringBuilder();
        while (pos < src.length() && src.charAt(pos) != '\'') {
            sb.append(src.charAt(pos++));
        }
        if (pos >= src.length()) {
            throw new FilterParseException("unterminated string literal at " + start);
        }
        pos++;
        return new Token(Token.Type.STRING, sb.toString(), start);
    }

    private Token operator() {
        int start = pos;
        char c = src.charAt(pos++);
        if ((c == '!' || c == '<' || c == '>') && pos < src.length() && src.charAt(pos) == '=') {
            pos++;
            return new Token(Token.Type.OP, c + "=", start);
        }
        if (c == '=' || c == '<' || c == '>') {
            return new Token(Token.Type.OP, String.valueOf(c), start);
        }
        throw new FilterParseException("invalid operator at " + start);
    }

    private Token number() {
        int start = pos;
        if (src.charAt(pos) == '-') {
            pos++;
        }
        while (pos < src.length() && (Character.isDigit(src.charAt(pos)) || src.charAt(pos) == '.')) {
            pos++;
        }
        return new Token(Token.Type.NUMBER, src.substring(start, pos), start);
    }

    private Token word() {
        int start = pos;
        while (pos < src.length() && (Character.isLetterOrDigit(src.charAt(pos)) || src.charAt(pos) == '_')) {
            pos++;
        }
        String text = src.substring(start, pos);
        return new Token(keywordType(text), text, start);
    }

    private static Token.Type keywordType(String text) {
        return switch (text.toUpperCase()) {
            case "AND" -> Token.Type.AND;
            case "OR" -> Token.Type.OR;
            case "NOT" -> Token.Type.NOT;
            case "IS" -> Token.Type.IS;
            case "NULL" -> Token.Type.NULL;
            case "TRUE", "FALSE" -> Token.Type.BOOL;
            default -> Token.Type.IDENT;
        };
    }

    private static boolean isOpStart(char c) {
        return c == '=' || c == '!' || c == '<' || c == '>';
    }
}
