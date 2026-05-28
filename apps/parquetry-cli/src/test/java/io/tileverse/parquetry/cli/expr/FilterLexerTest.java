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

import java.util.List;

import org.junit.jupiter.api.Test;

class FilterLexerTest {

    @Test
    void lexesComparisonWithStringLiteral() {
        List<Token> tokens = FilterLexer.lex("country = 'AR'");
        assertThat(tokens)
                .extracting(Token::type)
                .containsExactly(Token.Type.IDENT, Token.Type.OP, Token.Type.STRING, Token.Type.EOF);
        assertThat(tokens.get(0).text()).isEqualTo("country");
        assertThat(tokens.get(2).text()).isEqualTo("AR");
    }

    @Test
    void lexesKeywordsCaseInsensitively() {
        List<Token> tokens = FilterLexer.lex("a > 1 and B is not null");
        assertThat(tokens)
                .extracting(Token::type)
                .containsExactly(
                        Token.Type.IDENT,
                        Token.Type.OP,
                        Token.Type.NUMBER,
                        Token.Type.AND,
                        Token.Type.IDENT,
                        Token.Type.IS,
                        Token.Type.NOT,
                        Token.Type.NULL,
                        Token.Type.EOF);
    }

    @Test
    void rejectsUnterminatedString() {
        assertThatThrownBy(() -> FilterLexer.lex("a = 'oops"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("unterminated");
    }
}
