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

/** One lexical token of the filter DSL. {@code pos} is the 0-based source offset for error messages. */
public record Token(Token.Type type, String text, int pos) {

    public enum Type {
        IDENT,
        NUMBER,
        STRING,
        BOOL,
        OP,
        AND,
        OR,
        NOT,
        IS,
        NULL,
        LPAREN,
        RPAREN,
        DOT, // reserved for dotted-path column names (deferred)
        EOF
    }
}
