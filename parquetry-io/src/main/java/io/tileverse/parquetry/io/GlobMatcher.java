/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.io;

import java.util.regex.Pattern;

/**
 * Matches canonical {@code /}-separated paths against a DuckDB-style glob. Unlike the JDK's
 * {@link java.nio.file.FileSystem#getPathMatcher glob matcher}, {@code **}{@code /} matches zero or more directories,
 * so {@code **}{@code /*.parquet} matches a top-level file and a nested one alike.
 *
 * <ul>
 *   <li>{@code *} matches within a single path segment (not {@code /})
 *   <li>{@code ?} matches one character that is not {@code /}
 *   <li>{@code **} matches across segments; {@code **}{@code /} matches zero or more directories
 *   <li>{@code [abc]}/{@code [a-z]} matches a single character in the set or range; a leading {@code !}
 *       ({@code [!abc]}) negates it and never matches {@code /}. As in DuckDB, {@code ^} is an ordinary member, not a
 *       negation marker.
 *   <li>{@code &#123;a,b&#125;} is alternation, a tileverse/parquetry extension beyond DuckDB glob
 * </ul>
 *
 * <p>As in DuckDB, a metacharacter that cannot form a valid construct is a literal: an unclosed {@code [} or a
 * {@code &#123;} that opens no balanced group matches that character literally.
 *
 * <p>The translation is the cross-repo contract shared with tileverse-storage's {@code StoragePattern}; both are pinned
 * to the same DuckDB golden table ({@code glob-cases.tsv}).
 */
public final class GlobMatcher {

    private static final String REGEX_METACHARACTERS = "\\.[]{}()+-^$|";

    private final Pattern pattern;

    private GlobMatcher(Pattern pattern) {
        this.pattern = pattern;
    }

    /** Compiles {@code glob} into a matcher over canonical {@code /}-separated paths. */
    public static GlobMatcher compile(String glob) {
        return new GlobMatcher(Pattern.compile(globToRegex(glob)));
    }

    /** Whether {@code path} (a canonical {@code /}-separated key) matches this glob. */
    public boolean matches(String path) {
        return path != null && pattern.matcher(path).matches();
    }

    static String globToRegex(String glob) {
        StringBuilder regex = new StringBuilder("^");
        appendGlob(glob, 0, glob.length(), regex);
        return regex.append('$').toString();
    }

    private static void appendGlob(String glob, int from, int to, StringBuilder regex) {
        int i = from;
        while (i < to) {
            char c = glob.charAt(i);
            switch (c) {
                case '*' -> i = appendStar(glob, i, to, regex);
                case '[' -> i = appendCharClass(glob, i, to, regex);
                case '{' -> i = appendBrace(glob, i, regex);
                case '?' -> {
                    regex.append("[^/]");
                    i++;
                }
                default -> {
                    appendLiteral(c, regex);
                    i++;
                }
            }
        }
    }

    private static int appendStar(String glob, int start, int to, StringBuilder regex) {
        boolean crossesDirectories = start + 1 < to && glob.charAt(start + 1) == '*';
        if (!crossesDirectories) {
            regex.append("[^/]*");
            return start + 1;
        }
        boolean matchesLeadingDirectories = start + 2 < to && glob.charAt(start + 2) == '/';
        if (matchesLeadingDirectories) {
            regex.append("(?:.*/)?");
            return start + 3;
        }
        regex.append(".*");
        return start + 2;
    }

    /**
     * Translates a {@code [...]} character class. DuckDB spells negation with a leading {@code !} only ({@code ^} is an
     * ordinary member), and a negated class never matches the path separator.
     */
    private static int appendCharClass(String glob, int start, int to, StringBuilder regex) {
        int contentStart = start + 1;
        boolean negated = contentStart < to && glob.charAt(contentStart) == '!';
        int firstMember = negated ? contentStart + 1 : contentStart;
        int scanFrom = firstMember < to && glob.charAt(firstMember) == ']' ? firstMember + 1 : firstMember;
        int close = glob.indexOf(']', scanFrom);
        if (close < 0) {
            appendLiteral('[', regex);
            return start + 1;
        }
        regex.append('[');
        if (negated) {
            regex.append('^');
        }
        for (int i = firstMember; i < close; i++) {
            appendClassChar(glob.charAt(i), regex);
        }
        if (negated) {
            regex.append('/');
        }
        regex.append(']');
        return close + 1;
    }

    private static void appendClassChar(char c, StringBuilder regex) {
        if (c == '\\' || c == ']' || c == '[' || c == '^') {
            regex.append('\\');
        }
        regex.append(c);
    }

    private static int appendBrace(String glob, int start, StringBuilder regex) {
        int close = glob.indexOf('}', start);
        if (close < 0) {
            appendLiteral('{', regex);
            return start + 1;
        }
        String[] alternatives = glob.substring(start + 1, close).split(",");
        regex.append("(?:");
        for (int k = 0; k < alternatives.length; k++) {
            if (k > 0) {
                regex.append('|');
            }
            String alternative = alternatives[k];
            appendGlob(alternative, 0, alternative.length(), regex);
        }
        regex.append(')');
        return close + 1;
    }

    private static void appendLiteral(char c, StringBuilder regex) {
        if (REGEX_METACHARACTERS.indexOf(c) >= 0) {
            regex.append('\\');
        }
        regex.append(c);
    }
}
