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
package io.tileverse.parquetry.io;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class GlobMatcherTest {

    @Test
    void recursiveStarSlashStarMatchesTopLevelAndNested() {
        GlobMatcher matcher = GlobMatcher.compile("**/*.parquet");

        assertThat(matcher.matches("data.parquet")).isTrue();
        assertThat(matcher.matches("year=2024/data.parquet")).isTrue();
        assertThat(matcher.matches("a/b/c/data.parquet")).isTrue();
    }

    @Test
    void singleStarStaysWithinOneSegment() {
        GlobMatcher matcher = GlobMatcher.compile("*.parquet");

        assertThat(matcher.matches("data.parquet")).isTrue();
        assertThat(matcher.matches("year=2024/data.parquet")).isFalse();
    }

    @Test
    void braceWithSingleAlternativeMatchesExactly() {
        GlobMatcher matcher = GlobMatcher.compile("{example.parquet}");

        assertThat(matcher.matches("example.parquet")).isTrue();
        assertThat(matcher.matches("example.parquet.bak")).isFalse();
        assertThat(matcher.matches("sub/example.parquet")).isFalse();
    }

    @Test
    void braceAlternationMatchesTopLevelAndNested() {
        GlobMatcher matcher = GlobMatcher.compile("{*.parquet,**/*.parquet}");

        assertThat(matcher.matches("data.parquet")).isTrue();
        assertThat(matcher.matches("year=2024/data.parquet")).isTrue();
    }

    @Test
    void doubleStarSlashMatchesZeroOrMoreInteriorDirectories() {
        GlobMatcher matcher = GlobMatcher.compile("data/**/theme=buildings/f.parquet");

        assertThat(matcher.matches("data/theme=buildings/f.parquet")).isTrue();
        assertThat(matcher.matches("data/r=1/x=2/theme=buildings/f.parquet")).isTrue();
        assertThat(matcher.matches("data/r=1/theme=buildings/sub/f.parquet")).isFalse();
    }

    @Test
    void characterClassMatchesSetAndRange() {
        assertThat(GlobMatcher.compile("f[A-C].txt").matches("fB.txt")).isTrue();
        assertThat(GlobMatcher.compile("f[A-C].txt").matches("fD.txt")).isFalse();
    }

    @Test
    void negatedCharacterClassNeverMatchesSeparator() {
        assertThat(GlobMatcher.compile("a[!z]b.txt").matches("axb.txt")).isTrue();
        assertThat(GlobMatcher.compile("a[!z]b.txt").matches("a/b.txt")).isFalse();
    }

    @Test
    void unclosedBracketIsLiteral() {
        assertThat(GlobMatcher.compile("lt[1.txt").matches("lt[1.txt")).isTrue();
        assertThat(GlobMatcher.compile("lt[1.txt").matches("lt1.txt")).isFalse();
    }

    @Test
    void unclosedBraceIsLiteral() {
        assertThat(GlobMatcher.compile("br{a.txt").matches("br{a.txt")).isTrue();
        assertThat(GlobMatcher.compile("br{a.txt").matches("bra.txt")).isFalse();
    }

    /**
     * Replays the cross-repo golden table, the shared DuckDB-parity contract kept byte-identical with
     * tileverse-storage. Every {@code source=duckdb} row reflects the real DuckDB binary; {@code source=tileverse} rows
     * pin the brace-alternation extension.
     */
    @ParameterizedTest(name = "{0} ~ {1} -> {2}")
    @MethodSource("goldenCases")
    void matchesDuckDbGoldenTable(String glob, String key, boolean expected) {
        assertThat(GlobMatcher.compile(glob).matches(key)).isEqualTo(expected);
    }

    private static Stream<Arguments> goldenCases() {
        List<Arguments> cases = new ArrayList<>();
        try (InputStream in = GlobMatcherTest.class.getResourceAsStream("glob-cases.tsv");
                BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                String[] columns = line.split("\t");
                String glob = columns[0];
                String key = columns[1];
                boolean expected = Boolean.parseBoolean(columns[2]);
                cases.add(Arguments.of(glob, key, expected));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return cases.stream();
    }
}
