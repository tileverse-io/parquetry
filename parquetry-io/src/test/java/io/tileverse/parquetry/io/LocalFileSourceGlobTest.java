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
package io.tileverse.parquetry.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The glob contract of {@link LocalFileSource#directory}: tileverse's {@code StoragePattern} syntax, replayed end to
 * end through a real directory listing. The golden table is the cross-repo DuckDB-parity contract, kept byte-identical
 * with tileverse-storage's copy; the remaining cases pin what the listing adds around the matcher.
 */
class LocalFileSourceGlobTest {

    @ParameterizedTest(name = "[{3}] {0} ~ {1} -> {2}")
    @MethodSource("goldenCases")
    void listsTheGoldenTableAsTileverseMatchesIt(
            String glob, String key, boolean expected, String source, @TempDir Path dir) {
        createFile(dir, key);

        List<String> listed = relativePathsUnder(dir, glob);

        assertThat(listed).isEqualTo(expected ? List.of(key) : List.of());
    }

    @Test
    void aPlainNameListsExactlyThatFile(@TempDir Path dir) {
        createFile(dir, "data.parquet");
        createFile(dir, "data.parquet.bak");
        createFile(dir, "sub/data.parquet");

        assertThat(relativePathsUnder(dir, "data.parquet")).containsExactly("data.parquet");
        assertThat(relativePathsUnder(dir, "sub/data.parquet")).containsExactly("sub/data.parquet");
    }

    @Test
    void aRegexMetacharacterInAPlainNameIsLiteral(@TempDir Path dir) {
        createFile(dir, "a+b.parquet");
        createFile(dir, "ab.parquet");
        createFile(dir, "aab.parquet");

        assertThat(relativePathsUnder(dir, "a+b.parquet")).containsExactly("a+b.parquet");
    }

    /** A differently cased path resolves on a case-insensitive filesystem, but the listing reports real names only. */
    @Test
    void matchesKeysByTheirRealNameOnly(@TempDir Path dir) {
        createFile(dir, "data/Part.parquet");

        assertThat(relativePathsUnder(dir, "Data/*.parquet")).isEmpty();
        assertThat(relativePathsUnder(dir, "data/part.parquet")).isEmpty();
        assertThat(relativePathsUnder(dir, "data/*.parquet")).containsExactly("data/Part.parquet");
    }

    @Test
    void aSegmentStartingWithADotIsOrdinary(@TempDir Path dir) {
        createFile(dir, ".hidden/a.parquet");

        assertThat(relativePathsUnder(dir, ".hidden/*.parquet")).containsExactly(".hidden/a.parquet");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unsafePatterns")
    void rejectsAnUnsafePattern(String description, String pattern, @TempDir Path dir) {
        assertThatIllegalArgumentException().isThrownBy(() -> LocalFileSource.directory(dir, pattern));
    }

    private static Stream<Arguments> unsafePatterns() {
        return Stream.of(
                Arguments.of("leading slash", "/data/*.parquet"),
                Arguments.of("parent segment", "../*.parquet"),
                Arguments.of("current-directory segment", "./*.parquet"),
                Arguments.of("interior parent segment", "data/../*.parquet"),
                Arguments.of("NUL character", "a\0b.parquet"));
    }

    @Test
    void rejectsAnEmptyGlob(@TempDir Path dir) {
        assertThatIllegalArgumentException().isThrownBy(() -> LocalFileSource.directory(dir, ""));
    }

    /** An illegal character range fails when the source is built, not on the first listing. */
    @Test
    void rejectsAGlobThatDoesNotCompile(@TempDir Path dir) {
        assertThatIllegalArgumentException().isThrownBy(() -> LocalFileSource.directory(dir, "f[z-a].parquet"));
    }

    private static List<String> relativePathsUnder(Path dir, String glob) {
        try (FileSource source = LocalFileSource.directory(dir, glob);
                Stream<FileEntry> entries = source.list()) {
            return entries.map(FileEntry::relativePath).sorted().toList();
        }
    }

    private static void createFile(Path dir, String relativePath) {
        Path file = dir.resolve(relativePath);
        try {
            Files.createDirectories(file.getParent());
            Files.writeString(file, relativePath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Stream<Arguments> goldenCases() {
        List<Arguments> rows = new ArrayList<>();
        try (InputStream in = LocalFileSourceGlobTest.class.getResourceAsStream("glob-cases.tsv");
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
                String source = columns[3];
                rows.add(Arguments.of(glob, key, expected, source));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return rows.stream();
    }
}
