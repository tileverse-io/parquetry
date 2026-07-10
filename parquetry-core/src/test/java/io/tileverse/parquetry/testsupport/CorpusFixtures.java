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
package io.tileverse.parquetry.testsupport;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Resolves the {@code apache/parquet-testing} corpus for the core conformance tests by extracting it once, from the
 * classpath ({@code parquetry-testkit} jar) into a temp directory shared across the JVM, and returns the {@code data/}
 * directory. Replaces the former {@code Paths.get("src/test/resources/...")} lookups, which depended on the working
 * directory and on the submodule living inside this module.
 */
public final class CorpusFixtures {

    private CorpusFixtures() {}

    private static final Path PARQUET_TESTING_DATA = extractOnce("parquet-testing/data");

    /**
     * The extracted {@code apache/parquet-testing/data} directory; the {@code .parquet} fixtures sit directly in it.
     */
    public static Path parquetTestingData() {
        return PARQUET_TESTING_DATA;
    }

    private static Path extractOnce(String resourceDir) {
        Path target;
        try {
            target = Files.createTempDirectory("parquetry-corpus-");
        } catch (IOException e) {
            throw new UncheckedIOException("Could not create a temp directory for the test corpus", e);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> deleteRecursively(target)));
        try {
            return TestCorpus.extractDirectory(resourceDir, target);
        } catch (IllegalArgumentException notBundled) {
            throw new IllegalStateException(
                    "Test corpus '" + resourceDir + "' is not on the classpath; initialize the git submodules with"
                            + " `git submodule update --init --recursive`",
                    notBundled);
        }
    }

    private static void deleteRecursively(Path root) {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(CorpusFixtures::deleteQuietly);
        } catch (IOException _) {
            // Best-effort cleanup of a temp directory at JVM shutdown.
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException _) {
            // Best-effort.
        }
    }
}
