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
package io.tileverse.parquetry.internal.variant;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Reads the {@code apache/parquet-testing} Variant corpus cases for the Variant decode tests. Each case is a pair of
 * byte files, {@code <name>.metadata} and {@code <name>.value}, extracted once from the {@code parquetry-testkit} jar
 * into a temp directory shared across the JVM.
 */
final class VariantCorpus {

    private VariantCorpus() {}

    private static final String RESOURCE_DIR = "parquet-testing/variant";

    private static final Path EXTRACTED_DIR = extractOnce();

    static byte[] metadata(String name) {
        return readCaseFile(name + ".metadata");
    }

    static byte[] value(String name) {
        return readCaseFile(name + ".value");
    }

    private static byte[] readCaseFile(String fileName) {
        Path file = EXTRACTED_DIR.resolve(fileName);
        try {
            return Files.readAllBytes(file);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read variant corpus file " + fileName, e);
        }
    }

    private static Path extractOnce() {
        Path target = createTempDir();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> deleteRecursively(target)));
        return TestCorpus.extractDirectory(RESOURCE_DIR, target);
    }

    private static Path createTempDir() {
        try {
            return Files.createTempDirectory("parquetry-variant-corpus-");
        } catch (IOException e) {
            throw new UncheckedIOException("Could not create a temp directory for the variant corpus", e);
        }
    }

    private static void deleteRecursively(Path root) {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(VariantCorpus::deleteQuietly);
        } catch (IOException _) {
            // best-effort cleanup on JVM shutdown
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException _) {
            // best-effort cleanup on JVM shutdown
        }
    }
}
