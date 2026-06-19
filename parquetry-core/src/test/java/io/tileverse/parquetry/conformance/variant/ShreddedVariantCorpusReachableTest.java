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
package io.tileverse.parquetry.conformance.variant;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Pins that the bundled {@code parquet-testing/shredded_variant} corpus extracts from the testkit jar and arrives with
 * every Parquet case file the later Variant read suites depend on. The corpus is flat: {@code cases.json} describes 138
 * cases, one of which is a placeholder with no Parquet file, which leaves 137 {@code .parquet} fixtures on disk.
 */
class ShreddedVariantCorpusReachableTest {

    private static final long EXPECTED_PARQUET_FILES = 137L;

    @Test
    void corpusExtractsWithAllCaseFiles(@TempDir Path root) throws IOException {
        TestCorpus.extractDirectory("parquet-testing/shredded_variant", root);

        assertThat(root.resolve("cases.json")).exists();

        try (Stream<Path> walk = Files.walk(root)) {
            long parquetFiles =
                    walk.filter(path -> path.toString().endsWith(".parquet")).count();
            assertThat(parquetFiles).isEqualTo(EXPECTED_PARQUET_FILES);
        }
    }
}
