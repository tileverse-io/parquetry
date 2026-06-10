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
package io.tileverse.parquetry.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.data.ParquetRuntime;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * Conformance integration test that reads every {@code .parquet} file under
 * {@code parquetry-core/src/test/resources/parquet-testing/data/} (the {@code apache/parquet-testing} git submodule
 * corpus) through the parquetry pipeline and asserts each row matches a parquet-java {@code SimpleGroup} oracle reading
 * the same bytes. The comparison is a full materialized-tree deep equality: both readers produce the same
 * dependency-free canonical tree (nested maps for structs, lists for repeated columns, byte buffers for binary leaves)
 * and the trees are compared cell by cell.
 *
 * <p>Files that exercise features parquetry doesn't yet support (Parquet Modular Encryption, repeated columns and other
 * nested shapes, intentionally-corrupt fixtures, exotic codecs) are listed in {@code parquet-testing-exclusions.txt}
 * with a one-line justification each. Adding a file to that list is the intended way to acknowledge a known gap;
 * un-excluding it requires shipping the feature that makes it pass.
 *
 * <p>Runs under the failsafe phase (filename ends in {@code IT}) so {@code make test-unit} stays fast while {@code make
 * test-it} (or {@code make verify}) exercises the corpus end-to-end.
 */
class ParquetTestingCorpusIT {

    private static final Path DATA_DIR = CorpusFixtures.parquetTestingData();
    private static final String EXCLUSIONS_RESOURCE = "/parquet-testing-exclusions.txt";

    @Test
    void submoduleIsCheckedOut() {
        assertThat(DATA_DIR)
                .as("parquet-testing submodule must be initialized; run `git submodule update --init --recursive`")
                .isDirectory();
    }

    // Guards against an over-broad exclusion sweep silently emptying the corpus, which would let the parameterized
    // deep-compare report zero invocations and pass vacuously.
    @Test
    void corpusHasFixturesToCompare() throws IOException {
        assertThat(conformanceFixtures().toList())
                .as("active conformance corpus must not be near-empty")
                .hasSizeGreaterThan(30);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("conformanceFixtures")
    void readMatchesParquetJavaOracle(String fixtureName) {
        Path fixture = DATA_DIR.resolve(fixtureName);
        List<Map<String, Object>> expected = ParquetJavaOracle.read(fixture);

        SegmentPool pool = SegmentPool.create();
        List<Map<String, Object>> actual = readCanonicalViaParquetry(fixture, pool);

        assertThat(actual)
                .as("%s row count must match parquet-java oracle", fixtureName)
                .hasSameSizeAs(expected);
        for (int i = 0; i < actual.size(); i++) {
            assertThat(CanonicalRow.deepEquals(actual.get(i), expected.get(i)))
                    .as("%s row %d: parquetry %s vs oracle %s", fixtureName, i, actual.get(i), expected.get(i))
                    .isTrue();
        }
        assertThat(pool.stats().outstandingBorrows())
                .as("pooled buffers must drain after %s", fixtureName)
                .isZero();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("conformanceFixtures")
    void readBatchesMatchesOracleRowCount(String fixtureName) {
        Path fixture = DATA_DIR.resolve(fixtureName);
        long expectedRows = ParquetJavaOracle.read(fixture).size();

        SegmentPool pool = SegmentPool.create();
        long actualRows = totalRowsViaBatchApi(fixture, pool);

        assertThat(actualRows)
                .as("%s batch row count must match parquet-java oracle", fixtureName)
                .isEqualTo(expectedRows);
        assertThat(pool.stats().outstandingBorrows())
                .as("pooled buffers must drain after %s", fixtureName)
                .isZero();
    }

    // --- parameter source ---

    static Stream<String> conformanceFixtures() throws IOException {
        if (!Files.isDirectory(DATA_DIR)) {
            return Stream.empty(); // submodule not initialized; @Test submoduleIsCheckedOut will fail loudly
        }
        Set<String> exclusions = loadExclusions();
        Set<String> fixtures = new TreeSet<>();
        try (Stream<Path> entries = Files.list(DATA_DIR)) {
            entries.filter(Files::isRegularFile)
                    .map(p -> p.getFileName().toString())
                    .filter(name -> name.endsWith(".parquet"))
                    .filter(name -> !exclusions.contains(name))
                    .forEach(fixtures::add);
        }
        return fixtures.stream();
    }

    private static Set<String> loadExclusions() throws IOException {
        Set<String> excluded = new HashSet<>();
        try (InputStream in = ParquetTestingCorpusIT.class.getResourceAsStream(EXCLUSIONS_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("Missing exclusions resource: " + EXCLUSIONS_RESOURCE);
            }
            String content = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            for (String rawLine : content.split("\n")) {
                String entry = stripComment(rawLine).trim();
                if (!entry.isEmpty()) {
                    excluded.add(entry);
                }
            }
        }
        return excluded;
    }

    private static String stripComment(String line) {
        int hash = line.indexOf('#');
        return hash < 0 ? line : line.substring(0, hash);
    }

    // --- pipeline drivers ---

    private static List<Map<String, Object>> readCanonicalViaParquetry(Path fixture, SegmentPool pool) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixture)) {
            ParquetRuntime runtime = ParquetRuntime.builder().segmentPool(pool).build();
            ParquetReader dataset = ParquetReader.open(source, runtime, Optional.empty());
            ParquetSchema schema = dataset.schema();
            try (Stream<ParquetRecord> records =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                return records.map(parquetRecord -> CanonicalRow.fromParquetry(parquetRecord, schema))
                        .toList();
            }
        }
    }

    private static long totalRowsViaBatchApi(Path fixture, SegmentPool pool) {
        long[] total = {0L};
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixture)) {
            ParquetRuntime runtime = ParquetRuntime.builder().segmentPool(pool).build();
            ParquetReader dataset = ParquetReader.open(source, runtime, Optional.empty());
            try (Stream<ParquetRecordBatch> batches =
                    dataset.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                batches.forEach(batch -> {
                    try (ParquetRecordBatch owned = batch) {
                        total[0] += owned.rowCount();
                    }
                });
            }
        }
        return total[0];
    }
}
