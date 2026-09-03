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
package io.tileverse.parquetry.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * Verifies that the batch pass-through write path (readBatches into writeBatch, the {@code par cp} pump) preserves
 * container-in-container shapes - columns whose maximum repetition level is 2 or more - which
 * {@code ParquetRecordBatchBuilder} cannot author yet. The write pipeline below the builder is depth-agnostic: leaf
 * values and repetition / definition levels flow through verbatim, and this suite pins that property.
 *
 * <p>A copied file must read identically to its source through the parquet-java oracle AND through parquetry itself.
 * Parquetry-vs-oracle is deliberately not compared here: these legacy fixtures are the ones
 * {@code parquet-testing-exclusions.txt} excludes from the read conformance sweep (parquetry materializes their
 * unannotated structural groups as structs where the oracle reads lists / maps). That read-interpretation divergence is
 * tracked there and is independent of write fidelity.
 */
class NestedContainerPassThroughWriteIT {

    private static final Path DATA_DIR = CorpusFixtures.parquetTestingData();

    @TempDir
    Path tempDir;

    static Stream<String> deeplyNestedFixtures() {
        return Stream.of(
                "nested_lists.snappy.parquet",
                "nested_maps.snappy.parquet",
                "nullable.impala.parquet",
                "nonnullable.impala.parquet");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("deeplyNestedFixtures")
    void passThroughCopyPreservesContainerInContainerShapes(String fixtureName) throws IOException {
        Path original = DATA_DIR.resolve(fixtureName);
        Path copy = tempDir.resolve("copy-" + fixtureName);

        int maxRep = copyThroughBatchPump(original, copy);
        assertThat(maxRep)
                .as("%s must exercise container-in-container levels", fixtureName)
                .isGreaterThanOrEqualTo(2);

        assertSameRows(
                fixtureName + " through parquet-java", ParquetJavaOracle.read(original), ParquetJavaOracle.read(copy));
        assertSameRows(
                fixtureName + " through parquetry",
                readCanonicalViaParquetry(original),
                readCanonicalViaParquetry(copy));
    }

    /**
     * Copies the file the way {@code par cp} does - each batch streams straight from the reader into the writer - and
     * returns the schema's maximum repetition level across all leaves.
     */
    private int copyThroughBatchPump(Path original, Path copy) throws IOException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(original)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            ParquetSchema schema = reader.schema();
            WriteOptions options = WriteOptions.builder().tempDir(tempDir).build();
            try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(copy), schema, options);
                    Stream<ParquetRecordBatch> batches =
                            reader.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                batches.forEach(batch -> {
                    try (batch) {
                        writer.writeBatch(batch);
                    }
                });
            }
            return maxRepetitionLevel(schema);
        }
    }

    private static int maxRepetitionLevel(ParquetSchema schema) {
        return schema.leafColumns().stream()
                .mapToInt(leaf -> schema.maxLevels(leaf).maxRepetitionLevel())
                .max()
                .orElse(0);
    }

    private static List<Map<String, Object>> readCanonicalViaParquetry(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            ParquetSchema schema = reader.schema();
            try (Stream<ParquetRecord> records =
                    reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                return records.map(record -> CanonicalRow.fromParquetry(record, schema))
                        .toList();
            }
        }
    }

    private static void assertSameRows(
            String what, List<Map<String, Object>> expected, List<Map<String, Object>> actual) {
        assertThat(actual).as("%s row count", what).hasSameSizeAs(expected);
        for (int i = 0; i < expected.size(); i++) {
            assertThat(CanonicalRow.deepEquals(actual.get(i), expected.get(i)))
                    .as("%s row %d: original %s vs copy %s", what, i, expected.get(i), actual.get(i))
                    .isTrue();
        }
    }
}
