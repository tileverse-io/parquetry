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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.variant.Variant;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

import tools.jackson.databind.json.JsonMapper;

/**
 * Reader-level conformance against the bundled {@code parquet-testing/shredded_variant} corpus, exercising both read
 * paths for scalar, object, and array shreddings: the eager {@link ParquetFileReader#readBatches} columnar assembly
 * path and the lazy {@link ParquetFileReader#read} streaming row path. Every valid, non-error case reads the single
 * {@code var} column through each path and asserts each row's reconstructed {@link Variant} serializes byte-for-byte to
 * the corpus' expected {@code .variant.bin} bytes.
 *
 * <p>The streaming path is exercised under a filter, the form a server-side scan with a predicate takes. A trivially
 * all-matching {@link Predicate.IsNotNull} on the required {@code id} column keeps every row (the corpus schema
 * declares {@code id} as a required {@code INT32}) while forcing the reader onto its filtered streaming form, which
 * defers Dremel assembly to the per-row view that reconstructs the {@code var} group lazily.
 *
 * <p>The corpus' error cases and its spec-invalid object-conflict fixtures are exercised as the negative tier through
 * both paths: each one must reject the full read with a {@link ParquetFormatException}, whether the reconstructor
 * rejects it eagerly during batch assembly (unsupported shredded types) or lazily during per-row reconstruction (the
 * value-vs-typed_value and non-object conflicts). The reader's posture over the three {@code -INVALID} object-conflict
 * fixtures is strict: they reject rather than reading the shredded value.
 *
 * <p>A lockstep check asserts the extracted directory and {@code cases.json} agree on the set of {@code .parquet}
 * fixtures: a corpus refresh that adds a file forces a conscious classification here rather than silently going
 * untested.
 */
class ShreddedVariantReadConformanceIT {

    private static final String CORPUS = "parquet-testing/shredded_variant";
    private static final ColumnPath VAR_COLUMN = ColumnPath.of("var");
    private static final ColumnPath ID_COLUMN = ColumnPath.of("id");

    /**
     * An all-matching predicate on the required {@code id} column. It keeps every row (the corpus declares {@code id}
     * as a required {@code INT32}) yet survives predicate normalization unfolded, which routes the streaming read onto
     * its filtered form and hence the lazy levels assembly path under test.
     */
    private static final Predicate ALL_ROWS = new Predicate.IsNotNull(ID_COLUMN);

    @TempDir
    static Path corpusDir;

    static Stream<Arguments> validReadableCases() throws IOException {
        TestCorpus.extractDirectory(CORPUS, corpusDir);
        List<Map<String, Object>> cases = readCases(corpusDir);
        return cases.stream()
                .filter(ShreddedVariantReadConformanceIT::isValidReadableCase)
                .map(testCase -> {
                    String parquetFile = (String) testCase.get("parquet_file");
                    return Arguments.of(parquetFile, testCase);
                });
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> readCases(Path root) throws IOException {
        byte[] json = Files.readAllBytes(root.resolve("cases.json"));
        return JsonMapper.builder().build().readValue(json, List.class);
    }

    private static boolean isValidReadableCase(Map<String, Object> testCase) {
        Object parquetFile = testCase.get("parquet_file");
        if (parquetFile == null) {
            return false;
        }
        if (testCase.get("error_message") != null) {
            return false;
        }
        return !((String) parquetFile).contains("-INVALID");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("validReadableCases")
    void reconstructsEveryRowToItsCanonicalVariantBytes(String parquetFile, Map<String, Object> testCase)
            throws Exception {
        List<String> expectedRowFiles = expectedRowFiles(testCase);
        readBatchesAndAssertRows(parquetFile, expectedRowFiles);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("validReadableCases")
    void reconstructsEveryRowToItsCanonicalVariantBytesThroughStreamingRead(
            String parquetFile, Map<String, Object> testCase) throws Exception {
        List<String> expectedRowFiles = expectedRowFiles(testCase);
        streamRowsAndAssert(parquetFile, expectedRowFiles);
    }

    static Stream<Arguments> errorCases() throws IOException {
        TestCorpus.extractDirectory(CORPUS, corpusDir);
        List<Map<String, Object>> cases = readCases(corpusDir);
        return cases.stream()
                .filter(ShreddedVariantReadConformanceIT::isErrorCase)
                .map(testCase -> {
                    String parquetFile = (String) testCase.get("parquet_file");
                    return Arguments.of(parquetFile, testCase);
                });
    }

    private static boolean isErrorCase(Map<String, Object> testCase) {
        Object parquetFile = testCase.get("parquet_file");
        if (parquetFile == null) {
            return false;
        }
        if (testCase.get("error_message") != null) {
            return true;
        }
        return ((String) parquetFile).contains("-INVALID");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("errorCases")
    void rejectsEveryErrorAndInvalidCase(String parquetFile, Map<String, Object> testCase) {
        assertThatThrownBy(() -> readEveryRowThroughBatches(parquetFile))
                .as("%s must reject the full read", parquetFile)
                .isInstanceOf(ParquetFormatException.class);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("errorCases")
    void rejectsEveryErrorAndInvalidCaseThroughStreamingRead(String parquetFile, Map<String, Object> testCase) {
        assertThatThrownBy(() -> streamEveryRow(parquetFile))
                .as("%s must reject the streaming read", parquetFile)
                .isInstanceOf(ParquetFormatException.class);
    }

    private void readBatchesAndAssertRows(String parquetFile, List<String> expectedRowFiles) throws IOException {
        Path file = corpusDir.resolve(parquetFile);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            int globalRow = 0;
            try (Stream<ParquetRecordBatch> batches =
                    reader.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                for (ParquetRecordBatch batch : (Iterable<ParquetRecordBatch>) batches::iterator) {
                    globalRow = assertBatchRows(batch, expectedRowFiles, globalRow);
                }
            }
            assertThat(globalRow)
                    .as("%s: every expected row was read", parquetFile)
                    .isEqualTo(expectedRowFiles.size());
        }
    }

    private void readEveryRowThroughBatches(String parquetFile) throws IOException {
        Path file = corpusDir.resolve(parquetFile);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecordBatch> batches =
                    reader.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                for (ParquetRecordBatch batch : (Iterable<ParquetRecordBatch>) batches::iterator) {
                    forceVariantReconstruction(batch);
                }
            }
        }
    }

    private void forceVariantReconstruction(ParquetRecordBatch batch) {
        ColumnVector vector = (ColumnVector) batch.columns().get(VAR_COLUMN);
        for (int row = 0; row < batch.rowCount(); row++) {
            Object value = vector.get(row);
            if (value instanceof Variant variant) {
                variant.serialize();
            }
        }
    }

    private void streamRowsAndAssert(String parquetFile, List<String> expectedRowFiles) throws IOException {
        Path file = corpusDir.resolve(parquetFile);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            int globalRow = 0;
            try (Stream<ParquetRecord> records =
                    reader.read(ALL_ROWS, Projection.ALL, Materializer.defaultRecord(), ReadOptions.DEFAULTS)) {
                for (ParquetRecord record : (Iterable<ParquetRecord>) records::iterator) {
                    assertStreamedRow(record, expectedRowFiles.get(globalRow), globalRow);
                    globalRow++;
                }
            }
            assertThat(globalRow)
                    .as("%s: every expected row streamed through the filtered read", parquetFile)
                    .isEqualTo(expectedRowFiles.size());
        }
    }

    private void assertStreamedRow(ParquetRecord record, String expectedRowFile, int globalRow) {
        Object value = record.get(VAR_COLUMN);
        if (expectedRowFile == null) {
            assertThat(value).as("row %d is a null variant", globalRow).isNull();
            return;
        }
        byte[] expected = readBytes(expectedRowFile);
        assertThat(value).as("row %d is a non-null variant", globalRow).isInstanceOf(Variant.class);
        assertThat(((Variant) value).serialize())
                .as("row %d serializes to its canonical variant bytes", globalRow)
                .isEqualTo(expected);
    }

    private void streamEveryRow(String parquetFile) throws IOException {
        Path file = corpusDir.resolve(parquetFile);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> records =
                    reader.read(ALL_ROWS, Projection.ALL, Materializer.defaultRecord(), ReadOptions.DEFAULTS)) {
                for (ParquetRecord record : (Iterable<ParquetRecord>) records::iterator) {
                    forceVariantReconstruction(record);
                }
            }
        }
    }

    private void forceVariantReconstruction(ParquetRecord record) {
        Object value = record.get(VAR_COLUMN);
        if (value instanceof Variant variant) {
            variant.serialize();
        }
    }

    private int assertBatchRows(ParquetRecordBatch batch, List<String> expectedRowFiles, int globalRow) {
        ColumnVector vector = (ColumnVector) batch.columns().get(VAR_COLUMN);
        assertThat(vector).as("'var' column present").isNotNull();
        for (int row = 0; row < batch.rowCount(); row++) {
            String expectedRowFile = expectedRowFiles.get(globalRow);
            assertRow(vector, row, expectedRowFile, globalRow);
            globalRow++;
        }
        return globalRow;
    }

    private void assertRow(ColumnVector vector, int row, String expectedRowFile, int globalRow) {
        Object value = vector.get(row);
        if (expectedRowFile == null) {
            assertThat(value).as("row %d is a null variant", globalRow).isNull();
            return;
        }
        byte[] expected = readBytes(expectedRowFile);
        assertThat(value).as("row %d is a non-null variant", globalRow).isInstanceOf(Variant.class);
        assertThat(((Variant) value).serialize())
                .as("row %d serializes to its canonical variant bytes", globalRow)
                .isEqualTo(expected);
    }

    private byte[] readBytes(String fileName) {
        try {
            return Files.readAllBytes(corpusDir.resolve(fileName));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * The per-row expected variant file names in row order. A single-record case names its one file under
     * {@code variant_file}; a multi-record case lists one entry per row under {@code variant_files}, a null entry
     * meaning that row holds a null variant.
     */
    @SuppressWarnings("unchecked")
    private static List<String> expectedRowFiles(Map<String, Object> testCase) {
        Object multi = testCase.get("variant_files");
        if (multi != null) {
            return new ArrayList<>((List<String>) multi);
        }
        List<String> single = new ArrayList<>(1);
        single.add((String) testCase.get("variant_file"));
        return single;
    }

    @Test
    void manifestAndDirectoryStayInLockstep() throws IOException {
        TestCorpus.extractDirectory(CORPUS, corpusDir);
        Set<String> onDisk = parquetFilesOnDisk();
        Set<String> referenced = parquetFilesReferencedByCases();
        assertThat(onDisk)
                .as("every shredded variant parquet fixture is classified by cases.json")
                .isEqualTo(referenced);
    }

    private Set<String> parquetFilesOnDisk() throws IOException {
        try (Stream<Path> walk = Files.walk(corpusDir)) {
            return walk.filter(path -> path.toString().endsWith(".parquet"))
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toSet());
        }
    }

    private Set<String> parquetFilesReferencedByCases() throws IOException {
        return readCases(corpusDir).stream()
                .map(testCase -> (String) testCase.get("parquet_file"))
                .filter(parquetFile -> parquetFile != null)
                .collect(Collectors.toSet());
    }
}
