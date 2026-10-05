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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.format.UnknownCodeException;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Reads the top-level {@code bad_data} fixtures of the {@code apache/parquet-testing} corpus, files reproducing reader
 * bugs reported against other implementations. A file with bytes contradicting its own metadata must fail with a
 * {@link ParquetFormatException} through both {@link ParquetFileReader#read} and {@link ParquetFileReader#readBatches},
 * unfiltered and filtered, never with a raw JDK exception or silently wrong rows, and must leave no pooled buffer
 * behind. A file holding valid data must read in full.
 *
 * <p>The {@code variants} subdirectory holds malformed Variant values and has its own suite.
 */
class ParquetTestingBadDataIT {

    private static final String BAD_DATA_RESOURCE = "parquet-testing/bad_data";

    /** Bit width 0 for the dictionary indices of a one-entry dictionary is legal: all indices are 0. */
    private static final String ZERO_BIT_WIDTH_INDICES = "ARROW-GH-43605.parquet";

    private static final int ZERO_BIT_WIDTH_INDICES_ROWS = 21186;
    private static final ColumnPath MIN_FL = ColumnPath.of("min_fl");

    private static final Set<String> READABLE = Set.of(ZERO_BIT_WIDTH_INDICES);

    private static final Map<String, Class<? extends ParquetFormatException>> REJECTED = Map.of(
            // row group 1 holds a column chunk absent from the schema
            "ARROW-GH-41317.parquet", MalformedFileException.class,
            // a dictionary page declares a negative value count; level streams end before their page's values
            "ARROW-GH-41321.parquet", MalformedFileException.class,
            // a list's repetition levels open with 1, a level placing values in a row that never started
            "ARROW-GH-45185.parquet", MalformedFileException.class,
            // a required FIXED_LEN_BYTE_ARRAY(4) column with pages holding fewer value bytes than values
            "ARROW-GH-47662.parquet", MalformedFileException.class,
            // a dictionary page declares a negative value count; column chunks lie past the end of the file
            "ARROW-RS-GH-6229-DICTHEADER.parquet", MalformedFileException.class,
            // a data page declares more values than its levels and its whole column chunk hold
            "ARROW-RS-GH-6229-LEVELS.parquet", MalformedFileException.class,
            // a schema element holds a corrupt physical type code
            "PARQUET-1481.parquet", UnknownCodeException.class);

    /** Read options with each pruning tier off: a filtered read then scans the whole column chunk. */
    private static final ReadOptions WITHOUT_PRUNING = ReadOptions.builder()
            .useStatsFilter(false)
            .useDictionaryFilter(false)
            .useColumnIndexFilter(false)
            .useBloomFilter(false)
            .build();

    /** The first value of ARROW-GH-47662, placed by the column index in the first page alone. */
    private static final byte[] FIRST_FLBA_VALUE = {0, 0, 3, (byte) 0xe8};

    @TempDir
    static Path corpusRoot;

    private static Path badData;

    private final SegmentPool pool = SegmentPool.create();

    @BeforeAll
    static void extractCorpus() {
        badData = TestCorpus.extractDirectory(BAD_DATA_RESOURCE, corpusRoot);
    }

    @Test
    void eachTopLevelFileIsClassified() throws IOException {
        Set<String> classified = new TreeSet<>(REJECTED.keySet());
        classified.addAll(READABLE);

        assertThat(topLevelParquetFiles())
                .as("top-level bad_data files; classify a new upstream file as rejected or readable")
                .containsExactlyElementsOf(classified);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rejectedFiles")
    void readRejectsWithFormatException(String fileName) {
        CorpusRead unfiltered = CorpusRead.unfiltered(fileName);

        assertThatThrownBy(() -> countRowsViaRead(unfiltered)).isInstanceOf(REJECTED.get(fileName));
        assertPoolDrained(fileName);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rejectedFiles")
    void readBatchesRejectsWithFormatException(String fileName) {
        CorpusRead unfiltered = CorpusRead.unfiltered(fileName);

        assertThatThrownBy(() -> countRowsViaReadBatches(unfiltered)).isInstanceOf(REJECTED.get(fileName));
        assertPoolDrained(fileName);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("filteredReads")
    void filteredReadRejectsWithMalformedFile(CorpusRead filtered) {
        assertThatThrownBy(() -> countRowsViaRead(filtered)).isInstanceOf(MalformedFileException.class);
        assertPoolDrained(filtered.fileName());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("filteredReads")
    void filteredReadBatchesRejectsWithMalformedFile(CorpusRead filtered) {
        assertThatThrownBy(() -> countRowsViaReadBatches(filtered)).isInstanceOf(MalformedFileException.class);
        assertPoolDrained(filtered.fileName());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("predicatesOverCorruptColumns")
    void filteredCountRejectsWithMalformedFile(CorpusRead filtered) {
        assertThatThrownBy(() -> countMatchingRows(filtered)).isInstanceOf(MalformedFileException.class);
        assertPoolDrained(filtered.fileName());
    }

    @Test
    void zeroBitWidthDictionaryIndicesReadAsTheOnlyEntryViaRead() {
        Path file = badData.resolve(ZERO_BIT_WIDTH_INDICES);

        List<Integer> values = minFlValuesViaRead(file);

        assertThat(values).hasSize(ZERO_BIT_WIDTH_INDICES_ROWS).containsOnly(0);
        assertPoolDrained(ZERO_BIT_WIDTH_INDICES);
    }

    @Test
    void zeroBitWidthDictionaryIndicesReadAsTheOnlyEntryViaReadBatches() {
        Path file = badData.resolve(ZERO_BIT_WIDTH_INDICES);

        List<Integer> values = minFlValuesViaReadBatches(file);

        assertThat(values).hasSize(ZERO_BIT_WIDTH_INDICES_ROWS).containsOnly(0);
        assertPoolDrained(ZERO_BIT_WIDTH_INDICES);
    }

    static Stream<String> rejectedFiles() {
        return REJECTED.keySet().stream().sorted();
    }

    static Stream<CorpusRead> filteredReads() {
        return Stream.concat(predicatesOverCorruptColumns(), corruptOutputColumns());
    }

    /**
     * Filtered reads evaluating their predicate over a corrupt column: the masked scan decodes that column eagerly. The
     * {@code bounds} family has no case: no {@code bad_data} file holds a geometry column.
     */
    static Stream<CorpusRead> predicatesOverCorruptColumns() {
        Predicate firstFlbaValue = Pred.col("flba_field").eq(FIRST_FLBA_VALUE);
        Predicate anyItemPresent = new Predicate.Quantified(
                MatchAction.ANY, Pred.col("outer", "list", "item", "c").isNotNull());
        return Stream.of(
                new CorpusRead(
                        "ARROW-GH-47662, column index narrowing the fetch to the first page",
                        "ARROW-GH-47662.parquet",
                        firstFlbaValue,
                        Projection.ALL,
                        ReadOptions.DEFAULTS),
                new CorpusRead(
                        "ARROW-GH-47662, masked scan over the whole chunk",
                        "ARROW-GH-47662.parquet",
                        Pred.not(firstFlbaValue),
                        Projection.ALL,
                        WITHOUT_PRUNING),
                new CorpusRead(
                        "ARROW-RS-GH-6229-LEVELS, masked scan over a repeated leaf",
                        "ARROW-RS-GH-6229-LEVELS.parquet",
                        anyItemPresent,
                        Projection.ALL,
                        ReadOptions.DEFAULTS));
    }

    /**
     * Filtered reads evaluating their predicate over a sound column and returning a corrupt one: the masked scan
     * decodes that one window by window. ARROW-GH-47662 and ARROW-RS-GH-6229-LEVELS hold a single column and cannot
     * drive this lane.
     */
    static Stream<CorpusRead> corruptOutputColumns() {
        return Stream.of(new CorpusRead(
                "ARROW-GH-41321, windowed lane over an output-only column",
                "ARROW-GH-41321.parquet",
                Pred.col("int32").isNotNull(),
                Projection.ofPhysical(List.of(ColumnPath.of("int64"))),
                ReadOptions.DEFAULTS));
    }

    private static List<String> topLevelParquetFiles() throws IOException {
        try (Stream<Path> entries = Files.list(badData)) {
            return entries.filter(Files::isRegularFile)
                    .map(path -> path.getFileName().toString())
                    .filter(name -> name.endsWith(".parquet"))
                    .sorted()
                    .toList();
        }
    }

    private void assertPoolDrained(String fileName) {
        assertThat(pool.stats().outstandingBorrows())
                .as("pooled buffers must drain after %s", fileName)
                .isZero();
    }

    /** One read of a {@code bad_data} file, described for the test report. */
    record CorpusRead(
            String description, String fileName, Predicate predicate, Projection projection, ReadOptions options) {

        static CorpusRead unfiltered(String fileName) {
            return new CorpusRead(fileName, fileName, Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS);
        }

        @Override
        public String toString() {
            return description;
        }
    }

    // --- read drivers; each consumes the whole stream ---

    private long countRowsViaRead(CorpusRead read) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(badData.resolve(read.fileName()))) {
            ParquetFileReader reader = openReader(source);
            try (Stream<ParquetRecord> records = reader.read(read.predicate(), read.projection(), read.options())) {
                return records.count();
            }
        }
    }

    private long countRowsViaReadBatches(CorpusRead read) {
        long rows = 0;
        try (ByteRangeSource source = ByteRangeSource.ofFile(badData.resolve(read.fileName()))) {
            ParquetFileReader reader = openReader(source);
            try (Stream<ParquetRecordBatch> batches =
                    reader.readBatches(read.predicate(), read.projection(), read.options())) {
                for (ParquetRecordBatch batch : (Iterable<ParquetRecordBatch>) batches::iterator) {
                    try (batch) {
                        rows += batch.rowCount();
                    }
                }
            }
        }
        return rows;
    }

    private long countMatchingRows(CorpusRead read) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(badData.resolve(read.fileName()))) {
            ParquetFileReader reader = openReader(source);
            return reader.count(read.predicate(), read.options());
        }
    }

    private List<Integer> minFlValuesViaRead(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = openReader(source);
            try (Stream<ParquetRecord> records =
                    reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                return records.map(ParquetTestingBadDataIT::minFl).toList();
            }
        }
    }

    private List<Integer> minFlValuesViaReadBatches(Path file) {
        List<Integer> values = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = openReader(source);
            try (Stream<ParquetRecordBatch> batches =
                    reader.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                for (ParquetRecordBatch batch : (Iterable<ParquetRecordBatch>) batches::iterator) {
                    try (batch) {
                        collectMinFl(batch, values);
                    }
                }
            }
        }
        return values;
    }

    private static void collectMinFl(ParquetRecordBatch batch, List<Integer> values) {
        for (int row = 0; row < batch.rowCount(); row++) {
            values.add(minFl(batch.materialize(row)));
        }
    }

    /** The row's {@code min_fl} cell, or {@code null} when the cell is null. */
    private static Integer minFl(ParquetRecord row) {
        if (row.isNull(MIN_FL)) {
            return null;
        }
        return row.getInt(MIN_FL);
    }

    private ParquetFileReader openReader(ByteRangeSource source) {
        ParquetRuntime runtime = ParquetRuntime.builder().segmentPool(pool).build();
        return ParquetFileReader.open(source, runtime, Optional.empty());
    }
}
