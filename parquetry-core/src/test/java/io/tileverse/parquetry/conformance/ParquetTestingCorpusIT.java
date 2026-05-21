/*
 * Copyright (c) 2026 Tileverse.io
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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.LocalInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.read.ReadOptions;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;

import io.tileverse.io.ByteBufferPool;

/**
 * Conformance integration test that reads every {@code .parquet} file under
 * {@code parquetry-core/src/test/resources/parquet-testing/data/} (the {@code apache/parquet-testing} git submodule
 * corpus) through the parquetry pipeline and asserts each row matches a {@code parquet-avro} oracle reading the same
 * bytes.
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

    private static final Path DATA_DIR = Paths.get("src/test/resources/parquet-testing/data");
    private static final String EXCLUSIONS_RESOURCE = "/parquet-testing-exclusions.txt";

    @Test
    void submoduleIsCheckedOut() {
        assertThat(DATA_DIR)
                .as("parquet-testing submodule must be initialized; run `git submodule update --init --recursive`")
                .isDirectory();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("conformanceFixtures")
    void readMatchesParquetAvroOracle(String fixtureName) throws Exception {
        Path fixture = DATA_DIR.resolve(fixtureName);
        List<GenericRecord> expected = readAllViaParquetAvro(fixture);

        ByteBufferPool pool = new ByteBufferPool();
        List<ParquetRecord> actual = readAllViaParquetry(fixture, pool);

        assertThat(actual)
                .as("%s row count must match parquet-avro oracle", fixtureName)
                .hasSameSizeAs(expected);
        assertRowsMatchOracle(fixtureName, actual, expected);
        assertThat(outstandingBorrows(pool))
                .as("pooled buffers must drain after %s", fixtureName)
                .isZero();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("conformanceFixtures")
    void readBatchesMatchesOracleRowCount(String fixtureName) throws Exception {
        Path fixture = DATA_DIR.resolve(fixtureName);
        long expectedRows = readAllViaParquetAvro(fixture).size();

        ByteBufferPool pool = new ByteBufferPool();
        long actualRows = totalRowsViaBatchApi(fixture, pool);

        assertThat(actualRows)
                .as("%s batch row count must match parquet-avro oracle", fixtureName)
                .isEqualTo(expectedRows);
        assertThat(outstandingBorrows(pool))
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

    // --- row comparison ---

    private static void assertRowsMatchOracle(
            String fixtureName, List<ParquetRecord> actual, List<GenericRecord> expected) {
        if (actual.isEmpty()) {
            return;
        }
        ParquetSchema schema = actual.get(0).schema();
        List<ColumnPath> leaves = schema.leafColumns();
        for (int i = 0; i < actual.size(); i++) {
            assertRowMatchesOracle(fixtureName, i, leaves, schema, actual.get(i), expected.get(i));
        }
    }

    private static void assertRowMatchesOracle(
            String fixtureName,
            int row,
            List<ColumnPath> leaves,
            ParquetSchema schema,
            ParquetRecord actual,
            GenericRecord expected) {
        for (ColumnPath col : leaves) {
            assertCellMatchesOracle(fixtureName, row, col, schema, actual, expected);
        }
    }

    private static void assertCellMatchesOracle(
            String fixtureName,
            int row,
            ColumnPath col,
            ParquetSchema schema,
            ParquetRecord actual,
            GenericRecord expected) {
        Field.Primitive prim = primitiveAt(schema, col);
        Object oracleValue = oracleValueAt(expected, col);
        if (oracleValue == null) {
            assertThat(actual.isNull(col))
                    .as("%s @ row %d col %s should be null", fixtureName, row, col)
                    .isTrue();
            return;
        }
        assertThat(actual.isNull(col))
                .as("%s @ row %d col %s should not be null", fixtureName, row, col)
                .isFalse();
        switch (prim.kind()) {
            case BOOLEAN ->
                assertThat(actual.getBoolean(col))
                        .as("%s @ row %d col %s", fixtureName, row, col)
                        .isEqualTo((boolean) oracleValue);
            case INT32 ->
                assertThat(actual.getInt(col))
                        .as("%s @ row %d col %s", fixtureName, row, col)
                        .isEqualTo(((Number) oracleValue).intValue());
            case INT64 ->
                assertThat(actual.getLong(col))
                        .as("%s @ row %d col %s", fixtureName, row, col)
                        .isEqualTo(((Number) oracleValue).longValue());
            case FLOAT ->
                assertFloatsMatch(fixtureName, row, col, actual.getFloat(col), ((Number) oracleValue).floatValue());
            case DOUBLE ->
                assertDoublesMatch(fixtureName, row, col, actual.getDouble(col), ((Number) oracleValue).doubleValue());
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY ->
                assertThat(actual.getBinary(col))
                        .as("%s @ row %d col %s", fixtureName, row, col)
                        .isEqualTo(oracleBytes(oracleValue));
            // INT96 has no typed accessor in parquetry (it's the deprecated 96-bit timestamp) and is therefore
            // reached through the untyped get() method, which returns a read-only ByteBuffer matching what
            // parquet-avro returns once READ_INT96_AS_FIXED is enabled on the oracle.
            case INT96 ->
                assertThat(oracleBytes(actual.get(col)))
                        .as("%s @ row %d col %s", fixtureName, row, col)
                        .isEqualTo(oracleBytes(oracleValue));
        }
    }

    // FLOAT and DOUBLE need NaN-aware comparison: AssertJ's primitive isEqualTo uses ==, which is always false
    // for NaN; the parquet-testing corpus has fixtures (e.g. single_nan, nan_in_stats) that legitimately store NaN.
    private static void assertFloatsMatch(String fixtureName, int row, ColumnPath col, float actual, float expected) {
        if (Float.isNaN(expected)) {
            assertThat(Float.isNaN(actual))
                    .as("%s @ row %d col %s: expected NaN", fixtureName, row, col)
                    .isTrue();
        } else {
            assertThat(actual).as("%s @ row %d col %s", fixtureName, row, col).isEqualTo(expected);
        }
    }

    private static void assertDoublesMatch(
            String fixtureName, int row, ColumnPath col, double actual, double expected) {
        if (Double.isNaN(expected)) {
            assertThat(Double.isNaN(actual))
                    .as("%s @ row %d col %s: expected NaN", fixtureName, row, col)
                    .isTrue();
        } else {
            assertThat(actual).as("%s @ row %d col %s", fixtureName, row, col).isEqualTo(expected);
        }
    }

    private static Field.Primitive primitiveAt(ParquetSchema schema, ColumnPath col) {
        Optional<Field> field = schema.find(col);
        return field.filter(Field.Primitive.class::isInstance)
                .map(Field.Primitive.class::cast)
                .orElseThrow(() -> new IllegalStateException(
                        "Expected primitive leaf at " + col + " (corpus IT should not see nested columns)"));
    }

    // Walks a multi-part ColumnPath through nested GenericRecords. Returns null if any ancestor is null.
    private static Object oracleValueAt(GenericRecord row, ColumnPath col) {
        Object current = row;
        for (String part : col.parts()) {
            if (!(current instanceof GenericRecord g)) {
                return null;
            }
            current = g.get(part);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    private static byte[] oracleBytes(Object value) {
        return switch (value) {
            case Utf8 u -> u.getBytes();
            case ByteBuffer bb -> bytesOf(bb);
            case MemorySegment seg -> seg.toArray(JAVA_BYTE);
            case byte[] b -> b;
            case GenericData.Fixed f -> f.bytes();
            case CharSequence cs -> cs.toString().getBytes(StandardCharsets.UTF_8);
            default ->
                throw new IllegalStateException(
                        "Unhandled oracle value type: " + value.getClass().getName());
        };
    }

    private static byte[] bytesOf(ByteBuffer bb) {
        ByteBuffer dup = bb.duplicate();
        byte[] bytes = new byte[dup.remaining()];
        dup.get(bytes);
        return bytes;
    }

    // --- pipeline drivers ---

    private static List<ParquetRecord> readAllViaParquetry(Path fixture, ByteBufferPool pool) throws IOException {
        try (Storage storage = StorageFactory.open(fixture.getParent().toUri());
                RangeReader reader =
                        storage.openRangeReader(fixture.getFileName().toString())) {
            ParquetDataset dataset = ParquetDataset.open(reader);
            ReadOptions options = ReadOptions.builder().byteBufferPool(pool).build();
            try (Stream<ParquetRecord> records = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
                return records.toList();
            }
        }
    }

    private static long totalRowsViaBatchApi(Path fixture, ByteBufferPool pool) throws IOException {
        long[] total = {0L};
        try (Storage storage = StorageFactory.open(fixture.getParent().toUri());
                RangeReader reader =
                        storage.openRangeReader(fixture.getFileName().toString())) {
            ParquetDataset dataset = ParquetDataset.open(reader);
            ReadOptions options = ReadOptions.builder().byteBufferPool(pool).build();
            try (Stream<ParquetRecordBatch> batches =
                    dataset.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
                batches.forEach(batch -> {
                    try (ParquetRecordBatch owned = batch) {
                        total[0] += owned.rowCount();
                    }
                });
            }
        }
        return total[0];
    }

    private static List<GenericRecord> readAllViaParquetAvro(Path fixture) throws IOException {
        Configuration conf = new Configuration(false);
        // Avro deprecates INT96, so AvroParquetReader refuses fixtures whose schema has an INT96 column unless this
        // flag is set; surfacing INT96 as a fixed-length byte array matches what parquetry returns through get().
        conf.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);
        List<GenericRecord> rows = new ArrayList<>();
        try (ParquetReader<GenericData.Record> reader = AvroParquetReader.<GenericData.Record>builder(
                        new LocalInputFile(fixture))
                .withConf(conf)
                .build()) {
            GenericData.Record parquetRecord;
            while ((parquetRecord = reader.read()) != null) {
                rows.add(parquetRecord);
            }
        }
        return rows;
    }

    // --- pool accounting ---

    private static long outstandingBorrows(ByteBufferPool pool) {
        ByteBufferPool.PoolStatistics stats = pool.getStatistics();
        return (stats.created() + stats.reused()) - (stats.returned() + stats.discarded());
    }
}
