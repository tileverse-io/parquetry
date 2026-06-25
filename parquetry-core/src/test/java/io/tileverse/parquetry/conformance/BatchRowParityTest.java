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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

import lombok.NonNull;

/**
 * Regression net asserting that the row API ({@link ParquetFileReader#read()}) and the batch API
 * ({@link ParquetFileReader#readBatches()}) agree cell-by-cell across every non-excluded fixture in the
 * {@code apache/parquet-testing} corpus.
 *
 * <p>For each fixture both APIs decode the same bytes through the same column readers; the two paths only differ in how
 * rows are surfaced to user code. Disagreement between them indicates a defect in one of the two surface layers (record
 * adapter, batch vector materialization, or assembler) rather than in the page decoder.
 *
 * <p>Fixture iteration and the exclusion list are shared with {@link ParquetTestingCorpusIT}; nested LIST/MAP/STRUCT
 * fixtures whose batch driver wiring is pending land in {@code parquet-testing-exclusions.txt} and are filtered out.
 */
class BatchRowParityTest {

    private static final Path DATA_DIR = CorpusFixtures.parquetTestingData();
    private static final String EXCLUSIONS_RESOURCE = "/parquet-testing-exclusions.txt";

    @Test
    void submoduleIsCheckedOut() {
        assertThat(DATA_DIR)
                .as("parquet-testing submodule must be initialized; run `git submodule update --init --recursive`")
                .isDirectory();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("conformanceFixtures")
    void rowAndBatchApisAgree(String fixtureName) {
        Path fixture = DATA_DIR.resolve(fixtureName);
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixture)) {
            ParquetFileReader dataset = ParquetFileReader.open(source);
            ParquetSchema schema = dataset.schema();
            List<ColumnPath> leaves = schema.leafColumns();

            List<Map<ColumnPath, Object>> rowApiSnapshots = snapshotViaRowApi(dataset, schema, leaves);
            List<Map<ColumnPath, Object>> batchApiSnapshots = snapshotViaBatchApi(dataset, schema, leaves);

            assertThat(batchApiSnapshots)
                    .as("%s: batch API row count must match row API row count", fixtureName)
                    .hasSameSizeAs(rowApiSnapshots);

            for (int row = 0; row < rowApiSnapshots.size(); row++) {
                assertRowSnapshotsAgree(fixtureName, row, leaves, rowApiSnapshots.get(row), batchApiSnapshots.get(row));
            }
        }
    }

    // --- parameter source ---

    static Stream<String> conformanceFixtures() throws IOException {
        if (!Files.isDirectory(DATA_DIR)) {
            return Stream.empty();
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
        try (InputStream in = BatchRowParityTest.class.getResourceAsStream(EXCLUSIONS_RESOURCE)) {
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
        if (hash < 0) {
            return line;
        }
        return line.substring(0, hash);
    }

    // --- pipeline drivers ---

    private static List<Map<ColumnPath, Object>> snapshotViaRowApi(
            ParquetFileReader dataset, ParquetSchema schema, List<ColumnPath> leaves) {
        try (Stream<ParquetRecord> records =
                dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            return records.map(row -> snapshotRow(row, schema, leaves)).toList();
        }
    }

    private static List<Map<ColumnPath, Object>> snapshotViaBatchApi(
            ParquetFileReader dataset, ParquetSchema schema, List<ColumnPath> leaves) {
        List<Map<ColumnPath, Object>> snapshots = new ArrayList<>();
        ReadOptions options = ReadOptions.DEFAULTS;
        try (Stream<ParquetRecordBatch> batches = dataset.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
            batches.forEach(batch -> {
                try (ParquetRecordBatch owned = batch) {
                    for (int rowIndex = 0; rowIndex < owned.rowCount(); rowIndex++) {
                        snapshots.add(snapshotRow(owned.materialize(rowIndex), schema, leaves));
                    }
                }
            });
        }
        return snapshots;
    }

    // --- snapshot + comparison ---

    private static Map<ColumnPath, Object> snapshotRow(
            @NonNull ParquetRecord row, @NonNull ParquetSchema schema, @NonNull List<ColumnPath> leaves) {
        Map<ColumnPath, Object> cells = new LinkedHashMap<>(leaves.size());
        for (ColumnPath col : leaves) {
            cells.put(col, snapshotCell(row, schema, col));
        }
        return cells;
    }

    private static Object snapshotCell(ParquetRecord row, ParquetSchema schema, ColumnPath col) {
        if (row.isNull(col)) {
            return null;
        }
        PrimitiveKind kind = primitiveKindAt(schema, col);
        return switch (kind) {
            case BOOLEAN -> Boolean.valueOf(row.getBoolean(col));
            case INT32 -> Integer.valueOf(row.getInt(col));
            case INT64 -> Long.valueOf(row.getLong(col));
            case FLOAT -> Float.valueOf(row.getFloat(col));
            case DOUBLE -> Double.valueOf(row.getDouble(col));
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> row.getBinary(col);
            case INT96 -> int96Bytes(row.get(col));
        };
    }

    private static byte[] int96Bytes(Object raw) {
        if (raw instanceof MemorySegment segment) {
            return segment.toArray(JAVA_BYTE);
        }
        throw new IllegalStateException("INT96 column surface should be MemorySegment, got "
                + (raw == null ? "null" : raw.getClass().getName()));
    }

    private static PrimitiveKind primitiveKindAt(ParquetSchema schema, ColumnPath col) {
        Optional<SchemaNode> field = schema.find(col);
        if (field.isEmpty() || !(field.get() instanceof SchemaNode.Primitive primitive)) {
            throw new IllegalStateException(
                    "Expected primitive leaf at " + col + "; the parity test only covers flat-primitive fixtures");
        }
        return primitive.kind();
    }

    private static void assertRowSnapshotsAgree(
            String fixtureName,
            int row,
            List<ColumnPath> leaves,
            Map<ColumnPath, Object> rowApi,
            Map<ColumnPath, Object> batchApi) {
        assertThat(batchApi.keySet())
                .as("%s @ row %d: batch API columns must match row API columns", fixtureName, row)
                .isEqualTo(rowApi.keySet());
        for (ColumnPath col : leaves) {
            assertCellsAgree(fixtureName, row, col, rowApi.get(col), batchApi.get(col));
        }
    }

    private static void assertCellsAgree(
            String fixtureName, int row, ColumnPath col, Object rowApiValue, Object batchApiValue) {
        if (rowApiValue instanceof byte[] rowBytes) {
            assertThat(batchApiValue)
                    .as("%s @ row %d col %s: batch API value should be byte[] to match row API", fixtureName, row, col)
                    .isInstanceOf(byte[].class);
            byte[] batchBytes = (byte[]) batchApiValue;
            assertThat(batchBytes)
                    .as("%s @ row %d col %s", fixtureName, row, col)
                    .isEqualTo(rowBytes);
            return;
        }
        assertThat(Objects.equals(rowApiValue, batchApiValue))
                .as(
                        "%s @ row %d col %s: row API value <%s> differs from batch API value <%s>",
                        fixtureName, row, col, rowApiValue, batchApiValue)
                .isTrue();
    }
}
