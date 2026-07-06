/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.internal.write.conformance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.EncodingPolicy;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * Round-trips every flat fixture from {@code parquet-testing/data/} through the parquetry writer and asserts the
 * re-read records deep-equal the originals.
 *
 * <p>Fixtures with nested groups, repeated leaves, INT96 columns, Variant logical type, or other write-side gaps are
 * skipped via {@link org.junit.jupiter.api.Assumptions Assumptions} rather than failing the test.
 */
@Tag("conformance")
class RoundTripCorpusIT {

    private static final Path DATA_DIR = CorpusFixtures.parquetTestingData();
    private static final String EXCLUSIONS_RESOURCE = "/parquet-testing-exclusions.txt";

    @ParameterizedTest(name = "{0}")
    @MethodSource("corpusFixtures")
    void roundTripsCleanly(String fixtureName) throws Exception {
        Path fixture = DATA_DIR.resolve(fixtureName);

        ParquetSchema schema;
        List<RowSnapshot> originalRows;
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixture)) {
            ParquetFileReader dataset = ParquetFileReader.open(source);
            schema = dataset.schema();
            assumeTrue(isFlatPrimitiveSchema(schema), "fixture has nested or repeated columns");
            assumeTrue(
                    isWriterSupported(schema),
                    "fixture uses a primitive kind or logical type our writer cannot reproduce");
            originalRows = snapshotRows(dataset, schema);
        }

        Path workDir = Files.createTempDirectory("roundtrip-corpus-");
        try {
            Path rewritten = workDir.resolve("rewritten.parquet");
            rewrite(schema, originalRows, rewritten, workDir);

            List<RowSnapshot> roundTripped = readRowsFromFile(rewritten);
            assertThat(roundTripped)
                    .as("%s row count after round-trip", fixtureName)
                    .hasSameSizeAs(originalRows);
            for (int i = 0; i < originalRows.size(); i++) {
                assertRowsEqual(fixtureName, i, schema, originalRows.get(i), roundTripped.get(i));
            }
        } finally {
            deleteRecursively(workDir);
        }
    }

    // --- corpus iteration ---

    static Stream<String> corpusFixtures() throws IOException {
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
        try (InputStream in = RoundTripCorpusIT.class.getResourceAsStream(EXCLUSIONS_RESOURCE)) {
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

    // --- skip predicates ---

    private static boolean isFlatPrimitiveSchema(ParquetSchema schema) {
        for (SchemaNode child : schema.root().children()) {
            if (!(child instanceof SchemaNode.Primitive prim)) {
                return false;
            }
            if (prim.repetition() == Repetition.REPEATED) {
                return false;
            }
        }
        return true;
    }

    private static boolean isWriterSupported(ParquetSchema schema) {
        for (ColumnPath leaf : schema.leafColumns()) {
            SchemaNode.Primitive prim = (SchemaNode.Primitive)
                    schema.find(leaf).orElseThrow(() -> new IllegalStateException("missing leaf " + leaf));
            if (prim.kind() == PrimitiveKind.INT96) {
                return false;
            }
            if (prim.logicalType()
                    .filter(lt -> lt instanceof LogicalType.Variant)
                    .isPresent()) {
                return false;
            }
        }
        return true;
    }

    // --- snapshot + rewrite ---

    private static List<RowSnapshot> snapshotRows(ParquetFileReader dataset, ParquetSchema schema) {
        List<ColumnPath> leaves = schema.leafColumns();
        List<RowSnapshot> snapshots = new ArrayList<>();
        try (Stream<ParquetRecord> records =
                dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            records.forEach(parquetRecord -> snapshots.add(snapshotRow(parquetRecord, leaves)));
        }
        return snapshots;
    }

    private static RowSnapshot snapshotRow(ParquetRecord parquetRecord, List<ColumnPath> leaves) {
        Map<ColumnPath, Object> values = new HashMap<>();
        for (ColumnPath leaf : leaves) {
            if (parquetRecord.isNull(leaf)) {
                values.put(leaf, null);
                continue;
            }
            values.put(leaf, captureValue(parquetRecord, leaf));
        }
        return new RowSnapshot(values);
    }

    private static Object captureValue(ParquetRecord parquetRecord, ColumnPath leaf) {
        Object raw = parquetRecord.get(leaf);
        if (raw instanceof MemorySegment segment) {
            // Materialize a heap-resident copy so the backing page buffer can be released.
            return MemorySegment.ofArray(segment.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE))
                    .asReadOnly();
        }
        return raw;
    }

    private static void rewrite(ParquetSchema schema, List<RowSnapshot> rows, Path destination, Path tempDir)
            throws IOException {
        WriteOptions.Builder builder = WriteOptions.builder().tempDir(tempDir);
        // The reader can't decode FIXED_LEN_BYTE_ARRAY dictionary pages today; force PLAIN on those columns
        // so the rewritten file is consumable end-to-end. INT32/INT64/etc. still use the default dictionary attempt.
        for (ColumnPath leaf : schema.leafColumns()) {
            SchemaNode.Primitive prim = (SchemaNode.Primitive)
                    schema.find(leaf).orElseThrow(() -> new IllegalStateException("missing leaf " + leaf));
            if (prim.kind() == PrimitiveKind.FIXED_LEN_BYTE_ARRAY) {
                builder.encodingPolicy(prim.name(), EncodingPolicy.FORCE_PLAIN);
            }
        }
        WriteOptions options = builder.build();
        List<ColumnPath> leaves = schema.leafColumns();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(destination), schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            for (RowSnapshot row : rows) {
                appendSnapshotRow(appender, schema, leaves, row);
            }
            appender.flush();
        }
    }

    private static void appendSnapshotRow(
            ParquetRecordBatchBuilder appender, ParquetSchema schema, List<ColumnPath> leaves, RowSnapshot row) {
        for (ColumnPath leaf : leaves) {
            appendSnapshotCell(appender, primitiveAt(schema, leaf), leaf, row.valueOf(leaf));
        }
        appender.endRow();
    }

    private static SchemaNode.Primitive primitiveAt(ParquetSchema schema, ColumnPath leaf) {
        return (SchemaNode.Primitive)
                schema.find(leaf).orElseThrow(() -> new IllegalStateException("missing leaf " + leaf));
    }

    private static void appendSnapshotCell(
            ParquetRecordBatchBuilder appender, SchemaNode.Primitive leaf, ColumnPath path, Object value) {
        if (value == null) {
            appender.setNull(path);
            return;
        }
        switch (leaf.kind()) {
            case INT32 -> appender.setInt(path, (Integer) value);
            case INT64 -> appender.setLong(path, (Long) value);
            case FLOAT -> appender.setFloat(path, (Float) value);
            case DOUBLE -> appender.setDouble(path, (Double) value);
            case BOOLEAN -> appender.setBoolean(path, (Boolean) value);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> appender.setBinary(path, (MemorySegment) value);
            case INT96 -> throw new IllegalStateException("INT96 columns are excluded from the round-trip corpus");
        }
    }

    private static List<RowSnapshot> readRowsFromFile(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            List<ColumnPath> leaves = reader.schema().leafColumns();
            return snapshotRowsFromReader(reader, leaves);
        }
    }

    private static List<RowSnapshot> snapshotRowsFromReader(ParquetFileReader reader, List<ColumnPath> leaves) {
        List<RowSnapshot> snapshots = new ArrayList<>();
        try (Stream<ParquetRecord> records = reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            records.forEach(parquetRecord -> snapshots.add(snapshotRow(parquetRecord, leaves)));
        }
        return snapshots;
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted((a, b) -> b.getNameCount() - a.getNameCount()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException _) {
                    // best-effort cleanup
                }
            });
        }
    }

    // --- assertion ---

    private static void assertRowsEqual(
            String fixtureName, int row, ParquetSchema schema, RowSnapshot expected, RowSnapshot actual) {
        for (ColumnPath leaf : schema.leafColumns()) {
            Object expectedValue = expected.values().get(leaf);
            Object actualValue = actual.values().get(leaf);
            assertCellEqual(fixtureName, row, leaf, expectedValue, actualValue);
        }
    }

    private static void assertCellEqual(String fixtureName, int row, ColumnPath leaf, Object expected, Object actual) {
        if (expected == null) {
            assertThat(actual)
                    .as("%s @ row %d col %s expected null", fixtureName, row, leaf)
                    .isNull();
            return;
        }
        assertThat(actual)
                .as("%s @ row %d col %s should be non-null", fixtureName, row, leaf)
                .isNotNull();
        if (expected instanceof MemorySegment expectedSegment && actual instanceof MemorySegment actualSegment) {
            byte[] expectedBytes = expectedSegment.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE);
            byte[] actualBytes = actualSegment.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE);
            assertThat(actualBytes)
                    .as("%s @ row %d col %s binary bytes", fixtureName, row, leaf)
                    .isEqualTo(expectedBytes);
            return;
        }
        if (expected instanceof Float expectedFloat && actual instanceof Float actualFloat) {
            assertFloatsMatch(fixtureName, row, leaf, actualFloat, expectedFloat);
            return;
        }
        if (expected instanceof Double expectedDouble && actual instanceof Double actualDouble) {
            assertDoublesMatch(fixtureName, row, leaf, actualDouble, expectedDouble);
            return;
        }
        assertThat(actual).as("%s @ row %d col %s", fixtureName, row, leaf).isEqualTo(expected);
    }

    private static void assertFloatsMatch(String fixtureName, int row, ColumnPath leaf, float actual, float expected) {
        if (Float.isNaN(expected)) {
            assertThat(Float.isNaN(actual))
                    .as("%s @ row %d col %s expected NaN", fixtureName, row, leaf)
                    .isTrue();
        } else {
            assertThat(actual).as("%s @ row %d col %s", fixtureName, row, leaf).isEqualTo(expected);
        }
    }

    private static void assertDoublesMatch(
            String fixtureName, int row, ColumnPath leaf, double actual, double expected) {
        if (Double.isNaN(expected)) {
            assertThat(Double.isNaN(actual))
                    .as("%s @ row %d col %s expected NaN", fixtureName, row, leaf)
                    .isTrue();
        } else {
            assertThat(actual).as("%s @ row %d col %s", fixtureName, row, leaf).isEqualTo(expected);
        }
    }

    /** Heap-resident snapshot of one row; values are boxed primitives or read-only on-heap MemorySegments. */
    private record RowSnapshot(Map<ColumnPath, Object> values) {

        Object valueOf(ColumnPath path) {
            return values.get(path);
        }
    }
}
