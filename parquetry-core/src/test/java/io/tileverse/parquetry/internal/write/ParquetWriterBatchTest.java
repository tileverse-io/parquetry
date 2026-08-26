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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.FilteredRecordBatch;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class ParquetWriterBatchTest {

    @TempDir
    Path tempDir;

    @Test
    void batchRoundTripPreservesEveryCell() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"), requiredInt64("timestamp"), requiredBinary("name"));
        WriteOptions options = options().build();
        Path parquetFile = tempDir.resolve("batch-roundtrip.parquet");

        int rows = 64;
        int[] ids = new int[rows];
        long[] timestamps = new long[rows];
        MemorySegment[] names = new MemorySegment[rows];
        BitSet allValidBits = new BitSet(rows);
        allValidBits.set(0, rows);
        Validity allValid = Validity.of(allValidBits, rows);
        for (int i = 0; i < rows; i++) {
            ids[i] = i;
            timestamps[i] = 1_000_000L + i;
            names[i] = MemorySegment.ofArray(("row-" + i).getBytes(StandardCharsets.UTF_8))
                    .asReadOnly();
        }

        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(parquetFile), schema, options);
                ParquetRecordBatch batch = buildBatch(
                        schema,
                        rows,
                        Map.of(
                                ColumnPath.of("id"), IntVector.materialized(ids, allValid),
                                ColumnPath.of("timestamp"), LongVector.materialized(timestamps, allValid),
                                ColumnPath.of("name"), BinaryVector.materialized(names, allValid)))) {
            writer.writeBatch(batch);
        }

        List<Map<String, Object>> readBack = readAll(parquetFile, schema);
        assertThat(readBack).hasSize(rows);
        for (int i = 0; i < rows; i++) {
            assertThat(readBack.get(i))
                    .containsEntry("id", i)
                    .containsEntry("timestamp", 1_000_000L + i)
                    .containsEntry("name", ("row-" + i).getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void multipleBatchesFillSeveralRowGroups() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options().rowGroupSize(RowGroupSize.rows(1000)).build();
        Path parquetFile = tempDir.resolve("multi-batch.parquet");
        int batches = 100;
        int rowsPerBatch = 50;
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(parquetFile), schema, options)) {
            for (int b = 0; b < batches; b++) {
                int[] values = new int[rowsPerBatch];
                BitSet validBits = new BitSet(rowsPerBatch);
                validBits.set(0, rowsPerBatch);
                Validity valid = Validity.of(validBits, rowsPerBatch);
                for (int i = 0; i < rowsPerBatch; i++) {
                    values[i] = b * rowsPerBatch + i;
                }
                try (ParquetRecordBatch batch = buildBatch(
                        schema, rowsPerBatch, Map.of(ColumnPath.of("id"), IntVector.materialized(values, valid)))) {
                    writer.writeBatch(batch);
                }
            }
            assertThat(writer.rowGroupsWritten()).isEqualTo(5L);
        }
        long totalRows = batches * rowsPerBatch;
        try (ByteRangeSource source = ByteRangeSource.ofFile(parquetFile)) {
            io.tileverse.parquetry.format.FileMetaData footer =
                    io.tileverse.parquetry.format.ParquetFormat.readFooter(source);
            assertThat(footer.rowGroups()).hasSize(5);
            assertThat(footer.numRows()).isEqualTo(totalRows);
        }
    }

    @Test
    void batchWithNullsPreservesValidityBitset() throws Exception {
        ParquetSchema schema = flatSchema(optionalInt32("id"));
        WriteOptions options = options().build();
        Path parquetFile = tempDir.resolve("batch-nulls.parquet");

        int rows = 8;
        int[] ids = new int[rows];
        BitSet validBits = new BitSet(rows);
        for (int i = 0; i < rows; i++) {
            ids[i] = i * 10;
            if (i % 2 == 0) {
                validBits.set(i);
            }
        }
        Validity valid = Validity.of(validBits, rows);

        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(parquetFile), schema, options);
                ParquetRecordBatch batch =
                        buildBatch(schema, rows, Map.of(ColumnPath.of("id"), IntVector.materialized(ids, valid)))) {
            writer.writeBatch(batch);
        }

        try (ByteRangeSource source = ByteRangeSource.ofFile(parquetFile)) {
            ParquetFileReader dataset = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> stream =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                List<ParquetRecord> all = stream.toList();
                assertThat(all).hasSize(rows);
                for (int i = 0; i < rows; i++) {
                    if (i % 2 == 0) {
                        assertThat(all.get(i).isNull(ColumnPath.of("id"))).isFalse();
                        assertThat(all.get(i).getInt(ColumnPath.of("id"))).isEqualTo(i * 10);
                    } else {
                        assertThat(all.get(i).isNull(ColumnPath.of("id"))).isTrue();
                    }
                }
            }
        }
    }

    @Test
    void mixedSingleAndBatchWritesPreserveOrder() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt32("id"));
        WriteOptions options = options().build();
        Path parquetFile = tempDir.resolve("mixed.parquet");

        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(parquetFile), schema, options)) {
            writer.writeBatch(WriteFixtures.batch(schema, List.of(Map.of(ColumnPath.of("id"), 100))));
            int[] batchValues = {200, 201, 202};
            BitSet validBits = new BitSet(3);
            validBits.set(0, 3);
            Validity valid = Validity.of(validBits, 3);
            try (ParquetRecordBatch batch =
                    buildBatch(schema, 3, Map.of(ColumnPath.of("id"), IntVector.materialized(batchValues, valid)))) {
                writer.writeBatch(batch);
            }
            writer.writeBatch(WriteFixtures.batch(schema, List.of(Map.of(ColumnPath.of("id"), 300))));
        }

        List<Map<String, Object>> readBack = readAll(parquetFile, schema);
        assertThat(readBack).extracting(m -> m.get("id")).containsExactly(100, 200, 201, 202, 300);
    }

    @Test
    void sparselyFilteredBatchWritesExactlyTheSurvivingRows() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt64("id"), requiredDouble("measure"), requiredBinary("name"));
        WriteOptions options = options().build();
        Path parquetFile = tempDir.resolve("sparse-filtered.parquet");

        int rows = 1000;
        BitSet survivors = scatteredSurvivors(rows);
        List<Integer> expectedRows = survivors.stream().boxed().toList();

        try (ParquetRecordBatch source = denseBatch(schema, rows);
                ParquetFileWriter writer =
                        ParquetFileWriter.create(Files.newOutputStream(parquetFile), schema, options)) {
            ParquetRecordBatch filtered = FilteredRecordBatch.filtered(source, survivors, schema);
            writer.writeBatch(filtered);

            LongVector callersIds = (LongVector) filtered.columns().get(ColumnPath.of("id"));
            assertThat(callersIds.getLong(0))
                    .as("the writer must leave the caller's batch open and readable")
                    .isEqualTo(5_000_000_000L + expectedRows.get(0));
        }

        List<Map<String, Object>> readBack = readAll(parquetFile, schema);
        assertThat(readBack).hasSize(expectedRows.size());
        for (int logical = 0; logical < expectedRows.size(); logical++) {
            int sourceRow = expectedRows.get(logical);
            assertThat(readBack.get(logical))
                    .as("logical row %d (source row %d)", logical, sourceRow)
                    .containsEntry("id", 5_000_000_000L + sourceRow)
                    .containsEntry("measure", sourceRow * 0.25d)
                    .containsEntry("name", ("name-" + sourceRow).getBytes(StandardCharsets.UTF_8));
        }
    }

    // --- helpers ---

    @Test
    void rangeWindowedBatchWritesExactlyTheWindowsRows() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt64("id"), requiredDouble("measure"), requiredBinary("name"));
        Path parquetFile = tempDir.resolve("range-window.parquet");

        int rows = 200;
        int from = 37;
        int count = 45;
        try (ParquetRecordBatch source = denseBatch(schema, rows);
                ParquetFileWriter writer = ParquetFileWriter.create(
                        Files.newOutputStream(parquetFile), schema, options().build())) {
            writer.writeBatch(source.slice(from, count));
        }

        List<Map<String, Object>> readBack = readAll(parquetFile, schema);
        assertThat(readBack).hasSize(count);
        for (int logical = 0; logical < count; logical++) {
            int sourceRow = from + logical;
            assertThat(readBack.get(logical))
                    .as("logical row %d (source row %d)", logical, sourceRow)
                    .containsEntry("id", 5_000_000_000L + sourceRow)
                    .containsEntry("measure", sourceRow * 0.25d)
                    .containsEntry("name", ("name-" + sourceRow).getBytes(StandardCharsets.UTF_8));
        }
    }

    @Test
    void writingASelectedViewPackedIntoADenseBatchFailsLoudly() throws Exception {
        ParquetSchema schema = flatSchema(requiredInt64("id"), requiredDouble("measure"), requiredBinary("name"));
        Path parquetFile = tempDir.resolve("packed-selection.parquet");
        int rows = 100;
        BitSet survivors = scatteredSurvivors(rows);

        try (ParquetRecordBatch source = denseBatch(schema, rows);
                ParquetFileWriter writer = ParquetFileWriter.create(
                        Files.newOutputStream(parquetFile), schema, options().build())) {
            ParquetRecordBatch filtered = FilteredRecordBatch.filtered(source, survivors, schema);
            ParquetRecordBatch packed = DefaultParquetRecordBatch.ofHeap(
                    schema, new LinkedHashMap<>(filtered.columns()), filtered.rowCount());

            assertThatThrownBy(() -> writer.writeBatch(packed))
                    .isInstanceOf(ParquetWriteException.class)
                    .hasMessageContaining("selected view over its backing")
                    .hasMessageContaining("FilteredRecordBatch.compacted()")
                    .hasMessageMatching("(?s).*column (id|measure|name) .*");
        }
    }

    /** {@code rows} rows of int64 / double / binary, every cell derived from its row index. */
    private ParquetRecordBatch denseBatch(ParquetSchema schema, int rows) {
        long[] ids = new long[rows];
        double[] measures = new double[rows];
        MemorySegment[] names = new MemorySegment[rows];
        for (int i = 0; i < rows; i++) {
            ids[i] = 5_000_000_000L + i;
            measures[i] = i * 0.25d;
            names[i] = MemorySegment.ofArray(("name-" + i).getBytes(StandardCharsets.UTF_8))
                    .asReadOnly();
        }
        Validity allValid = Validity.allValid(rows);
        return buildBatch(
                schema,
                rows,
                Map.of(
                        ColumnPath.of("id"), LongVector.materialized(ids, allValid),
                        ColumnPath.of("measure"), DoubleVector.materialized(measures, allValid),
                        ColumnPath.of("name"), BinaryVector.materialized(names, allValid)));
    }

    /** Roughly a tenth of {@code rows}, scattered by an irregular stride to defeat any accidental pattern match. */
    private static BitSet scatteredSurvivors(int rows) {
        BitSet survivors = new BitSet(rows);
        for (int row = 3; row < rows; row += 7 + (row % 5)) {
            survivors.set(row);
        }
        return survivors;
    }

    private WriteOptions.Builder options() {
        return WriteOptions.builder().tempDir(tempDir);
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children = Stream.of(leaves).map(f -> (SchemaNode) f).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt32(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive optionalInt32(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredInt64(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredDouble(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredBinary(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static ParquetRecordBatch buildBatch(
            ParquetSchema schema, int rowCount, Map<ColumnPath, ColumnVector> columns) {
        Map<ColumnPath, ColumnVector> view = new LinkedHashMap<>(columns);
        return new DefaultParquetRecordBatch(schema, view, rowCount, Arena.ofShared());
    }

    private static List<Map<String, Object>> readAll(Path parquetFile, ParquetSchema schema) {
        List<Map<String, Object>> out = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(parquetFile)) {
            ParquetFileReader dataset = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> stream =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                stream.forEach(parquetRecord -> out.add(extractAll(parquetRecord, schema)));
            }
        }
        return out;
    }

    private static Map<String, Object> extractAll(ParquetRecord parquetRecord, ParquetSchema schema) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (ColumnPath leaf : schema.leafColumns()) {
            SchemaNode.Primitive primitive =
                    (SchemaNode.Primitive) schema.find(leaf).orElseThrow();
            if (parquetRecord.isNull(leaf)) {
                row.put(leaf.dot(), null);
                continue;
            }
            switch (primitive.kind()) {
                case INT32 -> row.put(leaf.dot(), parquetRecord.getInt(leaf));
                case INT64 -> row.put(leaf.dot(), parquetRecord.getLong(leaf));
                case DOUBLE -> row.put(leaf.dot(), parquetRecord.getDouble(leaf));
                case BYTE_ARRAY -> row.put(leaf.dot(), parquetRecord.getBinary(leaf));
                default -> throw new IllegalStateException("unsupported test kind: " + primitive.kind());
            }
        }
        return row;
    }
}
