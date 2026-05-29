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
package io.tileverse.parquetry.data.write;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.data.WriteRow;
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
        BitSet allValid = new BitSet(rows);
        allValid.set(0, rows);
        for (int i = 0; i < rows; i++) {
            ids[i] = i;
            timestamps[i] = 1_000_000L + i;
            names[i] = MemorySegment.ofArray(("row-" + i).getBytes(StandardCharsets.UTF_8))
                    .asReadOnly();
        }

        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(parquetFile), schema, options);
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
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(parquetFile), schema, options)) {
            for (int b = 0; b < batches; b++) {
                int[] values = new int[rowsPerBatch];
                BitSet valid = new BitSet(rowsPerBatch);
                valid.set(0, rowsPerBatch);
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
        BitSet valid = new BitSet(rows);
        for (int i = 0; i < rows; i++) {
            ids[i] = i * 10;
            if (i % 2 == 0) {
                valid.set(i);
            }
        }

        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(parquetFile), schema, options);
                ParquetRecordBatch batch =
                        buildBatch(schema, rows, Map.of(ColumnPath.of("id"), IntVector.materialized(ids, valid)))) {
            writer.writeBatch(batch);
        }

        try (ByteRangeSource source = ByteRangeSource.ofFile(parquetFile)) {
            ParquetDataset dataset = ParquetDataset.open(source);
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

        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(parquetFile), schema, options)) {
            writer.write(row(Map.of(ColumnPath.of("id"), 100)));
            int[] batchValues = {200, 201, 202};
            BitSet valid = new BitSet(3);
            valid.set(0, 3);
            try (ParquetRecordBatch batch =
                    buildBatch(schema, 3, Map.of(ColumnPath.of("id"), IntVector.materialized(batchValues, valid)))) {
                writer.writeBatch(batch);
            }
            writer.write(row(Map.of(ColumnPath.of("id"), 300)));
        }

        List<Map<String, Object>> readBack = readAll(parquetFile, schema);
        assertThat(readBack).extracting(m -> m.get("id")).containsExactly(100, 200, 201, 202, 300);
    }

    // --- helpers ---

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

    private static SchemaNode.Primitive requiredBinary(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static ParquetRecordBatch buildBatch(
            ParquetSchema schema, int rowCount, Map<ColumnPath, ColumnVector> columns) {
        Map<ColumnPath, ColumnVector> view = new LinkedHashMap<>(columns);
        return new DefaultParquetRecordBatch(schema, view, rowCount, Arena.ofShared());
    }

    private static WriteRow row(Map<ColumnPath, Object> values) {
        Map<ColumnPath, Object> copy = new HashMap<>(values);
        return copy::get;
    }

    private static List<Map<String, Object>> readAll(Path parquetFile, ParquetSchema schema) {
        List<Map<String, Object>> out = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(parquetFile)) {
            ParquetDataset dataset = ParquetDataset.open(source);
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
                case BYTE_ARRAY -> row.put(leaf.dot(), parquetRecord.getBinary(leaf));
                default -> throw new IllegalStateException("unsupported test kind: " + primitive.kind());
            }
        }
        return row;
    }
}
