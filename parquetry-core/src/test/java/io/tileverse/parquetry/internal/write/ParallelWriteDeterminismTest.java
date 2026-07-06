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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.runtime.ComputeExecutor;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.ByteArrayByteSink;

/**
 * Pins the parallel-encode byte-identity contract at the file level: output bytes are independent of the compute pool's
 * shape, of completion order, and of the submit-versus-inline mix. The inline reference is produced by holding every
 * slot of a pool up front: each fan-out unit then runs on the caller, the serial path.
 */
class ParallelWriteDeterminismTest {

    @TempDir
    Path tempDir;

    private final List<ComputeExecutor> pools = new ArrayList<>();

    @AfterEach
    void shutdownPools() {
        pools.forEach(ComputeExecutor::shutdownNow);
    }

    private ComputeExecutor pool(int parallelism) {
        ComputeExecutor pool = ComputeExecutor.ofParallelism(parallelism);
        pools.add(pool);
        return pool;
    }

    /** A pool whose only slot is held for the test's duration: every fan-out unit runs inline on the caller. */
    private ComputeExecutor inlinePool() {
        ComputeExecutor pool = pool(1);
        assertThat(pool.tryAcquire()).isTrue();
        return pool;
    }

    @Test
    void parallelWriteIsByteIdenticalToInlineWrite() {
        ParquetSchema schema = flatSchema();
        List<ParquetRecordBatch> batches = flatBatches(schema);

        byte[] parallel = writeFile(schema, batches, pool(4));
        byte[] inline = writeFile(schema, batches, inlinePool());

        assertThat(parallel).isEqualTo(inline);
    }

    /**
     * The striped path: leaves under an OPTIONAL struct are shredded serially by the striper, then fed per leaf on the
     * pool. Rows are authored through the writer-bound appender, which presents the struct as a struct vector.
     */
    @Test
    void structWriteIsByteIdenticalAcrossParallelAndInlinePools() {
        byte[] parallel = writeStructFile(pool(4));
        byte[] inline = writeStructFile(inlinePool());
        assertThat(parallel).isEqualTo(inline);
    }

    @Test
    void repeatedParallelWritesAreDeterministic() {
        ParquetSchema schema = wideSchema();
        List<ParquetRecordBatch> batches = wideBatches(schema);
        ComputeExecutor pool = pool(4);

        byte[] first = writeFile(schema, batches, pool);
        byte[] second = writeFile(schema, batches, pool);

        assertThat(first).isEqualTo(second);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 8})
    void poolParallelismDoesNotChangeOutputBytes(int parallelism) {
        ParquetSchema schema = wideSchema();
        List<ParquetRecordBatch> batches = wideBatches(schema);

        byte[] reference = writeFile(schema, batches, inlinePool());
        byte[] parallel = writeFile(schema, batches, pool(parallelism));

        assertThat(parallel).isEqualTo(reference);
    }

    /**
     * The mixed-load smoke from the spec: several writers race for the same undersized pool, forcing every mix of
     * pooled and inline units, and each writer's output must still match the serial reference.
     */
    @Test
    void concurrentWritersSharingOnePoolStayByteIdentical() throws Exception {
        ParquetSchema schema = wideSchema();
        List<ParquetRecordBatch> batches = wideBatches(schema);
        byte[] reference = writeFile(schema, batches, inlinePool());

        ComputeExecutor shared = pool(2);
        int writerCount = 4;
        byte[][] outputs = new byte[writerCount][];
        Throwable[] failures = new Throwable[writerCount];
        List<Thread> workers = new ArrayList<>(writerCount);
        for (int w = 0; w < writerCount; w++) {
            int index = w;
            Thread worker = new Thread(() -> {
                try {
                    outputs[index] = writeFile(schema, batches, shared);
                } catch (Throwable t) {
                    failures[index] = t;
                }
            });
            worker.start();
            workers.add(worker);
        }
        for (Thread worker : workers) {
            worker.join();
        }

        for (int w = 0; w < writerCount; w++) {
            assertThat(failures[w]).isNull();
            assertThat(outputs[w]).isEqualTo(reference);
        }
    }

    private byte[] writeFile(ParquetSchema schema, List<ParquetRecordBatch> batches, ComputeExecutor pool) {
        ParquetRuntime runtime = ParquetRuntime.builder().computeExecutor(pool).build();
        WriteOptions options = WriteOptions.builder().tempDir(tempDir).build();
        ByteArrayByteSink sink = new ByteArrayByteSink();
        try (ParquetFileWriter writer = ParquetFileWriter.create(sink, schema, options, runtime)) {
            for (ParquetRecordBatch batch : batches) {
                writer.writeBatch(batch);
            }
        }
        return sink.toByteArray();
    }

    private byte[] writeStructFile(ComputeExecutor pool) {
        ParquetSchema schema = optionalStructSchema();
        ParquetRuntime runtime = ParquetRuntime.builder().computeExecutor(pool).build();
        WriteOptions options = WriteOptions.builder().tempDir(tempDir).build();
        ByteArrayByteSink sink = new ByteArrayByteSink();
        ColumnPath id = ColumnPath.of("id");
        ColumnPath bbox = ColumnPath.of("bbox");
        ColumnPath xmin = ColumnPath.of("bbox", "xmin");
        ColumnPath xmax = ColumnPath.of("bbox", "xmax");
        try (ParquetFileWriter writer = ParquetFileWriter.create(sink, schema, options, runtime)) {
            ParquetRecordBatchBuilder appender = writer.appender(64);
            for (int i = 0; i < 300; i++) {
                appender.setInt(id, i);
                if (i % 3 == 0) {
                    appender.setNull(bbox);
                } else {
                    appender.setInt(xmin, i);
                    appender.setInt(xmax, i + 1);
                }
                appender.endRow();
            }
        }
        return sink.toByteArray();
    }

    private static ParquetSchema optionalStructSchema() {
        SchemaNode.Primitive id = requiredInt32("id");
        SchemaNode.Primitive xmin = requiredInt32("xmin");
        SchemaNode.Primitive xmax = requiredInt32("xmax");
        SchemaNode.Group bbox =
                new SchemaNode.Group("bbox", Repetition.OPTIONAL, List.of(xmin, xmax), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, bbox), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static ParquetSchema flatSchema() {
        List<SchemaNode> children = List.of(requiredInt32("id"), requiredInt64("ts"), optionalBinary("name"));
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static List<ParquetRecordBatch> flatBatches(ParquetSchema schema) {
        List<ParquetRecordBatch> batches = new ArrayList<>();
        for (int b = 0; b < 4; b++) {
            List<Map<ColumnPath, Object>> rows = new ArrayList<>();
            for (int r = 0; r < 500; r++) {
                int rowId = b * 500 + r;
                Map<ColumnPath, Object> row = new HashMap<>();
                row.put(ColumnPath.of("id"), rowId);
                row.put(ColumnPath.of("ts"), 1_000_000L + rowId);
                if (rowId % 5 != 0) {
                    row.put(ColumnPath.of("name"), ("name-" + rowId).getBytes(StandardCharsets.UTF_8));
                }
                rows.add(row);
            }
            batches.add(WriteFixtures.batch(schema, rows));
        }
        return batches;
    }

    private static ParquetSchema wideSchema() {
        List<SchemaNode> children = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            children.add(requiredInt32("i" + i));
            children.add(requiredInt64("l" + i));
            children.add(optionalBinary("s" + i));
        }
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static List<ParquetRecordBatch> wideBatches(ParquetSchema schema) {
        List<ParquetRecordBatch> batches = new ArrayList<>();
        int rowsPerBatch = 400;
        for (int b = 0; b < 5; b++) {
            List<Map<ColumnPath, Object>> rows = new ArrayList<>(rowsPerBatch);
            for (int r = 0; r < rowsPerBatch; r++) {
                int rowId = b * rowsPerBatch + r;
                Map<ColumnPath, Object> row = new HashMap<>();
                for (int c = 0; c < 8; c++) {
                    row.put(ColumnPath.of("i" + c), rowId * 31 + c);
                    row.put(ColumnPath.of("l" + c), 1_000_000L * c + rowId);
                    if (rowId % 7 != 0) {
                        row.put(ColumnPath.of("s" + c), ("value-" + c + "-" + rowId).getBytes(StandardCharsets.UTF_8));
                    }
                }
                rows.add(row);
            }
            batches.add(WriteFixtures.batch(schema, rows));
        }
        return batches;
    }

    private static SchemaNode.Primitive requiredInt32(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredInt64(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive optionalBinary(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }
}
