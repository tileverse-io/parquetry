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
package io.tileverse.parquetry.benchmarks;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Writes a Parquet file with a single, deliberately large row group, the input the fetch-spill characterization needs.
 *
 * <p>A mandatory fetch buffers a whole row group's column chunks. Pin the row group large enough and pin the read
 * runtime's fetch budget tiny enough and that buffer cannot fit in pooled native memory; the read path then maps it
 * onto an on-disk file (reclaimable page cache) instead of anonymous RAM. This fixture provides the large end of that
 * test by pinning the writer's row-group threshold to a byte budget.
 *
 * <p>The data is deterministic: every column value is a pure function of the row index, with no clock or random source.
 * Two runs with the same {@code rows} and {@code wideColumns} produce byte-identical files, which keeps a benchmark
 * comparable across runs and across machines.
 *
 * <p>The file is written under a caller-supplied directory (a temp directory or {@code target/}) and is never committed
 * as test data; it is large by construction.
 */
final class LargeRowGroupFixture {

    private LargeRowGroupFixture() {}

    /**
     * Writes a fixture of {@code rows} rows and {@code wideColumns} DOUBLE payload columns into {@code file}, pinning
     * the row group to {@code rowGroupBytes} uncompressed bytes. The row count is chosen so the accumulated
     * uncompressed bytes comfortably exceed that threshold, producing a single large row group whose mandatory fetch
     * overflows a tiny budget.
     */
    static void write(Path file, long rows, int wideColumns, long rowGroupBytes) throws IOException {
        ParquetSchema schema = wideSchema(wideColumns);
        WriteOptions options = WriteOptions.builder()
                .tempDir(file.getParent())
                .rowGroupSize(WriteOptions.RowGroupSize.bytes(rowGroupBytes))
                .encodingPolicy("id", WriteOptions.EncodingPolicy.FORCE_PLAIN)
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            for (long index = 0; index < rows; index++) {
                appender.setLong(0, index);
                for (int col = 0; col < wideColumns; col++) {
                    appender.setDouble(1 + col, index * (col + 1) * 0.5);
                }
                appender.endRow();
            }
            appender.flush();
        }
    }

    /**
     * The number of rows whose {@code id INT64} plus {@code wideColumns} DOUBLE values reach roughly
     * {@code targetBytes} of uncompressed payload. Used to size a fixture against a target row-group byte budget
     * without measuring the file.
     */
    static long rowsForUncompressedBytes(long targetBytes, int wideColumns) {
        long bytesPerRow = Long.BYTES + (long) wideColumns * Double.BYTES;
        return Math.max(1L, targetBytes / bytesPerRow);
    }

    private static ParquetSchema wideSchema(int wideColumns) {
        List<SchemaNode> leaves = new ArrayList<>(1 + wideColumns);
        leaves.add(required("id", PrimitiveKind.INT64));
        for (int i = 0; i < wideColumns; i++) {
            leaves.add(required("v" + i, PrimitiveKind.DOUBLE));
        }
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, leaves, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive required(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.REQUIRED, kind, OptionalInt.empty(), Optional.empty(), -1);
    }
}
