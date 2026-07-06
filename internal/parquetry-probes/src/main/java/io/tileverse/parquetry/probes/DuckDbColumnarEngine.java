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
package io.tileverse.parquetry.probes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBResultSet;

/**
 * DuckDB's columnar arm. Each scan opens its own connection (the pool model) and streams the result as Arrow batches
 * via {@code arrowExportStream}, then consumes every value of every {@link FieldVector} on-heap with the typed leaf
 * accessors - the cost an application vectorizing DuckDB output pays - recursing structs and lists to their leaves.
 * That consumption churns the JVM heap and shows in the heap columns, the same as the parquetry and parquet-java arms.
 * DuckDB's hidden footprint is its off-heap internal buffer pool: {@link #offHeapPeakBytes()} reports it from the
 * profiler (system_peak_buffer_memory). The Arrow export buffer itself is small, one streamed batch at a time.
 */
final class DuckDbColumnarEngine implements ColumnarEngine {

    // Rows per exported Arrow batch. Large enough that the C Data Interface round-trip per batch is not the bottleneck
    // (a small batch size made DuckDB look slower than its decode), while still streaming rather than buffering the
    // whole result.
    private static final long ARROW_BATCH_SIZE = 131072L;

    private final Path file;
    private long offHeapPeakBytes;
    private long sink;

    DuckDbColumnarEngine(Path file) {
        this.file = file;
    }

    @Override
    public String name() {
        return "duckdb";
    }

    @Override
    public long scan() throws SQLException, IOException {
        Path profile = Files.createTempFile("parquetry-probe-duckdb-columnar-", ".json");
        try (BufferAllocator allocator = new RootAllocator();
                Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            // arrowExportStream pulls batches lazily; an open transaction must outlive that streaming, which autocommit
            // would end between batches ("ActiveTransaction called without active transaction").
            connection.setAutoCommit(false);
            enableProfiling(connection, profile);
            try (PreparedStatement statement = connection.prepareStatement(query());
                    ResultSet resultSet = statement.executeQuery()) {
                long rows = consumeArrow(resultSet, allocator);
                // The Arrow export allocator only holds one streamed batch; DuckDB's hidden footprint is its internal
                // buffer pool, which the profiler reports as system_peak_buffer_memory.
                offHeapPeakBytes =
                        Math.max(offHeapPeakBytes, DuckDbProfile.read(profile).peakBufferBytes());
                return rows;
            }
        } finally {
            Files.deleteIfExists(profile);
        }
    }

    private void enableProfiling(Connection connection, Path profile) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA enable_profiling='json'");
            statement.execute("PRAGMA profiling_output='" + profile.toString().replace("'", "''") + "'");
        }
    }

    private long consumeArrow(ResultSet resultSet, BufferAllocator allocator) throws SQLException, IOException {
        DuckDBResultSet duck = resultSet.unwrap(DuckDBResultSet.class);
        long rows = 0L;
        try (ArrowReader arrowReader = (ArrowReader) duck.arrowExportStream(allocator, ARROW_BATCH_SIZE)) {
            while (arrowReader.loadNextBatch()) {
                VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
                for (FieldVector vector : root.getFieldVectors()) {
                    touchArrow(vector);
                }
                rows += root.getRowCount();
            }
        }
        return rows;
    }

    @Override
    public long checksum() {
        return sink;
    }

    @Override
    public long offHeapPeakBytes() {
        return offHeapPeakBytes;
    }

    private String query() {
        java.util.List<String> columns = ProbeColumns.requested();
        String selectList = columns.isEmpty() ? "*" : String.join(", ", columns);
        return "SELECT " + selectList + " FROM read_parquet('"
                + file.toAbsolutePath().toString().replace("'", "''") + "')";
    }

    /** Folds every value of {@code vector} into the sink, descending structs and lists to their leaves. */
    private void touchArrow(FieldVector vector) {
        switch (vector) {
            case StructVector struct -> {
                for (FieldVector child : struct.getChildrenFromFields()) {
                    touchArrow(child);
                }
            }
            case ListVector list -> touchArrow((FieldVector) list.getDataVector());
            case IntVector ints -> forEachDefined(ints, row -> sink += ints.get(row));
            case BigIntVector longs -> forEachDefined(longs, row -> sink += longs.get(row));
            case SmallIntVector shorts -> forEachDefined(shorts, row -> sink += shorts.get(row));
            case TinyIntVector bytes -> forEachDefined(bytes, row -> sink += bytes.get(row));
            case Float4Vector floats -> forEachDefined(floats, row -> sink += (long) floats.get(row));
            case Float8Vector doubles -> forEachDefined(doubles, row -> sink += (long) doubles.get(row));
            case BitVector bits -> forEachDefined(bits, row -> sink += bits.get(row));
            case VarCharVector text -> forEachDefined(text, row -> sink += text.getValueLength(row));
            case VarBinaryVector binary -> forEachDefined(binary, row -> sink += binary.getValueLength(row));
            default -> touchGeneric(vector);
        }
    }

    /** Reads every value of an uncommon vector type through the boxing accessor; the leaf types above avoid the box. */
    private void touchGeneric(FieldVector vector) {
        for (int row = 0; row < vector.getValueCount(); row++) {
            Object value = vector.getObject(row);
            if (value != null) {
                sink += value.hashCode();
            }
        }
    }

    private static void forEachDefined(FieldVector vector, java.util.function.IntConsumer touch) {
        for (int row = 0; row < vector.getValueCount(); row++) {
            if (!vector.isNull(row)) {
                touch.accept(row);
            }
        }
    }
}
