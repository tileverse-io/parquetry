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
package io.tileverse.parquetry.arrow;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.OutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.Pipe;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * End-to-end proof that parquetry's filtered, projected output agrees with DuckDB's native Parquet reader. A real
 * corpus file ({@code alltypes_plain.parquet}) is read by parquetry with a record-level predicate and a column
 * projection, encoded as an Arrow IPC stream, streamed through an NIO {@link Pipe} into DuckDB through the Arrow C Data
 * Interface, then cross-checked against DuckDB reading the same file directly with an equivalent SQL filter.
 *
 * <p>The projection drops the file's INT96 {@code timestamp_col} (which the Arrow writer rejects) along with the other
 * columns, keeping only the three Arrow-supported columns the test compares. Record-level filtering (not row-group
 * pruning) is what makes parquetry's output row-exact against DuckDB's {@code WHERE} clause: the corpus file has a
 * single row group whose statistics cover the whole {@code id} range, which leaves coarse pruning unable to drop any
 * row. Neither {@code read_parquet} nor {@code registerArrowStream} needs a DuckDB extension or network access.
 */
class DuckDbCorpusRoundTripIT {

    private static final String CORPUS_RESOURCE = "parquet-testing/data/alltypes_plain.parquet";
    private static final int FILTER_THRESHOLD = 4;
    private static final long TOTAL_ROW_COUNT = 8L;

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath INT_COL = ColumnPath.of("int_col");
    private static final ColumnPath STRING_COL = ColumnPath.of("string_col");
    private static final Set<ColumnPath> PROJECTED = Set.of(ID, INT_COL, STRING_COL);

    @Test
    void parquetryFilteredArrowMatchesDuckDbDirectRead(@TempDir Path tempDir) throws Exception {
        Path parquetFile = TestCorpus.extractFile(CORPUS_RESOURCE, tempDir);

        try (FileChannel channel = FileChannel.open(parquetFile, StandardOpenOption.READ);
                DuckDBConnection conn = openDuckDb()) {
            List<Row> actual = readThroughArrowIntoDuckDb(channel, conn);
            List<Row> expected = queryDuckDbDirect(conn, parquetFile);

            assertThat(actual)
                    .as("parquetry's filtered Arrow output must match DuckDB's direct filtered read")
                    .isEqualTo(expected);
            assertThat(actual)
                    .as("filter must keep a strict, non-empty subset of the %d rows", TOTAL_ROW_COUNT)
                    .hasSizeGreaterThan(0)
                    .hasSizeLessThan((int) TOTAL_ROW_COUNT);
        }
    }

    private List<Row> readThroughArrowIntoDuckDb(FileChannel channel, DuckDBConnection conn) throws Exception {
        ParquetDataset dataset = ParquetDataset.open(channel);
        Predicate predicate = Pred.col(ID).gtEq(FILTER_THRESHOLD);
        Projection projection = Projection.of(PROJECTED);

        ParquetSchema projectedSchema = dataset.schema().project(PROJECTED);
        ParquetRecordBatch batch = filteredBatch(dataset, predicate, projection, projectedSchema);

        Pipe pipe = Pipe.open();
        ExecutorService producer = Executors.newSingleThreadExecutor();
        try {
            Future<?> writeTask = producer.submit(() -> {
                try (OutputStream sink = Channels.newOutputStream(pipe.sink())) {
                    ArrowIpcWriter.write(projectedSchema, Optional.empty(), Stream.of(batch), sink);
                }
                return null;
            });

            List<Row> consumed = consumeRegisteredArrowStream(pipe, conn);
            writeTask.get(30, TimeUnit.SECONDS);
            return consumed;
        } finally {
            producer.shutdownNow();
        }
    }

    /**
     * Reads the rows that satisfy {@code predicate} through parquetry's record-level filter pipeline and packs them
     * into one Arrow-ready batch. Reading the filtered records here is what guarantees the output is row-exact; the
     * batch path on its own only prunes whole row groups.
     */
    private ParquetRecordBatch filteredBatch(
            ParquetDataset dataset, Predicate predicate, Projection projection, ParquetSchema projectedSchema) {
        List<Row> rows = new ArrayList<>();
        try (Stream<ParquetRecord> records = dataset.read(predicate, projection, ReadOptions.DEFAULTS)) {
            records.forEach(record ->
                    rows.add(new Row(record.getInt(ID), record.getInt(INT_COL), record.getString(STRING_COL))));
        }
        return batchOf(projectedSchema, rows);
    }

    private ParquetRecordBatch batchOf(ParquetSchema projectedSchema, List<Row> rows) {
        int rowCount = rows.size();
        int[] ids = new int[rowCount];
        int[] intCols = new int[rowCount];
        MemorySegment[] strings = new MemorySegment[rowCount];
        BitSet allValid = new BitSet();
        allValid.set(0, rowCount);
        for (int i = 0; i < rowCount; i++) {
            Row row = rows.get(i);
            ids[i] = row.id();
            intCols[i] = row.intCol();
            strings[i] = MemorySegment.ofArray(row.stringCol().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ID, IntVector.materialized(ids, allValid));
        columns.put(INT_COL, IntVector.materialized(intCols, allValid));
        columns.put(STRING_COL, BinaryVector.materialized(strings, allValid));
        return new DefaultParquetRecordBatch(projectedSchema, columns, rowCount, Arena.ofShared());
    }

    private List<Row> consumeRegisteredArrowStream(Pipe pipe, DuckDBConnection conn) throws Exception {
        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(Channels.newInputStream(pipe.source()), allocator);
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
            Data.exportArrayStream(allocator, reader, stream);
            conn.registerArrowStream("parquetry_out", stream);
            return queryRows(
                    conn, "SELECT id, int_col, CAST(string_col AS VARCHAR) AS sc FROM parquetry_out ORDER BY id");
        }
    }

    private List<Row> queryDuckDbDirect(DuckDBConnection conn, Path parquetFile) throws Exception {
        String sql = "SELECT id, int_col, CAST(string_col AS VARCHAR) AS sc"
                + " FROM read_parquet('" + parquetFile.toAbsolutePath() + "')"
                + " WHERE id >= " + FILTER_THRESHOLD
                + " ORDER BY id";
        return queryRows(conn, sql);
    }

    private List<Row> queryRows(DuckDBConnection conn, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                rows.add(new Row(rs.getInt("id"), rs.getInt("int_col"), rs.getString("sc")));
            }
        }
        return rows;
    }

    private DuckDBConnection openDuckDb() throws Exception {
        return (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
    }

    private record Row(int id, int intCol, String stringCol) {}
}
