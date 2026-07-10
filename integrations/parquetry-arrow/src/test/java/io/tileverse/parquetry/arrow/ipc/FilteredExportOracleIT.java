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
package io.tileverse.parquetry.arrow.ipc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.arrow.cdi.ArrowCDataExporter;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ResolvedColumn;

/**
 * The filtered-export oracle: a nested {@code LIST<STRUCT>} file written by DuckDB across several row groups, a
 * quantified predicate over the nested column, and the assertion that both export boundaries (Arrow IPC read back with
 * arrow-java; the C Data stream registered into DuckDB) emit exactly the rows the row path {@code read(predicate)}
 * produces, in read order. This is the shape that previously threw at the export boundary (a filtered batch's bulk
 * accessors reject a selection) or fell back to a flat-only record repack in the CLI.
 */
class FilteredExportOracleIT {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath LOGICAL_LOCALITY = ColumnPath.of("addresses", "locality");
    private static final Value BERLIN = new Value.StringVal("Berlin");

    @Test
    void filteredIpcStreamEqualsTheRowPathOracle(@TempDir Path tempDir) throws Exception {
        Path file = tempDir.resolve("addresses.parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            writeNestedFixture(conn, file);
        }

        List<Long> oracle;
        byte[] ipc;
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            Predicate predicate = anyLocalityIsBerlin(reader);
            oracle = rowPathIds(reader, predicate);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (Stream<ParquetRecordBatch> batches =
                    reader.readBatches(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                ArrowIpcWriter.write(reader.schema(), Optional.empty(), batches, out);
            }
            ipc = out.toByteArray();
        }

        assertThat(ipcIds(ipc)).containsExactlyElementsOf(oracle);
    }

    @Test
    void filteredCDataStreamFeedsDuckDbTheOracleRows(@TempDir Path tempDir) throws Exception {
        Path file = tempDir.resolve("addresses.parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            writeNestedFixture(conn, file);
        }

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            Predicate predicate = anyLocalityIsBerlin(reader);
            Set<Long> oracle = new TreeSet<>(rowPathIds(reader, predicate));

            try (RootAllocator allocator = new RootAllocator();
                    ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator);
                    DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
                ArrowCDataExporter.export(
                        reader.schema(),
                        Optional.empty(),
                        reader.readBatches(predicate, Projection.ALL, ReadOptions.DEFAULTS),
                        stream.memoryAddress());
                conn.registerArrowStream("filtered_rows", stream);
                Set<Long> consumed = new TreeSet<>();
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT id FROM filtered_rows")) {
                    while (rs.next()) {
                        consumed.add(rs.getLong("id"));
                    }
                }
                assertThat(consumed).isEqualTo(oracle);
            }
        }
        assertThat(ArrowCDataExporter.outstandingExports()).isZero();
    }

    @Test
    void filterMatchingNothingEmitsSchemaAndEndOfStreamOnly(@TempDir Path tempDir) throws Exception {
        Path file = tempDir.resolve("addresses.parquet");
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            writeNestedFixture(conn, file);
        }

        byte[] ipc;
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            Predicate none = new Predicate.Quantified(
                    MatchAction.ANY, new Predicate.Eq(localityLeaf(reader), new Value.StringVal("Nowhere")));

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (Stream<ParquetRecordBatch> batches = reader.readBatches(none, Projection.ALL, ReadOptions.DEFAULTS)) {
                ArrowIpcWriter.write(reader.schema(), Optional.empty(), batches, out);
            }
            ipc = out.toByteArray();
        }

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            assertThat(reader.loadNextBatch()).isFalse();
        }
    }

    /**
     * Six rows, two per DuckDB row group ({@code ROW_GROUP_SIZE 2}), Berlin present in rows 1, 3, and 6 across
     * different groups: the filtered read spans several batches and the export must preserve read order.
     */
    private void writeNestedFixture(Connection conn, Path file) throws SQLException {
        String rows = "(1, [{'locality':'Berlin'}, {'locality':'Bonn'}]), "
                + "(2, [{'locality':'Cologne'}]), "
                + "(3, [{'locality':'Berlin'}]), "
                + "(4, CAST([] AS STRUCT(locality VARCHAR)[])), "
                + "(5, [{'locality':'Hamburg'}]), "
                + "(6, [{'locality':'Munich'}, {'locality':'Berlin'}])";
        String copy = "COPY ("
                + "SELECT CAST(id AS BIGINT) AS id, CAST(addresses AS STRUCT(locality VARCHAR)[]) AS addresses "
                + "FROM (VALUES " + rows + ") t(id, addresses)"
                + ") TO '" + sqlPath(file) + "' (FORMAT PARQUET, ROW_GROUP_SIZE 2)";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(copy);
        }
    }

    private Predicate anyLocalityIsBerlin(ParquetFileReader reader) {
        return new Predicate.Quantified(MatchAction.ANY, new Predicate.Eq(localityLeaf(reader), BERLIN));
    }

    private ColumnPath localityLeaf(ParquetFileReader reader) {
        ResolvedColumn resolved = reader.schema().resolve(LOGICAL_LOCALITY).orElseThrow();
        return resolved.physical();
    }

    private List<Long> rowPathIds(ParquetFileReader reader, Predicate predicate) {
        try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.map(rec -> rec.getLong(ID)).toList();
        }
    }

    private List<Long> ipcIds(byte[] ipc) throws Exception {
        List<Long> ids = new ArrayList<>();
        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator)) {
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                assertThat(root.getRowCount()).isGreaterThan(0);
                BigIntVector idVector = (BigIntVector) root.getVector("id");
                for (int row = 0; row < root.getRowCount(); row++) {
                    ids.add(idVector.get(row));
                }
            }
        }
        return ids;
    }

    private static String sqlPath(Path file) {
        return file.toAbsolutePath().toString().replace('\\', '/');
    }
}
