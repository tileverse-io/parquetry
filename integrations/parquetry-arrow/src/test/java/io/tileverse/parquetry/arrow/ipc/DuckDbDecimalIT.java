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
package io.tileverse.parquetry.arrow.ipc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.Pipe;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Corpus parity for DECIMAL export across all four Parquet carriers. Each corpus decimal file is read through
 * parquetry's batch path, exported as Arrow Decimal128, streamed into DuckDB through the Arrow C Data Interface, and
 * the values (compared as text to be exact) are cross-checked against DuckDB reading the same file directly.
 */
class DuckDbDecimalIT {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "parquet-testing/data/int32_decimal.parquet",
                "parquet-testing/data/int64_decimal.parquet",
                "parquet-testing/data/fixed_length_decimal.parquet",
                "parquet-testing/data/byte_array_decimal.parquet"
            })
    void decimalValuesMatchDuckDbDirectRead(String resource, @TempDir Path tempDir) throws Exception {
        Path parquetFile = TestCorpus.extractFile(resource, tempDir);
        try (FileChannel channel = FileChannel.open(parquetFile, StandardOpenOption.READ);
                DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
            String firstColumn = firstColumnName(channel);
            List<String> actual = exportThroughArrow(channel, conn, firstColumn);
            List<String> expected = queryStrings(conn, directReadSql(parquetFile, firstColumn));
            assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
            assertThat(actual).isNotEmpty();
        }
    }

    private String firstColumnName(FileChannel channel) throws Exception {
        ParquetFileReader reader = ParquetFileReader.open(ByteRangeSource.ofChannel(channel));
        return reader.schema().leafColumns().get(0).name();
    }

    private String directReadSql(Path parquetFile, String column) {
        return "SELECT CAST(\"" + column + "\" AS VARCHAR) AS v FROM read_parquet('" + parquetFile.toAbsolutePath()
                + "')";
    }

    private List<String> exportThroughArrow(FileChannel channel, DuckDBConnection conn, String column)
            throws Exception {
        ParquetFileReader reader = ParquetFileReader.open(ByteRangeSource.ofChannel(channel));
        ParquetSchema schema = reader.schema();
        List<ParquetRecordBatch> batches = reader.readBatches(
                        Predicate.ALWAYS_TRUE, new Projection.All(), ReadOptions.DEFAULTS)
                .toList();

        Pipe pipe = Pipe.open();
        ExecutorService producer = Executors.newSingleThreadExecutor();
        try {
            Future<?> writeTask = producer.submit(() -> {
                try (OutputStream sink = Channels.newOutputStream(pipe.sink())) {
                    ArrowIpcWriter.write(schema, Optional.empty(), batches.stream(), sink);
                }
                return null;
            });
            List<String> consumed = consume(pipe, conn, column);
            writeTask.get(30, TimeUnit.SECONDS);
            return consumed;
        } finally {
            producer.shutdownNow();
        }
    }

    private List<String> consume(Pipe pipe, DuckDBConnection conn, String column) throws Exception {
        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(Channels.newInputStream(pipe.source()), allocator);
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
            Data.exportArrayStream(allocator, reader, stream);
            conn.registerArrowStream("parquetry_out", stream);
            return queryStrings(conn, "SELECT CAST(\"" + column + "\" AS VARCHAR) AS v FROM parquetry_out");
        }
    }

    private List<String> queryStrings(DuckDBConnection conn, String sql) throws Exception {
        List<String> values = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                values.add(rs.getString(1));
            }
        }
        return values;
    }
}
