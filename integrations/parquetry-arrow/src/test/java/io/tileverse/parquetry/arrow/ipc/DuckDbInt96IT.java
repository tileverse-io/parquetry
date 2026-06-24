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
import java.util.stream.Stream;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Corpus parity for INT96 timestamp export. {@code int96_from_spark.parquet} is read through parquetry's batch path,
 * exported as Arrow {@code Timestamp(MICROSECOND)}, streamed into DuckDB through the Arrow C Data Interface, and the
 * microseconds-since-epoch are cross-checked against DuckDB reading the same file's INT96 column directly.
 *
 * <p>The fixture (from Apache Spark) holds one far-future value, {@code 290000-12-31T01:00:00+02:00}, that is not
 * representable as a 64-bit nanosecond timestamp. DuckDB's direct INT96 read goes through a nanosecond representation
 * and overflows on that row, returning a wrapped negative microsecond value. parquetry converts straight to
 * microseconds and represents it exactly. The cross-check therefore compares only the rows DuckDB reads without
 * overflow; the far-future value is asserted directly against its independently computed microseconds.
 */
class DuckDbInt96IT {

    private static final String CORPUS_RESOURCE = "parquet-testing/data/int96_from_spark.parquet";

    // 290000-12-31T01:00:00+02:00 in microseconds since epoch; overflows a 64-bit nanosecond timestamp.
    private static final long FAR_FUTURE_MICROS = 9_089_380_393_200_000_000L;

    @Test
    void int96TimestampsMatchDuckDbDirectRead(@TempDir Path tempDir) throws Exception {
        Path parquetFile = TestCorpus.extractFile(CORPUS_RESOURCE, tempDir);
        try (FileChannel channel = FileChannel.open(parquetFile, StandardOpenOption.READ);
                DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
            List<Long> exported = exportThroughArrow(channel, conn);
            assertThat(exported).hasSize(6).contains((Long) null).contains(FAR_FUTURE_MICROS);

            List<Long> duckDbDirect = queryRows(
                    conn, "SELECT epoch_us(a) AS us FROM read_parquet('" + parquetFile.toAbsolutePath() + "')");
            assertThat(withoutFarFuture(exported)).containsExactlyInAnyOrderElementsOf(withoutOverflow(duckDbDirect));
        }
    }

    /** Drops the far-future row parquetry exports exactly, leaving the rows DuckDB also reads without overflow. */
    private static List<Long> withoutFarFuture(List<Long> exported) {
        List<Long> comparable = new ArrayList<>();
        for (Long micros : exported) {
            if (micros == null || micros != FAR_FUTURE_MICROS) {
                comparable.add(micros);
            }
        }
        return comparable;
    }

    /** Drops the negative microsecond DuckDB returns when its nanosecond INT96 read overflows on the far-future row. */
    private static List<Long> withoutOverflow(List<Long> duckDbDirect) {
        List<Long> comparable = new ArrayList<>();
        for (Long micros : duckDbDirect) {
            if (micros == null || micros >= 0) {
                comparable.add(micros);
            }
        }
        return comparable;
    }

    private List<Long> exportThroughArrow(FileChannel channel, DuckDBConnection conn) throws Exception {
        ParquetReader reader = ParquetReader.open(ByteRangeSource.ofChannel(channel));
        ParquetSchema schema = reader.schema();
        ParquetRecordBatch firstBatch = reader.readBatches(
                        Predicate.ALWAYS_TRUE, new Projection.All(), ReadOptions.DEFAULTS)
                .findFirst()
                .orElseThrow();

        Pipe pipe = Pipe.open();
        ExecutorService producer = Executors.newSingleThreadExecutor();
        try {
            Future<?> writeTask = producer.submit(() -> {
                try (OutputStream sink = Channels.newOutputStream(pipe.sink())) {
                    ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(firstBatch), sink);
                }
                return null;
            });
            List<Long> consumed = consume(pipe, conn);
            writeTask.get(30, TimeUnit.SECONDS);
            return consumed;
        } finally {
            producer.shutdownNow();
        }
    }

    private List<Long> consume(Pipe pipe, DuckDBConnection conn) throws Exception {
        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(Channels.newInputStream(pipe.source()), allocator);
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
            Data.exportArrayStream(allocator, reader, stream);
            conn.registerArrowStream("parquetry_out", stream);
            return queryRows(conn, "SELECT epoch_us(a) AS us FROM parquetry_out");
        }
    }

    private List<Long> queryRows(DuckDBConnection conn, String sql) throws Exception {
        List<Long> values = new ArrayList<>();
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                long us = rs.getLong("us");
                values.add(rs.wasNull() ? null : us);
            }
        }
        return values;
    }
}
