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
package io.tileverse.parquetry.arrow.cdi;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.arrow.c.ArrowArrayStream;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Feeds DuckDB a stream large enough to trigger its parallel scan, which exercises the serialized {@code get_next} pull
 * alongside concurrent {@code release} callbacks from DuckDB's worker threads. The aggregate must still be exact.
 */
class CDataConcurrencyIT {

    private static final int BATCHES = 64;
    private static final int ROWS_PER_BATCH = 2048;
    private static final long TOTAL_ROWS = (long) BATCHES * ROWS_PER_BATCH;

    @Test
    void parallelScanProducesTheExactAggregate() throws Exception {
        ParquetSchema schema = idSchema();
        Stream<ParquetRecordBatch> batches = IntStream.range(0, BATCHES).mapToObj(batch -> idBatch(schema, batch));

        try (Arena arena = Arena.ofShared()) {
            MemorySegment streamSegment = arena.allocate(CDataLayouts.ARROW_ARRAY_STREAM);
            ArrowCDataExporter.export(schema, Optional.empty(), batches, SegmentPool.getDefault(), streamSegment);
            ArrowArrayStream stream = ArrowArrayStream.wrap(streamSegment.address());

            try (DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
                conn.registerArrowStream("arrow_data", stream);
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT count(*) AS n, sum(id) AS s FROM arrow_data")) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getLong("n")).isEqualTo(TOTAL_ROWS);
                    assertThat(rs.getLong("s")).isEqualTo(TOTAL_ROWS * (TOTAL_ROWS - 1) / 2);
                }
            }
        }
    }

    private static ParquetRecordBatch idBatch(ParquetSchema schema, int batch) {
        long[] ids = new long[ROWS_PER_BATCH];
        long base = (long) batch * ROWS_PER_BATCH;
        for (int row = 0; row < ROWS_PER_BATCH; row++) {
            ids[row] = base + row;
        }
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("id"), LongVector.materialized(ids, Validity.allValid(ROWS_PER_BATCH)));
        return new DefaultParquetRecordBatch(schema, columns, ROWS_PER_BATCH, Arena.ofShared());
    }

    private static ParquetSchema idSchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), 0);
        return new ParquetSchema(new SchemaNode.Group("root", Repetition.REQUIRED, List.of(id), Optional.empty(), -1));
    }
}
