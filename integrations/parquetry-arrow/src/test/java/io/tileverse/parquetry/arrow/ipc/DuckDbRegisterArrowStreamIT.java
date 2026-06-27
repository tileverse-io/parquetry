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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Proves DuckDB consumes our Arrow IPC streaming output in-memory through the Arrow C Data Interface. The IPC bytes are
 * re-read with the canonical {@link ArrowStreamReader}, exported as an {@link ArrowArrayStream} over the C Data
 * Interface, and registered as a DuckDB virtual table. This path needs neither network access nor a DuckDB extension.
 */
class DuckDbRegisterArrowStreamIT {

    @Test
    void duckDbConsumesRegisteredArrowStream() throws Exception {
        byte[] ipc = writeKnownBatch();

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator);
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
            Data.exportArrayStream(allocator, reader, stream);
            try (DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
                conn.registerArrowStream("arrow_data", stream);
                assertCountAndSum(conn);
            }
        }
    }

    private void assertCountAndSum(DuckDBConnection conn) throws Exception {
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT count(*) AS n, sum(id) AS s FROM arrow_data")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("n")).isEqualTo(3L);
            assertThat(rs.getLong("s")).isEqualTo(6L);
        }
    }

    private byte[] writeKnownBatch() {
        ParquetSchema schema = idAndNameSchema();
        ParquetRecordBatch batch = idAndNameBatch(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(batch), out);
        return out.toByteArray();
    }

    private static ParquetSchema idAndNameSchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), 0);
        SchemaNode.Primitive name = new SchemaNode.Primitive(
                "name",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                1);
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(id, name), Optional.empty(), -1));
    }

    private static ParquetRecordBatch idAndNameBatch(ParquetSchema schema) {
        BitSet validBits = new BitSet();
        validBits.set(0, 3);
        Validity validity = Validity.of(validBits, 3);
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("id"), LongVector.materialized(new long[] {1L, 2L, 3L}, validity));
        columns.put(ColumnPath.of("name"), BinaryVector.materialized(utf8Segments("alpha", "beta", "gamma"), validity));
        return new DefaultParquetRecordBatch(schema, columns, 3, Arena.ofShared());
    }

    private static MemorySegment[] utf8Segments(String... values) {
        MemorySegment[] result = new MemorySegment[values.length];
        for (int row = 0; row < values.length; row++) {
            result[row] = MemorySegment.ofArray(values[row].getBytes(StandardCharsets.UTF_8));
        }
        return result;
    }
}
