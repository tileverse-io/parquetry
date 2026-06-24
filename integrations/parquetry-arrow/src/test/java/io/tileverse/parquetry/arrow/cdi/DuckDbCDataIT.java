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
package io.tileverse.parquetry.arrow.cdi;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.memory.RootAllocator;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Registers a parquetry-produced {@code ArrowArrayStream} directly with DuckDB through {@code registerArrowStream} and
 * queries it, which proves DuckDB pulls batches from the hand-rolled C Data exporter without an intermediate Arrow
 * file.
 */
class DuckDbCDataIT {

    @Test
    void duckDbScansAFlatStream() throws Exception {
        ParquetSchema schema = flatSchema();
        Stream<ParquetRecordBatch> batches = Stream.of(flatBatch(schema, 1L, 2L), flatBatch(schema, 3L, 4L));

        try (RootAllocator allocator = new RootAllocator();
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
            ArrowCDataExporter.export(schema, Optional.empty(), batches, stream.memoryAddress());
            try (DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
                conn.registerArrowStream("arrow_data", stream);
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT count(*) AS n, sum(id) AS s FROM arrow_data")) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getLong("n")).isEqualTo(4L);
                    assertThat(rs.getLong("s")).isEqualTo(10L);
                }
            }
        }
    }

    @Test
    void duckDbScansAStructStream() throws Exception {
        ParquetSchema schema = structSchema();
        Stream<ParquetRecordBatch> batches = Stream.of(structBatch(schema, 30, 40));

        try (RootAllocator allocator = new RootAllocator();
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
            ArrowCDataExporter.export(schema, Optional.empty(), batches, stream.memoryAddress());
            try (DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
                conn.registerArrowStream("arrow_data", stream);
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(
                                "SELECT sum(struct_extract(person, 'age')) AS total FROM arrow_data")) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getLong("total")).isEqualTo(70L);
                }
            }
        }
    }

    private static ParquetSchema flatSchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), 0);
        return new ParquetSchema(new SchemaNode.Group("root", Repetition.REQUIRED, List.of(id), Optional.empty(), -1));
    }

    private static ParquetRecordBatch flatBatch(ParquetSchema schema, long id0, long id1) {
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("id"), LongVector.materialized(new long[] {id0, id1}, Validity.allValid(2)));
        return new DefaultParquetRecordBatch(schema, columns, 2, Arena.ofShared());
    }

    private static ParquetSchema structSchema() {
        SchemaNode.Primitive age = new SchemaNode.Primitive(
                "age", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 0);
        SchemaNode.Group person =
                new SchemaNode.Group("person", Repetition.OPTIONAL, List.of(age), Optional.empty(), 1);
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(person), Optional.empty(), -1));
    }

    private static ParquetRecordBatch structBatch(ParquetSchema schema, int age0, int age1) {
        Map<ColumnPath, ColumnVector> fields = new LinkedHashMap<>();
        fields.put(ColumnPath.of("age"), IntVector.materialized(new int[] {age0, age1}, Validity.allValid(2)));
        StructVector person = new StructVector(fields, Validity.allValid(2), 2);
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("person"), person);
        return new DefaultParquetRecordBatch(schema, columns, 2, Arena.ofShared());
    }
}
