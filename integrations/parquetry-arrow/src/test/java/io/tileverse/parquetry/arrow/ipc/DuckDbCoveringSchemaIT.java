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

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Third-party interop proof that a plain parquetry geo write produces a bbox covering another Parquet reader
 * understands. A geometry-only file is written with parquetry's defaults (a WGS84 geometry, no explicit covering
 * request), then DuckDB's native {@code parquet_schema} table function reports its physical schema. DuckDB must see a
 * {@code bbox} group of four children whose {@code xmin}/{@code xmax}/{@code ymin}/{@code ymax} leaves are FLOAT.
 *
 * <p>This locks the on-disk shape against the ecosystem: GDAL and DuckDB emit the GeoParquet 1.1 covering as float32,
 * and a parquetry default write matches that convention. Reading a Parquet schema needs no DuckDB extension or network
 * access.
 */
class DuckDbCoveringSchemaIT {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");
    private static final int ROW_COUNT = 16;
    private static final List<String> COVERING_LEAVES = List.of("xmin", "xmax", "ymin", "ymax");

    @Test
    void defaultGeoWriteExposesAFloatBboxStructToDuckDb(@TempDir Path tempDir) throws Exception {
        Path file = writeGeometryOnlyFileWithDefaults(tempDir);

        Map<String, ParquetColumn> schema = readParquetSchema(file);

        assertThat(schema)
                .as("DuckDB must see the derived bbox covering group in the physical schema")
                .containsKey("bbox");
        assertThat(schema.get("bbox").numChildren())
                .as("the bbox covering group has one leaf per box edge")
                .isEqualTo(4);
        for (String leaf : COVERING_LEAVES) {
            assertThat(schema).as("DuckDB must see the %s covering leaf", leaf).containsKey(leaf);
            assertThat(schema.get(leaf).type())
                    .as("a default geo write over a WGS84 geometry emits a FLOAT covering leaf %s", leaf)
                    .isEqualTo("FLOAT");
        }
    }

    private Path writeGeometryOnlyFileWithDefaults(Path tempDir) throws Exception {
        ParquetSchema schema = geometryOnlySchema();
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .crsEpsg("geometry", 4326)
                .build();
        Path file = tempDir.resolve("default-geo-covering.parquet");
        try (ParquetRecordBatch batch = marchingPointsBatch(schema);
                ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(batch);
        }
        return file;
    }

    private static Map<String, ParquetColumn> readParquetSchema(Path file) throws Exception {
        String sql = "SELECT name, type, num_children FROM parquet_schema(?)";
        Map<String, ParquetColumn> columns = new LinkedHashMap<>();
        try (DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
                PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, file.toAbsolutePath().toString());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String name = rs.getString("name");
                    String type = rs.getString("type");
                    int numChildren = rs.getInt("num_children");
                    columns.put(name, new ParquetColumn(type, numChildren));
                }
            }
        }
        return columns;
    }

    private static ParquetRecordBatch marchingPointsBatch(ParquetSchema schema) {
        BitSet allValidBits = new BitSet();
        allValidBits.set(0, ROW_COUNT);
        Validity allValid = Validity.of(allValidBits, ROW_COUNT);
        MemorySegment[] geometries = new MemorySegment[ROW_COUNT];
        for (int id = 0; id < ROW_COUNT; id++) {
            geometries[id] = MemorySegment.ofArray(wkbPoint(id, 5.0));
        }
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(GEOMETRY, BinaryVector.materialized(geometries, allValid));
        return DefaultParquetRecordBatch.ofHeap(schema, columns, ROW_COUNT);
    }

    private static ParquetSchema geometryOnlySchema() {
        SchemaNode.Primitive geometry = new SchemaNode.Primitive(
                "geometry", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), 0);
        return new ParquetSchema(
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(geometry), Optional.empty(), -1));
    }

    private static byte[] wkbPoint(double x, double y) {
        ByteBuffer buffer = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(1);
        buffer.putDouble(x);
        buffer.putDouble(y);
        return buffer.array();
    }

    private record ParquetColumn(String type, int numChildren) {}
}
