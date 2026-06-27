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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Blob;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Proves the GeoArrow geometry metadata ({@code ARROW:extension:name = geoarrow.wkb} plus the {@code crs}/{@code edges}
 * document) survives the Arrow IPC stream all the way to DuckDB. A batch with one WKB geometry column and a GeoParquet
 * {@code geo} block is written through {@link ArrowIpcWriter}, re-read with the canonical {@link ArrowStreamReader},
 * exported over the Arrow C Data Interface, and registered as a DuckDB virtual table.
 *
 * <p>The geometry column reaches DuckDB as a {@code BLOB}; its WKB bytes must equal the source for each row, which
 * needs no DuckDB extension. When the {@code spatial} extension loads, the test additionally confirms DuckDB interprets
 * the column as geometry; offline it aborts that part via a JUnit assumption.
 */
class DuckDbGeometryIT {

    private static final byte[] POINT_ONE = wkbPoint(1.0, 2.0);
    private static final byte[] POINT_TWO = wkbPoint(30.5, -45.25);

    @Test
    void geoArrowGeometrySurvivesToDuckDb() throws Exception {
        byte[] ipc = writeGeometryBatch();

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(ipc), allocator);
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
            Data.exportArrayStream(allocator, reader, stream);
            try (DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
                conn.registerArrowStream("geo_stream", stream);
                drainStreamIntoTable(conn);
                assertGeometryBytesSurvive(conn);
                assertSpatialReadsGeometryWhenAvailable(conn);
            }
        }
    }

    private void drainStreamIntoTable(DuckDBConnection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE geo_data AS SELECT * FROM geo_stream");
        }
    }

    private void assertGeometryBytesSurvive(DuckDBConnection conn) throws SQLException {
        String sql = "SELECT id, geom FROM geo_data ORDER BY id";
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("id")).isEqualTo(1L);
            assertThat(blobBytes(rs, "geom")).containsExactly(POINT_ONE);
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("id")).isEqualTo(2L);
            assertThat(blobBytes(rs, "geom")).containsExactly(POINT_TWO);
            assertThat(rs.next()).isFalse();
        }
    }

    private static byte[] blobBytes(ResultSet rs, String column) throws SQLException {
        Blob blob = rs.getBlob(column);
        return blob.getBytes(1, (int) blob.length());
    }

    private void assertSpatialReadsGeometryWhenAvailable(DuckDBConnection conn) throws SQLException {
        loadSpatialOrAbort(conn);
        String sql = "SELECT ST_AsText(geom) AS wkt FROM geo_data ORDER BY id";
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("wkt")).isNotNull();
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("wkt")).isNotNull();
            assertThat(rs.next()).isFalse();
        }
    }

    private void loadSpatialOrAbort(DuckDBConnection conn) {
        try (Statement install = conn.createStatement()) {
            install.execute("INSTALL spatial");
            install.execute("LOAD spatial");
        } catch (SQLException e) {
            Assumptions.abort("DuckDB 'spatial' extension unavailable (offline?): " + e.getMessage());
        }
    }

    private byte[] writeGeometryBatch() {
        ParquetSchema schema = geometrySchema();
        ParquetRecordBatch batch = geometryBatch(schema);
        Optional<GeoParquetMetadata> geo = Optional.of(GeoParquetMetadata.parse(GEO_JSON));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowIpcWriter.write(schema, geo, Stream.of(batch), out);
        return out.toByteArray();
    }

    private static final String GEO_JSON = """
            {"version":"1.1.0","primary_column":"geom",\
            "columns":{"geom":{"encoding":"WKB","geometry_types":["Point"]}}}""";

    private static ParquetSchema geometrySchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), 0);
        SchemaNode.Primitive geom = new SchemaNode.Primitive(
                "geom", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), 1);
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(id, geom), Optional.empty(), -1));
    }

    private static ParquetRecordBatch geometryBatch(ParquetSchema schema) {
        BitSet validBits = new BitSet();
        validBits.set(0, 2);
        Validity validity = Validity.of(validBits, 2);
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("id"), LongVector.materialized(new long[] {1L, 2L}, validity));
        MemorySegment[] geometries = {MemorySegment.ofArray(POINT_ONE), MemorySegment.ofArray(POINT_TWO)};
        columns.put(ColumnPath.of("geom"), BinaryVector.materialized(geometries, validity));
        return new DefaultParquetRecordBatch(schema, columns, 2, Arena.ofShared());
    }

    private static byte[] wkbPoint(double x, double y) {
        ByteBuffer buffer = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(1);
        buffer.putDouble(x);
        buffer.putDouble(y);
        return buffer.array();
    }
}
