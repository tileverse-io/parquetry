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
package io.tileverse.parquetry.arrow.ipc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.OutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
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

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
 * Proves DuckDB reads our Arrow IPC streaming output through its {@code read_arrow} table function. The IPC stream is
 * written to a temporary {@code .arrows} file via a {@link OutputStream} (not an in-memory buffer) so the check also
 * exercises the file-backed path that large streams take.
 *
 * <p>{@code read_arrow} ships in DuckDB's {@code arrow} community extension, which DuckDB downloads on demand. When the
 * download is unavailable (offline build), the test aborts via a JUnit assumption rather than failing.
 */
class DuckDbReadArrowFileIT {

    @Test
    void duckDbReadsArrowFile(@TempDir Path tempDir) throws Exception {
        Path arrowFile = tempDir.resolve("interop.arrows");
        writeKnownBatch(arrowFile);

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            loadArrowExtensionOrAbort(conn);
            assertCountAndSum(conn, arrowFile);
            assertNamesInIdOrder(conn, arrowFile);
        }
    }

    private void writeKnownBatch(Path arrowFile) throws Exception {
        ParquetSchema schema = idAndNameSchema();
        ParquetRecordBatch batch = idAndNameBatch(schema);
        try (OutputStream out = Files.newOutputStream(arrowFile)) {
            ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(batch), out);
        }
    }

    private void loadArrowExtensionOrAbort(Connection conn) {
        try (Statement install = conn.createStatement()) {
            install.execute("INSTALL arrow FROM community");
            install.execute("LOAD arrow");
        } catch (SQLException e) {
            Assumptions.abort("DuckDB 'arrow' community extension unavailable (offline?): " + e.getMessage());
        }
    }

    private void assertCountAndSum(Connection conn, Path arrowFile) throws SQLException {
        String sql = "SELECT count(*) AS n, sum(id) AS s FROM read_arrow('" + sqlPath(arrowFile) + "')";
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getLong("n")).isEqualTo(3L);
            assertThat(rs.getLong("s")).isEqualTo(6L);
        }
    }

    private void assertNamesInIdOrder(Connection conn, Path arrowFile) throws SQLException {
        String sql = "SELECT name FROM read_arrow('" + sqlPath(arrowFile) + "') ORDER BY id";
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("name")).isEqualTo("alpha");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("name")).isEqualTo("beta");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("name")).isEqualTo("gamma");
            assertThat(rs.next()).isFalse();
        }
    }

    private static String sqlPath(Path arrowFile) {
        return arrowFile.toAbsolutePath().toString().replace("'", "''");
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
