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
package io.tileverse.parquetry.probes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Optional;

import io.tileverse.parquetry.filter.Bbox;

/**
 * DuckDB's read arm over JDBC, in-process. Each read opens its own connection - the connection-pool model a real server
 * uses - runs the scan, and materializes every value of every {@code ResultSet} row with the typed JDBC getters
 * (falling back to {@code getObject} for nested and complex columns), the cost a row-oriented consumer (e.g. a GeoTools
 * datastore over DuckDB) actually pays. DuckDB's engine scan itself is fast (the profiler's latency); the wall is
 * dominated by this JDBC row materialization. That consumption churns the heap the same way the JVM engines do and
 * shows in the heap columns; DuckDB's own decode buffers stay off-heap and are reported separately from the profiler
 * ({@link DuckDbProfile#peakBufferBytes()}).
 */
final class DuckDbReadEngine implements ReadEngine {

    private final ReadContext context;
    private DuckDbProfile lastProfile;
    private long sink;

    DuckDbReadEngine(ReadContext context) {
        this.context = context;
    }

    @Override
    public String name() {
        return "duckdb";
    }

    @Override
    public long read(Scenario scenario) throws SQLException, IOException {
        boolean spatial = scenario == Scenario.SPATIAL
                || scenario == Scenario.ATTRIBUTE_AND_SPATIAL
                || (scenario == Scenario.BBOX && !context.bboxCoveringAvailable());
        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            if (spatial) {
                loadSpatial(connection);
            }
            Path profile = Files.createTempFile("parquetry-probe-duckdb-", ".json");
            try {
                enableProfiling(connection, profile);
                long rows = executeAndConsume(connection, query(scenario));
                lastProfile = DuckDbProfile.read(profile);
                return rows;
            } finally {
                Files.deleteIfExists(profile);
            }
        }
    }

    @Override
    public long checksum() {
        return sink;
    }

    @Override
    public Optional<DuckDbProfile> profile() {
        return Optional.ofNullable(lastProfile);
    }

    private void loadSpatial(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("INSTALL spatial");
            statement.execute("LOAD spatial");
        }
    }

    private void enableProfiling(Connection connection, Path profile) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA enable_profiling='json'");
            statement.execute("PRAGMA profiling_output='" + sqlLiteral(profile.toString()) + "'");
        }
    }

    private long executeAndConsume(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            int[] columnTypes = columnTypes(resultSet.getMetaData());
            long rows = 0L;
            while (resultSet.next()) {
                for (int column = 1; column <= columnTypes.length; column++) {
                    touchValue(resultSet, column, columnTypes[column - 1]);
                }
                rows++;
            }
            return rows;
        }
    }

    private static int[] columnTypes(ResultSetMetaData meta) throws SQLException {
        int count = meta.getColumnCount();
        int[] types = new int[count];
        for (int i = 0; i < count; i++) {
            types[i] = meta.getColumnType(i + 1);
        }
        return types;
    }

    /**
     * Reads one value with the typed JDBC getter for its SQL type, mirroring the typed leaf walks of the other engines
     * (primitives are not boxed). Nested and complex columns (struct, list, blob, decimal, temporal) have no scalar
     * getter and fall back to {@code getObject}, which materializes the Java object the way a JDBC consumer must.
     */
    private void touchValue(ResultSet resultSet, int column, int sqlType) throws SQLException {
        switch (sqlType) {
            case Types.BOOLEAN, Types.BIT -> {
                boolean value = resultSet.getBoolean(column);
                if (!resultSet.wasNull()) {
                    sink += value ? 1L : 0L;
                }
            }
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> {
                int value = resultSet.getInt(column);
                if (!resultSet.wasNull()) {
                    sink += value;
                }
            }
            case Types.BIGINT -> {
                long value = resultSet.getLong(column);
                if (!resultSet.wasNull()) {
                    sink += value;
                }
            }
            case Types.REAL -> {
                float value = resultSet.getFloat(column);
                if (!resultSet.wasNull()) {
                    sink += (long) value;
                }
            }
            case Types.FLOAT, Types.DOUBLE -> {
                double value = resultSet.getDouble(column);
                if (!resultSet.wasNull()) {
                    sink += (long) value;
                }
            }
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> {
                String value = resultSet.getString(column);
                if (value != null) {
                    sink += value.length();
                }
            }
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> {
                byte[] value = resultSet.getBytes(column);
                if (value != null) {
                    sink += value.length;
                }
            }
            default -> {
                Object value = resultSet.getObject(column);
                if (value != null) {
                    sink += value.hashCode();
                }
            }
        }
    }

    private String query(Scenario scenario) {
        String source =
                "read_parquet('" + sqlLiteral(context.file().toAbsolutePath().toString()) + "')";
        String where =
                switch (scenario) {
                    case NO_FILTER -> "";
                    case ATTRIBUTE -> " WHERE " + attributeSql();
                    case BBOX -> " WHERE " + bboxSql();
                    case SPATIAL -> " WHERE " + spatialSql();
                    case ATTRIBUTE_AND_SPATIAL -> " WHERE " + attributeSql() + " AND " + spatialSql();
                };
        return "SELECT " + selectList() + " FROM " + source + where;
    }

    /**
     * The output projection from {@code parquetry.probe.columns}, or {@code *} for every column. Only output columns
     * are listed; the filter columns named in the {@code WHERE} clause are read regardless and need no entry here.
     */
    private static String selectList() {
        java.util.List<String> columns = ProbeColumns.requested();
        return columns.isEmpty() ? "*" : String.join(", ", columns);
    }

    private String attributeSql() {
        return context.attributeColumnName() + " = '" + sqlLiteral(context.attributeValue()) + "'";
    }

    private String spatialSql() {
        return "ST_Intersects(" + context.geometryColumnName() + ", ST_GeomFromText('"
                + ProbeGeometry.wkt(context.queryDiamond()) + "'))";
    }

    /**
     * The bbox-only filter: the geometry's bounding box overlaps the query envelope. Reads the numeric covering columns
     * when the file has them (no geometry decoded), otherwise computes the box with DuckDB's {@code ST_} functions over
     * the geometry.
     */
    private String bboxSql() {
        Bbox q = context.queryEnvelope();
        if (context.bboxCoveringAvailable()) {
            String bbox = context.bboxColumn();
            return structField(bbox, "xmin") + " <= " + q.maxX()
                    + " AND " + structField(bbox, "xmax") + " >= " + q.minX()
                    + " AND " + structField(bbox, "ymin") + " <= " + q.maxY()
                    + " AND " + structField(bbox, "ymax") + " >= " + q.minY();
        }
        String geom = context.geometryColumnName();
        return "ST_XMin(" + geom + ") <= " + q.maxX()
                + " AND ST_XMax(" + geom + ") >= " + q.minX()
                + " AND ST_YMin(" + geom + ") <= " + q.maxY()
                + " AND ST_YMax(" + geom + ") >= " + q.minY();
    }

    private static String structField(String struct, String leaf) {
        return "struct_extract(" + struct + ", '" + leaf + "')";
    }

    private static String sqlLiteral(String raw) {
        return raw.replace("'", "''");
    }
}
