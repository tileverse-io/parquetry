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
package io.tileverse.parquetry.geotools;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.ParquetDatasetCatalog;
import io.tileverse.parquetry.io.LocalFileSource;

/**
 * Writes a small, deterministic nested GeoParquet file for the GeoTools tests and opens it as a catalog.
 *
 * <p>The parquetry writer is flat-only and cannot emit {@code LIST}/{@code MAP}/{@code STRUCT} columns, while several
 * GeoTools features (nested-type projection, value translation, schema mapping, feature reading, filter push-down) need
 * a nested fixture. DuckDB writes it: loading the spatial extension and a plain {@code COPY (SELECT ...) TO ... (FORMAT
 * PARQUET)} over a {@code GEOMETRY} column auto-writes the GeoParquet 1.0.0 {@code geo} key-value metadata (WKB
 * encoding, primary column = the geometry column), and nested columns get the standard physical
 * {@code list}/{@code element} and {@code key_value}/{@code key}/{@code value} levels.
 */
final class NestedFixtures {

    private NestedFixtures() {}

    /**
     * Writes the three-row nested GeoParquet sample to {@code target} (overwriting any existing file). The rows are
     * fixed and ordered by {@code id} so other tests can assert exact values against them:
     *
     * <ul>
     *   <li>id=1: geometry POINT(1 2); brand {name: 'Acme'}; addresses [{NYC,10001}, {LA,90001}]; tags {k1: v1}
     *   <li>id=2: geometry POINT(3 4); brand {name: 'Globex'}; addresses [] (empty list); tags {k2: v2}
     *   <li>id=3: geometry POINT(5 6); brand {name: 'Initech'}; addresses [{NYC,10001}, {NULL,00000}] (a present struct
     *       with a null {@code locality}, exercising the null-leaf-under-ALL semantics); tags {k3: v3}
     * </ul>
     *
     * Columns: {@code id} INTEGER, {@code geometry} GEOMETRY, {@code brand} STRUCT(name VARCHAR), {@code addresses}
     * LIST&lt;STRUCT(locality VARCHAR, postcode VARCHAR)&gt;, {@code tags} MAP(VARCHAR, VARCHAR).
     */
    static void writeSample(Path target) throws SQLException, IOException {
        Files.deleteIfExists(target);
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
                Statement stmt = conn.createStatement()) {
            stmt.execute("INSTALL spatial");
            stmt.execute("LOAD spatial");
            stmt.execute(copyStatement(target));
        }
    }

    /**
     * Opens {@code file} as a single-dataset catalog named {@code nested}, mirroring how {@code GeoParquetReadIT} opens
     * a GeoParquet file: a {@link LocalFileSource#file(Path) single-file source} passed to
     * {@link ParquetDatasetCatalog#open(io.tileverse.parquetry.io.FileSource, CatalogOptions)}.
     */
    static ParquetDatasetCatalog openCatalog(Path file) {
        return ParquetDatasetCatalog.open(
                LocalFileSource.file(file),
                CatalogOptions.builder().datasetName("nested").build());
    }

    private static String copyStatement(Path target) {
        return "COPY (SELECT * FROM (VALUES " + rowsLiteral() + ") t(id, geometry, brand, addresses, tags)) TO '"
                + sqlPath(target) + "' (FORMAT PARQUET)";
    }

    private static String rowsLiteral() {
        return row(1, "ST_Point(1, 2)", "{'name': 'Acme'}", acmeAddresses(), "MAP(['k1'], ['v1'])")
                + ", "
                + row(2, "ST_Point(3, 4)", "{'name': 'Globex'}", emptyAddresses(), "MAP(['k2'], ['v2'])")
                + ", "
                + row(3, "ST_Point(5, 6)", "{'name': 'Initech'}", initechAddresses(), "MAP(['k3'], ['v3'])");
    }

    private static String row(int id, String geometry, String brand, String addresses, String tags) {
        return "(" + id + ", " + geometry + ", " + brand + ", " + addresses + ", " + tags + ")";
    }

    private static String acmeAddresses() {
        return "[{'locality': 'NYC', 'postcode': '10001'}, {'locality': 'LA', 'postcode': '90001'}]";
    }

    private static String emptyAddresses() {
        return "CAST([] AS STRUCT(locality VARCHAR, postcode VARCHAR)[])";
    }

    private static String initechAddresses() {
        return "[{'locality': 'NYC', 'postcode': '10001'}, {'locality': NULL, 'postcode': '00000'}]";
    }

    private static String sqlPath(Path file) {
        return file.toAbsolutePath().toString().replace("'", "''");
    }
}
