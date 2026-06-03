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
package io.tileverse.parquetry.cli.cmd;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.cli.support.CliRunner;
import io.tileverse.parquetry.cli.support.Fixtures;

/**
 * End-to-end checks that the {@code --filter} SQL predicate selects the right rows through {@code cat}, covering scalar
 * comparisons, list membership, spatial {@code ST_*} functions resolved against a GeoParquet file's geometry column,
 * and syntax-error reporting.
 */
class SqlFilterIT {

    @Test
    void scalarFilterSelectsMatchingRows(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("cities.parquet");
        Fixtures.writeCities(file);

        CliRunner.Result result = CliRunner.run("cat", file.toString(), "--filter", "pop > 1300000");

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("Cordoba", "Buenos Aires");
        assertThat(result.stdout()).doesNotContain("Rosario");
    }

    @Test
    void inFilterSelectsListedValues(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("cities.parquet");
        Fixtures.writeCities(file);

        CliRunner.Result result = CliRunner.run("cat", file.toString(), "--filter", "name IN ('Rosario', 'Cordoba')");

        assertThat(result.exitCode()).isZero();
        assertThat(result.stdout()).contains("Rosario", "Cordoba");
        assertThat(result.stdout()).doesNotContain("Buenos Aires");
    }

    @Test
    void spatialFilterSelectsRowsInEnvelope(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("geocities.parquet");
        Fixtures.writeGeoCities(file);

        CliRunner.Result result = CliRunner.run(
                "cat",
                file.toString(),
                "--columns",
                "id",
                "--filter",
                "ST_Intersects(geometry, ST_MakeEnvelope(-61, -33.5, -60, -32.5))");

        assertThat(result.exitCode()).isZero();
        String[] lines = result.stdout().strip().split("\n");
        assertThat(lines).hasSize(1);
        assertThat(lines[0]).contains("\"id\":1");
        assertThat(result.stdout()).doesNotContain("\"id\":2");
    }

    @Test
    void invalidFilterReportsError(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("cities.parquet");
        Fixtures.writeCities(file);

        CliRunner.Result result = CliRunner.run("cat", file.toString(), "--filter", "pop >");

        assertThat(result.exitCode()).isNotZero();
        assertThat(result.stderr()).contains("invalid filter syntax");
    }
}
