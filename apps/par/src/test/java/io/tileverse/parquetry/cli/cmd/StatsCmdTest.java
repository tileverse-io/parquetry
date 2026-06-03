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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.cli.Par;
import io.tileverse.parquetry.cli.support.Fixtures;

import picocli.CommandLine;

class StatsCmdTest {

    @Test
    void printsStatsAsText(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("stats", file.toString());
        assertThat(code).isZero();
        String output = out.toString();
        // Header columns must be present
        assertThat(output).contains("min");
        // All four leaf columns must appear
        assertThat(output).contains("id");
        assertThat(output).contains("name");
        assertThat(output).contains("pop");
        assertThat(output).contains("capital");
        // The INT32 type token for the id column must appear
        assertThat(output).contains("INT32");
        // The writer emits min/max statistics; verify a concrete max value for pop
        assertThat(output).contains("3100000");
    }

    @Test
    void printsStatsAsJson(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("stats", file.toString(), "-o", "json");
        assertThat(code).isZero();
        String output = out.toString();
        assertThat(output).startsWith("[");
        assertThat(output).contains("\"column\"");
        assertThat(output).contains("\"type\"");
    }

    @Test
    void rejectsUnsupportedFormat(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        CommandLine cmd = Par.newCommandLine();
        int code = cmd.execute("stats", file.toString(), "-o", "csv");
        assertThat(code).isEqualTo(2);
    }
}
