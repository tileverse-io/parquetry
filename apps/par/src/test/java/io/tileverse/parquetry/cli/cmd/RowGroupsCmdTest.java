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

class RowGroupsCmdTest {

    @Test
    void listsRowGroupsAsText(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));

        int code = cmd.execute("row-groups", file.toString());

        assertThat(code).isZero();
        String output = out.toString();
        // Header row must mention both column labels
        assertThat(output).contains("index");
        assertThat(output).contains("rows");
        // Cities fixture has 4 rows in a single row group at index 0
        assertThat(output).contains("4");
        assertThat(output).contains("0");
    }

    @Test
    void listsRowGroupsAsJson(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));

        int code = cmd.execute("row-groups", file.toString(), "-o", "json");

        assertThat(code).isZero();
        String output = out.toString().strip();
        // JSON output is an array
        assertThat(output).startsWith("[");
        assertThat(output).contains("\"rows\"");
    }

    @Test
    void rejectsUnsupportedFormat(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        CommandLine cmd = Par.newCommandLine();

        int code = cmd.execute("row-groups", file.toString(), "-o", "csv");

        // picocli returns exit code 2 for parameter exceptions
        assertThat(code).isEqualTo(2);
    }
}
