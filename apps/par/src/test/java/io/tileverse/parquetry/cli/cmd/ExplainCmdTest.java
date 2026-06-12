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

class ExplainCmdTest {

    @Test
    void explainsWithFilter(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("explain", file.toString(), "--filter", "pop > 1000000");
        assertThat(code).isZero();
        // The ASCII table always includes these column headers from AsciiTableRenderer
        assertThat(out.toString()).contains("RG").contains("Outcome");
    }

    @Test
    void explainAsJson(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("explain", file.toString(), "-o", "json");
        assertThat(code).isZero();
        // JsonRenderer always opens with {"originalPredicate":...
        assertThat(out.toString()).contains("{").contains("originalPredicate");
    }

    @Test
    void rejectsUnsupportedFormat(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        cmd.setErr(new PrintWriter(err));
        // picocli maps ParameterException to exit code 2
        int code = cmd.execute("explain", file.toString(), "-o", "csv");
        assertThat(code).isEqualTo(2);
    }

    @Test
    void analyzeAnnotatesActualCounts(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("explain", file.toString(), "--filter", "pop > 1000000", "--analyze");
        assertThat(code).isZero();
        // The execution annotation adds an "actual" column and a totals line.
        assertThat(out.toString()).contains("actual");
    }

    @Test
    void unfilteredAnalyzeAnnotatesActualCounts(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        // A full scan (no --filter) decodes the projected columns and reports their real read cost.
        int code = cmd.execute("explain", file.toString(), "--analyze");
        assertThat(code).isZero();
        assertThat(out.toString()).contains("actual");
    }

    @Test
    void analyzeAsJsonAddsExecution(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("explain", file.toString(), "--filter", "pop > 1000000", "--analyze", "-o", "json");
        assertThat(code).isZero();
        assertThat(out.toString()).contains("execution");
    }

    @Test
    void analyzeIncludesTiming(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("explain", file.toString(), "--filter", "pop > 1000000", "--analyze");
        assertThat(code).isZero();
        // --analyze always measures per-phase timing, which adds the "time" column.
        assertThat(out.toString()).contains("actual").contains("time");
    }
}
