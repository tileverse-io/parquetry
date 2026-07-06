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
package io.tileverse.parquetry.cli.cmd;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.cli.CliExitCode;
import io.tileverse.parquetry.cli.Par;
import io.tileverse.parquetry.cli.support.Fixtures;

import picocli.CommandLine;

class HeadCmdTest {

    /**
     * The fixture has 4 rows, which is fewer than the default cap of 10. All 4 rows must be emitted, proving that the
     * default cap does not drop rows when the file is smaller than the cap.
     */
    @Test
    void defaultsToTenRows(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("head", file.toString());
        assertThat(code).isZero();
        assertThat(out.toString().strip().split("\n")).hasSize(4);
    }

    /** An explicit --limit must override the default cap of 10, emitting exactly the requested number of rows. */
    @Test
    void limitOverridesDefault(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("head", file.toString(), "--limit", "2");
        assertThat(code).isZero();
        assertThat(out.toString().strip().split("\n")).hasSize(2);
    }

    /**
     * head inherits CatCmd's rejection of the json format option, which is not supported as a streaming output format.
     */
    @Test
    void rejectsJsonFormat(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        int code = Par.newCommandLine().execute("head", file.toString(), "-o", "json");
        assertThat(code).isEqualTo(CliExitCode.USAGE);
    }
}
