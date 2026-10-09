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
package io.tileverse.parquetry.cli.cmd;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.cli.CliExitCode;
import io.tileverse.parquetry.cli.Par;
import io.tileverse.parquetry.cli.support.Fixtures;

import picocli.CommandLine;

class RowCountCmdTest {

    @Test
    void countsAllRows(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("row-count", file.toString());
        assertThat(code).isZero();
        assertThat(out.toString().strip()).isEqualTo("4");
    }

    @Test
    void countsFilteredRows(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("row-count", file.toString(), "--filter", "pop > 1000000");
        assertThat(code).isZero();
        assertThat(out.toString().strip()).isEqualTo("3");
    }

    @ParameterizedTest(name = "{0} counts {1}")
    @MethodSource("int32FiltersBeyondTheRangeOfTheColumn")
    void countsWithLiteralBeyondInt32(String filter, String expectedCount, @TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));

        int code = cmd.execute("row-count", file.toString(), "--filter", filter);

        assertThat(code).isZero();
        assertThat(out.toString().strip()).isEqualTo(expectedCount);
    }

    /** The {@code id} column is an INT32 holding 1 to 4; 4294967297 is 2^32 + 1. */
    private static Stream<Arguments> int32FiltersBeyondTheRangeOfTheColumn() {
        return Stream.of(
                Arguments.of("id = 4294967297", "0"),
                Arguments.of("id <> 4294967297", "4"),
                Arguments.of("id < 4294967297", "4"),
                Arguments.of("id <= 4294967297", "4"),
                Arguments.of("id > 4294967297", "0"),
                Arguments.of("id >= 4294967297", "0"),
                Arguments.of("id > -4294967297", "4"),
                Arguments.of("id < -4294967297", "0"),
                Arguments.of("id IN (4294967297, 2)", "1"),
                Arguments.of("id NOT IN (4294967297)", "4"),
                Arguments.of("id BETWEEN -4294967297 AND 4294967297", "4"),
                Arguments.of("id NOT BETWEEN 2 AND 4294967297", "1"));
    }

    @Test
    void countsADirectoryAsOneDataset(@TempDir Path dir) throws Exception {
        Fixtures.writeCities(dir.resolve("a.parquet"));
        Fixtures.writeCities(dir.resolve("b.parquet"));
        StringWriter single = new StringWriter();
        CommandLine one = Par.newCommandLine();
        one.setOut(new PrintWriter(single));
        one.execute("row-count", dir.resolve("a.parquet").toString());

        StringWriter merged = new StringWriter();
        CommandLine all = Par.newCommandLine();
        all.setOut(new PrintWriter(merged));
        int code = all.execute("row-count", dir.toString());

        assertThat(code).isZero();
        long perFile = Long.parseLong(single.toString().trim());
        long total = Long.parseLong(merged.toString().trim());
        assertThat(total).isEqualTo(perFile * 2);
    }

    @Test
    void badFilterExitsFive(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        int code = Par.newCommandLine().execute("row-count", file.toString(), "--filter", "pop >");
        assertThat(code).isEqualTo(CliExitCode.FILTER);
    }
}
