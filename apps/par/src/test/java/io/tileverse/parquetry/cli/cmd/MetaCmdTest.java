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

class MetaCmdTest {

    @Test
    void printsRowCountAndRowGroups(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("meta", file.toString());
        assertThat(code).isZero();
        assertThat(out.toString()).contains("rows", "4").contains("row groups", "1");
        assertThat(out.toString()).contains("parquetry");
    }

    @Test
    void textListsKeyValueKeysAndPlaceholderWhenNone(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("meta", file.toString());
        assertThat(code).isZero();
        assertThat(out.toString()).contains("key-value:", "(none)");
    }

    @Test
    void textShowsGeoKeyForGeoParquet(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("geo.parquet");
        Fixtures.writeGeoCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("meta", file.toString());
        assertThat(code).isZero();
        assertThat(out.toString()).contains("key-value:", "geo");
    }

    @Test
    void jsonIncludesKeyValueMetadataObject(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("geo.parquet");
        Fixtures.writeGeoCities(file);
        StringWriter out = new StringWriter();
        CommandLine cmd = Par.newCommandLine();
        cmd.setOut(new PrintWriter(out));
        int code = cmd.execute("meta", file.toString(), "-o", "json");
        assertThat(code).isZero();
        assertThat(out.toString()).contains("\"keyValueMetadata\"", "geo");
    }
}
