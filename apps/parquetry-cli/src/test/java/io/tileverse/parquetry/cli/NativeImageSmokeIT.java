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
package io.tileverse.parquetry.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.cli.support.Fixtures;

/** Runs the built native binary as a subprocess. Skipped (assumption) when target/par is absent. */
class NativeImageSmokeIT {

    private static final Path NATIVE_BINARY = Path.of("target", "par");

    @Test
    void nativeBinaryCatsAFixture(@TempDir Path dir) throws Exception {
        assumeTrue(Files.isExecutable(NATIVE_BINARY), "native binary not built; run -Pnative");
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);

        Process process = new ProcessBuilder(
                        NATIVE_BINARY.toAbsolutePath().toString(), "cat", file.toString(), "--limit", "1")
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes());
        int code = process.waitFor();
        assertThat(code).as("output was: %s", output).isZero();
        assertThat(output).contains("Rosario");
    }

    @Test
    void nativeBinaryReadsGeoParquetSchema(@TempDir Path dir) throws Exception {
        assumeTrue(Files.isExecutable(NATIVE_BINARY), "native binary not built; run -Pnative");
        Path file = dir.resolve("geo-cities.parquet");
        Fixtures.writeGeoCities(file);

        Process process = new ProcessBuilder(NATIVE_BINARY.toAbsolutePath().toString(), "schema", file.toString())
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes());
        int code = process.waitFor();
        assertThat(code).as("output was: %s", output).isZero();
        assertThat(output).contains("geometry");
    }
}
