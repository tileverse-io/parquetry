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
package io.tileverse.parquetry.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
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

    @Test
    void nativeBinaryAppliesScalarSqlFilter(@TempDir Path dir) throws Exception {
        assumeTrue(Files.isExecutable(NATIVE_BINARY), "native binary not built; run -Pnative");
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);

        Process process = new ProcessBuilder(
                        NATIVE_BINARY.toAbsolutePath().toString(), "cat", file.toString(), "--filter", "pop > 1000000")
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes());
        int code = process.waitFor();
        assertThat(code).as("output was: %s", output).isZero();
        assertThat(output).contains("Rosario").contains("Cordoba").contains("Buenos Aires");
        assertThat(output).doesNotContain("\"id\":4");
    }

    @Test
    void nativeBinaryStreamsArrowIpc(@TempDir Path dir) throws Exception {
        assumeTrue(Files.isExecutable(NATIVE_BINARY), "native binary not built; run -Pnative");
        Path file = dir.resolve("cities.parquet");
        Fixtures.writeCities(file);

        Process process = new ProcessBuilder(
                        NATIVE_BINARY.toAbsolutePath().toString(), "cat", "-o", "arrow", file.toString())
                .start();
        byte[] stdout = process.getInputStream().readAllBytes();
        int code = process.waitFor();
        assertThat(code)
                .as("stderr was: %s", new String(process.getErrorStream().readAllBytes()))
                .isZero();
        assertThat(stdout).startsWith((byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF);
        assertThat(arrowRowCount(stdout)).isEqualTo(4);
    }

    private static int arrowRowCount(byte[] bytes) throws Exception {
        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
            int rowCount = 0;
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                rowCount += root.getRowCount();
            }
            return rowCount;
        }
    }

    @Test
    void nativeBinaryAppliesSpatialSqlFilter(@TempDir Path dir) throws Exception {
        assumeTrue(Files.isExecutable(NATIVE_BINARY), "native binary not built; run -Pnative");
        Path file = dir.resolve("geo-cities.parquet");
        Fixtures.writeGeoCities(file);

        Process process = new ProcessBuilder(
                        NATIVE_BINARY.toAbsolutePath().toString(),
                        "cat",
                        file.toString(),
                        "--columns",
                        "id",
                        "--filter",
                        "ST_Intersects(geometry, ST_MakeEnvelope(-61,-33.5,-60,-32.5))")
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes());
        int code = process.waitFor();
        assertThat(code).as("output was: %s", output).isZero();
        assertThat(output).contains("\"id\":1");
        assertThat(output).doesNotContain("\"id\":2");
    }
}
