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

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.cli.support.CliRunner;
import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * End-to-end checks that {@code cat -o arrow} streams valid Apache Arrow IPC bytes to stdout. Each case runs the CLI,
 * captures stdout as raw bytes, and reads them back with the arrow-vector reader to verify row counts and values
 * survive the round-trip through the real {@code System.out}.
 */
class ArrowOutputIT {

    @Test
    void arrowFullRead(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("cities.parquet");
        Fixtures.writeCities(file);

        CliRunner.BytesResult result = CliRunner.runCapturingBytes("cat", "-o", "arrow", file.toString());

        assertThat(result.exitCode()).isZero();
        ArrowRows rows = readArrow(result.stdout());
        assertThat(rows.rowCount()).isEqualTo(4);
        assertThat(rows.populations()).contains(1_300_000L, 1_400_000L, 3_100_000L, 500L);
        assertThat(rows.names()).contains("Rosario");
    }

    @Test
    void arrowWithFilter(@TempDir Path tmp) throws Exception {
        Path file = tmp.resolve("cities.parquet");
        Fixtures.writeCities(file);

        CliRunner.BytesResult result =
                CliRunner.runCapturingBytes("cat", "-o", "arrow", file.toString(), "--filter", "pop > 1300000");

        assertThat(result.exitCode()).isZero();
        ArrowRows rows = readArrow(result.stdout());
        assertThat(rows.rowCount()).isEqualTo(2);
    }

    @Test
    void arrowOverIcebergTable(@TempDir Path tmp) throws Exception {
        Path tableDir = TestCorpus.extractDirectory("iceberg-geo-testbed/v3_geometry", tmp);

        CliRunner.BytesResult result = CliRunner.runCapturingBytes("cat", "-o", "arrow", tableDir.toString());

        assertThat(result.exitCode()).isZero();
        assertThat(totalRows(result.stdout())).isEqualTo(10_000);
    }

    private static int totalRows(byte[] bytes) throws Exception {
        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
            int rowCount = 0;
            while (reader.loadNextBatch()) {
                rowCount += reader.getVectorSchemaRoot().getRowCount();
            }
            return rowCount;
        }
    }

    private record ArrowRows(int rowCount, List<Long> populations, List<String> names) {}

    private static ArrowRows readArrow(byte[] bytes) throws Exception {
        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
            int rowCount = 0;
            List<Long> populations = new ArrayList<>();
            List<String> names = new ArrayList<>();
            while (reader.loadNextBatch()) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                collectColumns(root, populations, names);
                rowCount += root.getRowCount();
            }
            return new ArrowRows(rowCount, populations, names);
        }
    }

    private static void collectColumns(VectorSchemaRoot root, List<Long> populations, List<String> names) {
        BigIntVector pop = (BigIntVector) root.getVector("pop");
        VarCharVector name = (VarCharVector) root.getVector("name");
        for (int row = 0; row < root.getRowCount(); row++) {
            if (!pop.isNull(row)) {
                populations.add(pop.get(row));
            }
            if (!name.isNull(row)) {
                names.add(new String(name.get(row), StandardCharsets.UTF_8));
            }
        }
    }
}
