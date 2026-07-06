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
package io.tileverse.parquetry.internal.write;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Write-side performance probe. Gated by the {@code PARQUETRY_WRITE_PROBE} environment variable so the test is a no-op
 * under normal {@code mvn verify}.
 *
 * <p>Each scenario writes a synthetic record stream through {@link ParquetFileWriter} and prints throughput plus the
 * resulting file size. When {@code PARQUETRY_WRITE_PROBE_INPUT} points at an existing Parquet file the probe also
 * round-trips that file (read + re-write at the configured options) to compare against the synthetic numbers.
 */
// S2699: env-var-gated probe; each test prints throughput numbers and only asserts the run does not crash.
@SuppressWarnings("java:S2699")
@EnabledIfEnvironmentVariable(named = "PARQUETRY_WRITE_PROBE", matches = "true")
class WritePerformanceProbeTest {

    private static final int SYNTHETIC_ROW_COUNT = 1_000_000;

    @TempDir
    Path tempDir;

    @Test
    void defaultSettingsThroughput() throws Exception {
        runScenario("default", WriteOptions.builder().tempDir(tempDir).build());
    }

    @Test
    void compressionScenarioZstdVsSnappy() throws Exception {
        runScenario(
                "zstd-3",
                WriteOptions.builder()
                        .tempDir(tempDir)
                        .defaultCompression(Compression.zstd(3))
                        .build());
        runScenario(
                "snappy",
                WriteOptions.builder()
                        .tempDir(tempDir)
                        .defaultCompression(Compression.snappy())
                        .build());
    }

    @Test
    void pageFormatV2VsV1() throws Exception {
        runScenario(
                "page-v2",
                WriteOptions.builder()
                        .tempDir(tempDir)
                        .parquetVersion(ParquetVersion.V2_0)
                        .build());
        runScenario(
                "page-v1",
                WriteOptions.builder()
                        .tempDir(tempDir)
                        .parquetVersion(ParquetVersion.V1_1)
                        .build());
    }

    @Test
    void rowGroupSizingAdaptiveVsSmall() throws Exception {
        runScenario(
                "rg-adaptive",
                WriteOptions.builder()
                        .tempDir(tempDir)
                        .rowGroupSize(RowGroupSize.adaptive())
                        .build());
        runScenario(
                "rg-bytes-64k",
                WriteOptions.builder()
                        .tempDir(tempDir)
                        .rowGroupSize(RowGroupSize.bytes(64L * 1024L))
                        .build());
    }

    // --- driver ---

    private void runScenario(String label, WriteOptions options) throws Exception {
        String inputPath = System.getenv("PARQUETRY_WRITE_PROBE_INPUT");
        if (inputPath != null && Files.isRegularFile(Path.of(inputPath))) {
            runReplayScenario(label, Path.of(inputPath), options);
            return;
        }
        runSyntheticScenario(label, options);
    }

    private void runSyntheticScenario(String label, WriteOptions options) throws Exception {
        ParquetSchema schema = syntheticSchema();
        Path file = tempDir.resolve(label + "-synthetic.parquet");
        long startNanos = System.nanoTime();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int i = 0; i < SYNTHETIC_ROW_COUNT; i++) {
            rows.add(syntheticRow(i));
        }
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(WriteFixtures.batch(schema, rows));
        }
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
        long size = Files.size(file);
        System.out.println(String.format(
                "[probe] %s: rows=%d bytes=%d duration=%d ms (synthetic)",
                label, SYNTHETIC_ROW_COUNT, size, elapsedMs));
    }

    private void runReplayScenario(String label, Path input, WriteOptions options) throws Exception {
        Path file = tempDir.resolve(label + "-replay.parquet");
        long startNanos = System.nanoTime();
        long rowCount = 0L;
        try (ByteRangeSource source = ByteRangeSource.ofFile(input)) {
            ParquetFileReader dataset = ParquetFileReader.open(source);
            ParquetSchema schema = dataset.schema();
            try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options);
                    Stream<ParquetRecord> records =
                            dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                long[] counter = {0L};
                records.forEach(parquetRecord -> {
                    try {
                        writer.writeBatch(WriteFixtures.batch(schema, List.of(toRowMap(schema, parquetRecord))));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    counter[0]++;
                });
                rowCount = counter[0];
            }
        }
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
        long size = Files.size(file);
        System.out.println(String.format(
                "[probe] %s: rows=%d bytes=%d duration=%d ms (replay of %s)",
                label, rowCount, size, elapsedMs, input.getFileName()));
    }

    private static Map<ColumnPath, Object> toRowMap(ParquetSchema schema, ParquetRecord parquetRecord) {
        Map<ColumnPath, Object> values = new HashMap<>();
        for (ColumnPath leaf : schema.leafColumns()) {
            if (parquetRecord.isNull(leaf)) {
                continue;
            }
            values.put(leaf, parquetRecord.get(leaf));
        }
        return values;
    }

    // --- synthetic data ---

    private static ParquetSchema syntheticSchema() {
        SchemaNode.Primitive id = primitive("id", PrimitiveKind.INT64);
        SchemaNode.Primitive timestamp = primitive("ts", PrimitiveKind.INT64);
        SchemaNode.Primitive measurement = primitive("value", PrimitiveKind.DOUBLE);
        SchemaNode.Primitive flag = primitive("flag", PrimitiveKind.INT32);
        SchemaNode.Primitive label = primitive("label", PrimitiveKind.BYTE_ARRAY);
        List<SchemaNode> children = Stream.of(id, timestamp, measurement, flag, label)
                .map(f -> (SchemaNode) f)
                .toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive primitive(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.REQUIRED, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static Map<ColumnPath, Object> syntheticRow(int i) {
        Map<ColumnPath, Object> values = new HashMap<>(5);
        values.put(ColumnPath.of("id"), (long) i);
        values.put(ColumnPath.of("ts"), 1_700_000_000_000L + i);
        values.put(ColumnPath.of("value"), Math.sin(i * 0.001));
        values.put(ColumnPath.of("flag"), i % 8);
        values.put(
                ColumnPath.of("label"), MemorySegment.ofArray(("label-" + (i % 64)).getBytes(StandardCharsets.UTF_8)));
        return values;
    }
}
