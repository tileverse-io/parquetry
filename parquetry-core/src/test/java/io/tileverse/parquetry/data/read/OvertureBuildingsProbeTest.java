/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.data.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.RowGroup;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Probe test: opens a real Overture Maps buildings GeoParquet file (ZSTD, ~11M rows, nested schema with structs / lists
 * / maps) and reads through the first row group via parquetry's {@code ParquetDataset.open(...)} facade.
 *
 * <p>Gated by the {@code PARQUETRY_OVERTURE_PROBE} environment variable - skipped unless explicitly enabled. Intended
 * for manual local verification of end-to-end Parquet reading against a non-trivial, production-grade file, not as a
 * regression test in CI. Requires the absolute file path below to resolve to a readable file.
 */
@EnabledIfEnvironmentVariable(named = "PARQUETRY_OVERTURE_PROBE", matches = "true")
class OvertureBuildingsProbeTest {

    private static final String OVERTURE_FILE = "/Volumes/geodata/geoparquet/overturemaps/2025-02-19.0/"
            + "theme=buildings/type=building/part-00017-bc5e53eb-b1f2-4193-9ba4-84e6c7ca7995-c000.zstd.parquet";

    /**
     * Full-schema regression check via the row API: read every row with {@link Projection#ALL} (no flat-only
     * workaround). Asserts the row count and inspects a sample row to confirm the repeated / map columns
     * ({@code sources}, {@code names.common}) come back as the expected {@link java.util.List} / {@link java.util.Map}
     * shapes.
     */
    @Test
    void readsFullSchema() throws Exception {
        Path file = Path.of(OVERTURE_FILE);
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetDataset dataset = ParquetDataset.open(reader);
            int leafCount = dataset.schema().leafColumns().size();
            long totalRows =
                    dataset.rowGroups().stream().mapToLong(RowGroup::rowCount).sum();
            System.out.println();
            System.out.println("=== Overture buildings probe :: full schema ===");
            System.out.println("Leaf columns: " + leafCount + "; rows across "
                    + dataset.rowGroups().size() + " row groups: " + totalRows);

            long[] counted = {0L};
            ParquetRecord[] sampleHolder = {null};
            long start = System.nanoTime();
            try (Stream<ParquetRecord> records =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                records.forEach(r -> {
                    if (sampleHolder[0] == null) {
                        sampleHolder[0] = r;
                    }
                    counted[0]++;
                });
            }
            long ms = (System.nanoTime() - start) / 1_000_000L;
            System.out.println(String.format(
                    "Full-schema iterate: %s rows in %d ms (%.1fk rows/sec)",
                    counted[0], ms, (counted[0] / (ms / 1000.0)) / 1000.0));

            assertThat(counted[0]).as("full-schema read row count").isEqualTo(totalRows);

            ParquetRecord sample = sampleHolder[0];
            if (sample == null) {
                return;
            }
            ColumnPath sources = ColumnPath.of("sources");
            Object sourcesValue = sample.get(sources);
            System.out.println("Sample row[0] sources: "
                    + (sourcesValue == null
                            ? "(null)"
                            : sourcesValue.getClass().getSimpleName() + " of size "
                                    + ((java.util.Collection<?>) sourcesValue).size()));
            assertThat(sourcesValue)
                    .as("sources column must be a non-empty List of structs")
                    .isInstanceOf(java.util.List.class);

            ColumnPath namesCommon = ColumnPath.of("names", "common");
            Object namesCommonValue = sample.get(namesCommon);
            System.out.println("Sample row[0] names.common: "
                    + (namesCommonValue == null
                            ? "(null)"
                            : namesCommonValue.getClass().getSimpleName() + " of size "
                                    + ((namesCommonValue instanceof java.util.Map<?, ?> m) ? m.size() : -1)));
            if (namesCommonValue != null) {
                assertThat(namesCommonValue)
                        .as("names.common column must be a Map when present")
                        .isInstanceOf(java.util.Map.class);
            }
        }
    }

    /**
     * Full-schema regression check via the batch API. Asserts the summed batch row count matches the dataset's total
     * row count and spot-checks the first emitted batch's row 0 for the same {@code sources} List shape that
     * {@link #readsFullSchema} verifies on the row API.
     */
    @Test
    void readsFullSchemaViaBatchApi() throws Exception {
        Path file = Path.of(OVERTURE_FILE);
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetDataset dataset = ParquetDataset.open(reader);
            long totalRows =
                    dataset.rowGroups().stream().mapToLong(RowGroup::rowCount).sum();
            System.out.println();
            System.out.println("=== Overture buildings probe :: full schema via batch API ===");
            System.out.println("Rows across " + dataset.rowGroups().size() + " row groups: " + totalRows);

            ColumnPath sourcesPath = ColumnPath.of("sources");
            long[] counted = {0L};
            boolean[] sampleChecked = {false};
            long start = System.nanoTime();
            try (Stream<ParquetRecordBatch> batches =
                    dataset.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                batches.forEach(batch -> {
                    try (ParquetRecordBatch owned = batch) {
                        int rowCount = owned.rowCount();
                        if (!sampleChecked[0] && rowCount > 0) {
                            ParquetRecord sample = owned.materialize(0);
                            Object sourcesValue = sample.get(sourcesPath);
                            System.out.println("Sample row[0] sources: "
                                    + (sourcesValue == null
                                            ? "(null)"
                                            : sourcesValue.getClass().getSimpleName() + " of size "
                                                    + ((java.util.Collection<?>) sourcesValue).size()));
                            assertThat(sourcesValue)
                                    .as("sources column must be a non-empty List of structs (batch API)")
                                    .isInstanceOf(java.util.List.class);
                            sampleChecked[0] = true;
                        }
                        counted[0] += rowCount;
                    }
                });
            }
            long ms = (System.nanoTime() - start) / 1_000_000L;
            System.out.println(String.format(
                    "Full-schema batch iterate: %s rows in %d ms (%.1fk rows/sec)",
                    counted[0], ms, (counted[0] / (ms / 1000.0)) / 1000.0));

            assertThat(counted[0]).as("full-schema batch read row count").isEqualTo(totalRows);
        }
    }

    @Test
    void opensAndReadsFirstRowGroup() throws Exception {
        Path file = Path.of(OVERTURE_FILE);
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetDataset dataset = ParquetDataset.open(reader);

            System.out.println();
            System.out.println("=== Overture buildings probe ===");
            System.out.println("File: " + file);
            System.out.println("Size: " + Files.size(file) + " bytes");

            Map<String, String> kv = dataset.keyValueMetadata();
            System.out.println("Key-value metadata keys: " + kv.keySet());
            String geo = kv.get("geo");
            if (geo != null) {
                int previewLen = Math.min(geo.length(), 240);
                System.out.println(
                        "'geo' (preview): " + geo.substring(0, previewLen) + (geo.length() > previewLen ? " ..." : ""));
            }

            assertThat(dataset.rowGroups()).isNotEmpty();
            System.out.println("Row groups: " + dataset.rowGroups().size());
            RowGroup first = dataset.rowGroups().get(0);
            System.out.println("First row group: rowCount=" + first.rowCount()
                    + ", totalByteSize=" + first.totalByteSize()
                    + ", totalCompressedSize=" + first.totalCompressedSize());

            int leafCount = dataset.schema().leafColumns().size();
            System.out.println("Leaf column count: " + leafCount);
            System.out.println("First 6 leaves: "
                    + dataset.schema().leafColumns().stream().limit(6).toList());

            Set<ColumnPath> flatLeaves = Set.of(
                    ColumnPath.of("id"),
                    ColumnPath.of("geometry"),
                    ColumnPath.of("bbox", "xmin"),
                    ColumnPath.of("bbox", "xmax"),
                    ColumnPath.of("bbox", "ymin"),
                    ColumnPath.of("bbox", "ymax"),
                    ColumnPath.of("version"),
                    ColumnPath.of("subtype"),
                    ColumnPath.of("height"),
                    ColumnPath.of("has_parts"));
            Projection projection = Projection.of(flatLeaves);
            System.out.println("Projected " + flatLeaves.size() + " leaves (flat / optional-group only).");

            long totalRows =
                    dataset.rowGroups().stream().mapToLong(RowGroup::rowCount).sum();
            System.out.println("Total rows across all " + dataset.rowGroups().size() + " row groups: " + totalRows);

            ReadOptions opts = ReadOptions.DEFAULTS;
            long fileSize = Files.size(file);

            long[] counted = {0L};
            ParquetRecord[] sampleHolder = {null};
            long startIter = System.nanoTime();
            try (Stream<ParquetRecord> records = dataset.read(Predicate.ALWAYS_TRUE, projection, opts)) {
                records.forEach(r -> {
                    if (sampleHolder[0] == null) {
                        sampleHolder[0] = r;
                    }
                    counted[0]++;
                });
            }
            long iterMs = (System.nanoTime() - startIter) / 1_000_000L;
            System.out.println(String.format(
                    "Iterate-only:    %s rows in %d ms (%.1fk rows/sec, %.1f MB/sec compressed)",
                    counted[0],
                    iterMs,
                    (counted[0] / (iterMs / 1000.0)) / 1000.0,
                    (fileSize / 1024.0 / 1024.0) / (iterMs / 1000.0)));

            Path csvOut = Path.of("/tmp/parquetry_proj.csv");
            long[] csvRows = {0L};
            long startCsv = System.nanoTime();
            try (BufferedWriter writer = Files.newBufferedWriter(csvOut, StandardCharsets.UTF_8);
                    Stream<ParquetRecord> records = dataset.read(Predicate.ALWAYS_TRUE, projection, opts)) {
                records.forEach(r -> {
                    appendCsvRow(writer, r);
                    csvRows[0]++;
                });
            }
            long csvMs = (System.nanoTime() - startCsv) / 1_000_000L;
            long csvSize = Files.size(csvOut);
            System.out.println(String.format(
                    "Iterate + CSV:   %s rows in %d ms (%.1fk rows/sec) -> %.1f MB at %s",
                    csvRows[0], csvMs, (csvRows[0] / (csvMs / 1000.0)) / 1000.0, csvSize / 1024.0 / 1024.0, csvOut));
            System.out.println(
                    "duckdb reference (same projection -> CSV with HEADER false): 15,379 ms (~736k rows/sec)");
            ParquetRecord sample = sampleHolder[0];
            if (sample != null) {
                System.out.println("Sample row[0]:");
                System.out.println("  id = " + sample.getString(ColumnPath.of("id")));
                System.out.println("  bbox.xmin = " + sample.getFloat(ColumnPath.of("bbox", "xmin")));
                System.out.println("  bbox.ymin = " + sample.getFloat(ColumnPath.of("bbox", "ymin")));
                System.out.println("  version = " + sample.getInt(ColumnPath.of("version")));
                ColumnPath subtype = ColumnPath.of("subtype");
                System.out.println("  subtype = " + (sample.isNull(subtype) ? "(null)" : sample.getString(subtype)));
                ColumnPath height = ColumnPath.of("height");
                System.out.println("  height = " + (sample.isNull(height) ? "(null)" : sample.getDouble(height)));
                System.out.println("  has_parts = " + sample.getBoolean(ColumnPath.of("has_parts")));
                ColumnPath primary = ColumnPath.of("names", "primary");
                System.out.println(
                        "  names.primary = " + (sample.isNull(primary) ? "(null)" : sample.getString(primary)));
                byte[] wkb = sample.getGeometryBytes(ColumnPath.of("geometry"));
                System.out.println("  geometry (WKB) length = " + (wkb == null ? "null" : wkb.length));
            }
            assertThat(counted[0]).isEqualTo(totalRows);
            assertThat(csvRows[0]).isEqualTo(totalRows);
        }
    }

    private static final ColumnPath C_ID = ColumnPath.of("id");
    private static final ColumnPath C_GEOM = ColumnPath.of("geometry");
    private static final ColumnPath C_BBOX_XMIN = ColumnPath.of("bbox", "xmin");
    private static final ColumnPath C_BBOX_XMAX = ColumnPath.of("bbox", "xmax");
    private static final ColumnPath C_BBOX_YMIN = ColumnPath.of("bbox", "ymin");
    private static final ColumnPath C_BBOX_YMAX = ColumnPath.of("bbox", "ymax");
    private static final ColumnPath C_VERSION = ColumnPath.of("version");
    private static final ColumnPath C_SUBTYPE = ColumnPath.of("subtype");
    private static final ColumnPath C_HEIGHT = ColumnPath.of("height");
    private static final ColumnPath C_HAS_PARTS = ColumnPath.of("has_parts");
    private static final HexFormat HEX = HexFormat.of();

    // CSV serializer matching the projection in opensAndReadsFirstRowGroup. Field handling:
    // - Strings (id, subtype): written raw; Overture IDs are hex and subtype values don't contain commas/quotes.
    // - Geometry: WKB bytes -> lowercase hex (parquetry has no WKB->WKT decoder yet; that lands with the geo
    // integration).
    // - bbox sub-fields: Float.toString.
    // - Optional fields: empty token for null (matching duckdb's default CSV null serialization).
    private static void appendCsvRow(BufferedWriter writer, ParquetRecord r) {
        try {
            writer.write(r.getString(C_ID));
            writer.write(',');
            byte[] wkb = r.getGeometryBytes(C_GEOM);
            if (wkb != null) {
                writer.write(HEX.formatHex(wkb));
            }
            writer.write(',');
            writer.write(Float.toString(r.getFloat(C_BBOX_XMIN)));
            writer.write(',');
            writer.write(Float.toString(r.getFloat(C_BBOX_XMAX)));
            writer.write(',');
            writer.write(Float.toString(r.getFloat(C_BBOX_YMIN)));
            writer.write(',');
            writer.write(Float.toString(r.getFloat(C_BBOX_YMAX)));
            writer.write(',');
            writer.write(Integer.toString(r.getInt(C_VERSION)));
            writer.write(',');
            if (!r.isNull(C_SUBTYPE)) {
                writer.write(r.getString(C_SUBTYPE));
            }
            writer.write(',');
            if (!r.isNull(C_HEIGHT)) {
                writer.write(Double.toString(r.getDouble(C_HEIGHT)));
            }
            writer.write(',');
            writer.write(Boolean.toString(r.getBoolean(C_HAS_PARTS)));
            writer.write('\n');
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
