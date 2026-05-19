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
package io.tileverse.parquetry.read.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.dataset.Dataset;
import io.tileverse.parquetry.dataset.RowGroup;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.read.ConcurrencyMode;
import io.tileverse.parquetry.read.ReadOptions;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Probe test: opens a real Overture Maps buildings GeoParquet file (ZSTD, ~11M rows, nested schema with structs / lists
 * / maps) and reads through the first row group via parquetry's {@code Dataset.open(...)} facade.
 *
 * <p>Gated on the absolute file path - skips silently when the volume is not mounted. Intended for manual local
 * verification of end-to-end Parquet reading against a non-trivial, production-grade file, not as a regression test.
 */
class OvertureBuildingsProbeTest {

    private static final String OVERTURE_FILE = "/Volumes/geodata/geoparquet/overturemaps/2025-02-19.0/"
            + "theme=buildings/type=building/part-00017-bc5e53eb-b1f2-4193-9ba4-84e6c7ca7995-c000.zstd.parquet";

    static boolean fileAvailable() {
        return Files.exists(Path.of(OVERTURE_FILE));
    }

    @Test
    @EnabledIf("fileAvailable")
    void opensAndReadsFirstRowGroup() throws Exception {
        Path file = Path.of(OVERTURE_FILE);
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            Dataset dataset = Dataset.open(reader);

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
                    ColumnPath.of("bbox", "ymin"),
                    ColumnPath.of("version"),
                    ColumnPath.of("subtype"),
                    ColumnPath.of("height"),
                    ColumnPath.of("has_parts"),
                    ColumnPath.of("names", "primary"));
            Projection projection = Projection.of(flatLeaves);
            System.out.println("Projected " + flatLeaves.size() + " leaves (flat / optional-group only).");

            long totalRows =
                    dataset.rowGroups().stream().mapToLong(RowGroup::rowCount).sum();
            System.out.println("Total rows across all " + dataset.rowGroups().size() + " row groups: " + totalRows);

            long start = System.nanoTime();
            long[] counted = {0L};
            ParquetRecord[] sampleHolder = {null};
            ReadOptions opts =
                    ReadOptions.builder().concurrencyMode(ConcurrencyMode.FULL).build();
            try (Stream<ParquetRecord> records = dataset.read(Predicate.ALWAYS_TRUE, projection, opts)) {
                records.forEach(r -> {
                    if (sampleHolder[0] == null) {
                        sampleHolder[0] = r;
                    }
                    counted[0]++;
                });
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

            long fileSize = Files.size(file);
            double mbPerSec = (fileSize / 1024.0 / 1024.0) / (elapsedMs / 1000.0);
            double rowsPerSec = counted[0] / (elapsedMs / 1000.0);
            System.out.println("Read " + counted[0] + " rows in " + elapsedMs + " ms ("
                    + String.format("%.1f", rowsPerSec / 1000.0) + "k rows/sec, "
                    + String.format("%.1f", mbPerSec) + " MB/sec over compressed bytes)");
            System.out.println("gpio reference: 56,115 ms total (11,338,073 rows = ~202k rows/sec)");
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
        }
    }
}
