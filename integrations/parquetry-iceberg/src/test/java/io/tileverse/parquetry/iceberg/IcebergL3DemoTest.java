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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.prune.FilePruner;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Prints a human-readable report of manifest-bound file pruning (L3) over the {@code v3_geometry} testbed table: each
 * data file's geometry bounds, the keep-or-skip decision a regional query produces, and the resulting read. Run it to
 * narrate L3 without reading test assertions:
 *
 * <pre>./mvnw -q -pl :parquetry-iceberg test -Dtest=IcebergL3DemoTest \
 *     -DextraArgLine="-Xmx2g" -Dsurefire.failIfNoSpecifiedTests=false \
 *     -Dmaven.test.redirectTestOutputToFile=false</pre>
 */
class IcebergL3DemoTest {

    private static final String TABLE = "v3_geometry";
    private static final String GEOM_COLUMN = "geom";

    @TempDir
    Path tempDir;

    @Test
    void reportsManifestBoundPruning() throws IOException {
        Path tableDir = extractTable();
        IcebergTableMetadata metadata = readMetadata(tableDir);
        List<IcebergField> fields = metadata.fields();
        Bbox california = Bbox.of2d(-125.0, 32.0, -115.0, 42.0);
        Predicate query = new Predicate.Spatial.BboxIntersects(ColumnPath.of(GEOM_COLUMN), california);

        StringBuilder report = new StringBuilder();
        int filesRead = printPruningTable(tableDir, metadata, fields, query, california, report);

        printEndToEndRead(tableDir, query, report);
        System.out.println(report);

        assertThat(filesRead).isPositive();
    }

    private int printPruningTable(
            Path tableDir,
            IcebergTableMetadata metadata,
            List<IcebergField> fields,
            Predicate query,
            Bbox california,
            StringBuilder out) {
        out.append("=== parquetry-iceberg L3: manifest-bound file pruning ===\n");
        out.append("table        : ")
                .append(TABLE)
                .append(" (Iceberg format-version ")
                .append(metadata.formatVersion())
                .append(")\n");
        out.append("snapshot     : ").append(metadata.currentSnapshotId()).append('\n');
        out.append("geometry col : ").append(GEOM_COLUMN).append('\n');
        out.append("query        : BboxIntersects(")
                .append(GEOM_COLUMN)
                .append(", ")
                .append(format(california))
                .append(")  [California]\n\n");
        out.append(String.format("%-26s %-44s %9s   %s%n", "file", "geometry bbox", "rows", "decision"));

        try (IcebergFileIO io =
                StorageIcebergFileIO.owning(StorageFactory.open(tableDir.toUri()), metadata.tableLocation())) {
            String manifestListLocation = metadata.manifestListLocation();
            List<IcebergManifests.DataFileRef> refs = IcebergManifests.readDataFiles(manifestListLocation, io);
            int filesRead = 0;
            long rowsRead = 0L;
            long rowsSkipped = 0L;
            for (IcebergManifests.DataFileRef ref : refs) {
                FileStats stats = IcebergFileStats.from(ref, fields, Map.of());
                BoundingBox box = stats.geometryBounds().get(ColumnPath.of(GEOM_COLUMN));
                PruningDecision decision = FilePruner.evaluate(query, stats);
                boolean skip = decision instanceof PruningDecision.Eliminated;
                if (skip) {
                    rowsSkipped += ref.recordCount();
                } else {
                    filesRead++;
                    rowsRead += ref.recordCount();
                }
                out.append(String.format(
                        "%-26s %-44s %9d   %s%n",
                        shortName(ref.location()), format(box), ref.recordCount(), decisionLabel(decision)));
            }
            out.append('\n');
            out.append(String.format(
                    "result       : %d of %d files read, %d pruned from manifest geometry bounds%n",
                    filesRead, refs.size(), refs.size() - filesRead));
            out.append(String.format(
                    "rows in I/O  : %d read, %d skipped without opening the file%n", rowsRead, rowsSkipped));
            return filesRead;
        }
    }

    private void printEndToEndRead(Path tableDir, Predicate query, StringBuilder out) {
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            IcebergDataset dataset = (IcebergDataset) catalog.dataset(TABLE);
            long total = dataset.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
            long matched;
            try (Stream<ParquetRecord> rows = dataset.read(query, Projection.ALL, ReadOptions.DEFAULTS)) {
                matched = rows.count();
            }
            out.append(String.format(
                    "read         : %d rows match the query (read from the surviving file); table total is %d rows%n",
                    matched, total));
        }
    }

    private Path extractTable() {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve(TABLE));
        return root.resolve(TABLE);
    }

    private static IcebergTableMetadata readMetadata(Path tableDir) throws IOException {
        Path metadataJson = tableDir.resolve("metadata").resolve("v1.metadata.json");
        String json = Files.readString(metadataJson);
        return IcebergTableMetadata.read(json, IcebergOptions.defaults());
    }

    private static String decisionLabel(PruningDecision decision) {
        if (decision instanceof PruningDecision.Eliminated eliminated) {
            return "SKIP  (" + eliminated.reason() + ")";
        }
        return "READ";
    }

    private static String format(BoundingBox box) {
        return String.format("(%.2f, %.2f) .. (%.2f, %.2f)", box.xmin(), box.ymin(), box.xmax(), box.ymax());
    }

    private static String format(Bbox box) {
        return String.format("(%.0f, %.0f) .. (%.0f, %.0f)", box.minX(), box.minY(), box.maxX(), box.maxY());
    }

    private static String shortName(String location) {
        int slash = location.lastIndexOf('/');
        return slash < 0 ? location : location.substring(slash + 1);
    }
}
