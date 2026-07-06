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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.iceberg.IcebergDataset.EliminatedFile;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Proves the Iceberg reader prunes whole data files by their manifest bounds at read time: a regional bbox query keeps
 * only the files whose bounds meet it, and an all-true read keeps every file. Pruning never changes the result, which
 * is checked against a brute-force oracle over every row.
 */
class IcebergPruningTest {

    private static final String TABLE = "v3_geometry";
    private static final String GEOM_COLUMN = "geom";
    private static final long EXPECTED_TOTAL_ROWS = 10_000L;

    @TempDir
    Path tempDir;

    @Test
    void prunesToSpatialSurvivors() {
        Path tableDir = extractTable();
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            IcebergDataset dataset = (IcebergDataset) catalog.dataset(TABLE);
            int totalFiles = dataset.plan(Predicate.ALWAYS_TRUE).survivors().size();

            Bbox california = Bbox.of2d(-125.0, 32.0, -115.0, 42.0);
            Predicate predicate = new Predicate.Spatial.BboxIntersects(ColumnPath.of(GEOM_COLUMN), california);

            IcebergDataset.FilePlan plan = dataset.plan(predicate);
            assertThat(plan.survivors()).isNotEmpty();
            assertThat(plan.survivors().size()).isLessThan(totalFiles);
            assertThat(plan.eliminated()).isNotEmpty();
            for (EliminatedFile eliminated : plan.eliminated()) {
                assertThat(eliminated.decision()).isInstanceOf(PruningDecision.Eliminated.class);
            }

            long pushedDown;
            try (Stream<ParquetRecord> rows = dataset.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                pushedDown = rows.count();
            }
            long oracle = bruteForceInWindow(dataset, california);
            assertThat(pushedDown).isEqualTo(oracle);
            assertThat(pushedDown).isGreaterThan(0L);
        }
    }

    @Test
    void alwaysTrueKeepsAllAndReadsEveryRow() {
        Path tableDir = extractTable();
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            IcebergDataset dataset = (IcebergDataset) catalog.dataset(TABLE);
            int totalFiles = dataset.plan(Predicate.ALWAYS_TRUE).survivors().size();
            assertThat(totalFiles).isGreaterThan(1);
            assertThat(dataset.plan(Predicate.ALWAYS_TRUE).eliminated()).isEmpty();

            long decoded;
            try (Stream<ParquetRecord> rows =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                decoded = rows.count();
            }
            assertThat(decoded).isEqualTo(EXPECTED_TOTAL_ROWS);
        }
    }

    @Test
    void eliminatingEveryFileReadsNothing() {
        Path tableDir = extractTable();
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            IcebergDataset dataset = (IcebergDataset) catalog.dataset(TABLE);
            int totalFiles = dataset.plan(Predicate.ALWAYS_TRUE).survivors().size();

            Bbox farOffshore = Bbox.of2d(160.0, -80.0, 170.0, -70.0);
            Predicate predicate = new Predicate.Spatial.BboxIntersects(ColumnPath.of(GEOM_COLUMN), farOffshore);

            IcebergDataset.FilePlan plan = dataset.plan(predicate);
            assertThat(plan.survivors()).isEmpty();
            assertThat(plan.eliminated()).hasSize(totalFiles);

            long counted = dataset.count(predicate, ReadOptions.DEFAULTS);
            assertThat(counted).isZero();

            long decoded;
            try (Stream<ParquetRecord> rows = dataset.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                decoded = rows.count();
            }
            assertThat(decoded).isZero();
        }
    }

    private Path extractTable() {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve(TABLE));
        return root.resolve(TABLE);
    }

    private static long bruteForceInWindow(IcebergDataset dataset, Bbox window) {
        long inside = 0L;
        try (Stream<ParquetRecord> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            for (ParquetRecord row : (Iterable<ParquetRecord>) rows::iterator) {
                if (pointInWindow(row, window)) {
                    inside++;
                }
            }
        }
        return inside;
    }

    private static boolean pointInWindow(ParquetRecord row, Bbox window) {
        byte[] wkb = row.getBinary(ColumnPath.of(GEOM_COLUMN));
        ByteBuffer buffer = ByteBuffer.wrap(wkb).order(ByteOrder.LITTLE_ENDIAN);
        // Little-endian WKB point: 1-byte byte-order flag + 4-byte geometry type, then the X and Y doubles.
        double x = buffer.getDouble(5);
        double y = buffer.getDouble(13);
        return x >= window.minX() && x <= window.maxX() && y >= window.minY() && y <= window.maxY();
    }
}
