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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.internal.filter.spatial.BoundsAccumulator;
import io.tileverse.parquetry.internal.filter.spatial.WkbEnvelope;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Exercises {@link IcebergDataset#bounds} over the {@code v3_geometry} testbed table: ten single-region data files of a
 * native {@code geometry} column, one thousand points each. Every file records exact native geometry statistics, which
 * makes a brute-force scan of the same predicate an exact oracle for the dataset's answer. The dataset has no cheap
 * geometry aggregate. Every bounds call visits the survivor files, folding each file's exact per-file bounds through
 * one shared accumulator.
 */
class IcebergDatasetBoundsTest {

    private static final String TABLE = "v3_geometry";
    private static final ColumnPath GEOM = ColumnPath.of("geom");
    private static final ColumnPath ID = ColumnPath.of("id");

    private static final Bbox CALIFORNIA = Bbox.of2d(-125.0, 32.0, -115.0, 42.0);
    private static final Bbox FAR_OFFSHORE = Bbox.of2d(160.0, -80.0, 170.0, -70.0);

    @TempDir
    Path tempDir;

    @Test
    void unfilteredEqualsBruteForce() {
        withDataset(dataset -> {
            Optional<BoundingBox> bounds = dataset.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
            Optional<BoundingBox> oracle = scanBounds(dataset, Predicate.ALWAYS_TRUE);

            assertThat(oracle).as("the table holds geometry rows to bound").isPresent();
            assertThat(bounds).isPresent();
            assertSameBox2d(bounds.orElseThrow(), oracle.orElseThrow());
        });
    }

    @Test
    void filteredEqualsBruteForce() {
        withDataset(dataset -> {
            Optional<BoundingBox> full = dataset.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);

            // A spatial window whose manifest bounds prune every file but california.
            Predicate spatial = new Predicate.Spatial.BboxIntersects(GEOM, CALIFORNIA);
            assertFilteredMatchesOracle(dataset, spatial, full.orElseThrow());

            // An attribute predicate whose id bounds prune every file but texas and uk.
            Predicate attribute = new Predicate.GtEq(ID, new Value.StringVal("texas"));
            assertFilteredMatchesOracle(dataset, attribute, full.orElseThrow());
        });
    }

    @Test
    void noMatchIsEmpty() {
        withDataset(dataset -> {
            Predicate disjoint = new Predicate.Spatial.BboxIntersects(GEOM, FAR_OFFSHORE);

            Optional<BoundingBox> bounds = dataset.bounds(disjoint, ReadOptions.DEFAULTS);

            assertThat(bounds).isEmpty();
        });
    }

    @Test
    void capabilitiesDoNotAdvertiseCheapBounds() {
        withDataset(dataset -> assertThat(dataset.capabilities().cheapBounds()).isFalse());
    }

    // No merge-on-read delete fixture with a geometry column exists in the corpus yet: the iceberg-geo-testbed data
    // files are append-only. When one is vendored, add it here and assert a deleted row stays out of the box: the
    // per-file query folds the delete predicate, and a merge-on-read deleted row must not widen the bounds.
    @Test
    @Disabled(
            "no merge-on-read delete fixture with a geometry column exists in the corpus yet; add one and assert deleted rows stay out of the box")
    @SuppressWarnings("java:S2699") // placeholder documenting a missing corpus fixture; no assertion until it exists
    void deletedRowsDoNotContribute() {
        // Empty until the corpus gains a merge-on-read delete fixture with a geometry column.
    }

    // --- assertions ---

    private void assertFilteredMatchesOracle(IcebergDataset dataset, Predicate predicate, BoundingBox full) {
        Optional<BoundingBox> bounds = dataset.bounds(predicate, ReadOptions.DEFAULTS);
        Optional<BoundingBox> oracle = scanBounds(dataset, predicate);

        assertThat(oracle)
                .as("the predicate must clip a non-empty subset for a meaningful test")
                .isPresent();
        assertThat(bounds).isPresent();
        assertSameBox2d(bounds.orElseThrow(), oracle.orElseThrow());
        assertThat(boxArea(bounds.orElseThrow()))
                .as("a scan ignoring the predicate would return the full table extent")
                .isLessThan(boxArea(full));
    }

    private static double boxArea(BoundingBox box) {
        return (box.xmax() - box.xmin()) * (box.ymax() - box.ymin());
    }

    private static void assertSameBox2d(BoundingBox actual, BoundingBox expected) {
        assertThat(actual.xmin()).as("xmin").isEqualTo(expected.xmin());
        assertThat(actual.ymin()).as("ymin").isEqualTo(expected.ymin());
        assertThat(actual.xmax()).as("xmax").isEqualTo(expected.xmax());
        assertThat(actual.ymax()).as("ymax").isEqualTo(expected.ymax());
    }

    // --- oracle ---

    /** The exact bounding box of every {@code predicate}-matching row across the table, scanned from the WKB. */
    private static Optional<BoundingBox> scanBounds(IcebergDataset dataset, Predicate predicate) {
        BoundsAccumulator oracle = new BoundsAccumulator();
        try (Stream<ParquetRecordBatch> batches =
                dataset.readBatches(predicate, Projection.ofPhysical(List.of(GEOM)), ReadOptions.DEFAULTS)) {
            batches.forEach(batch -> {
                foldBatchEnvelopes(batch, oracle);
                batch.close();
            });
        }
        return oracle.snapshot();
    }

    private static void foldBatchEnvelopes(ParquetRecordBatch batch, BoundsAccumulator oracle) {
        BinaryVector cells = (BinaryVector) batch.columns().get(GEOM);
        for (int row = 0; row < batch.rowCount(); row++) {
            MemorySegment wkb = cells.get(row);
            if (wkb != null) {
                Bbox envelope = WkbEnvelope.compute(wkb);
                oracle.unionXy(envelope.minX(), envelope.minY(), envelope.maxX(), envelope.maxY());
            }
        }
    }

    // --- dataset construction ---

    private void withDataset(Consumer<IcebergDataset> assertions) {
        Path tableDir = extractTable();
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            IcebergDataset dataset = (IcebergDataset) catalog.dataset(TABLE);
            assertions.accept(dataset);
        }
    }

    private Path extractTable() {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve(TABLE));
        return root.resolve(TABLE);
    }
}
