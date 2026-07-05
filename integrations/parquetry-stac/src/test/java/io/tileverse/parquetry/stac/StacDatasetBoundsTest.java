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
package io.tileverse.parquetry.stac;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.dataset.OpenOptions;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.internal.filter.spatial.BoundsAccumulator;
import io.tileverse.parquetry.internal.filter.spatial.WkbEnvelope;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Exercises {@link StacDataset#bounds} over a multi-part collection. The first part spans the whole collection extent
 * and the others nest inside it, which lets the geo-metadata fast path (which reads only the first part) agree with a
 * brute-force scan of every part. Every part records exact native geometry statistics, making a scan of the same
 * predicate an exact oracle for the dataset's answer. Item boxes are deliberately padded wider than their part's true
 * extent. A bounds answer that equals the tight scan therefore proves the conservative item-box union was never
 * returned.
 */
class StacDatasetBoundsTest {

    private static final String GEOMETRY = "geometry";
    private static final ColumnPath GEOMETRY_PATH = ColumnPath.of(GEOMETRY);

    /** Part 0 spans the collection; parts 1-3 nest inside it. Union over every point is {@code [0, 0, 120, 120]}. */
    private static final double[][][] POINTS_PER_PART = {
        {{0.0, 0.0}, {120.0, 0.0}, {0.0, 120.0}, {120.0, 120.0}, {60.0, 60.0}},
        {{5.0, 5.0}, {10.0, 10.0}, {15.0, 15.0}},
        {{105.0, 105.0}, {110.0, 110.0}, {115.0, 115.0}},
        {{55.0, 55.0}, {60.0, 60.0}, {65.0, 65.0}},
    };

    /** Item boxes padded ten units beyond each part's true extent: their union is wider than any true geometry. */
    private static final double[][] ITEM_BBOXES = {
        {-10.0, -10.0, 130.0, 130.0},
        {-5.0, -5.0, 25.0, 25.0},
        {95.0, 95.0, 125.0, 125.0},
        {45.0, 45.0, 75.0, 75.0},
    };

    @TempDir
    Path tempDir;

    @Test
    void unfilteredEqualsBruteForce() throws Exception {
        StacDataset dataset = dataset(GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0);

        Optional<BoundingBox> bounds = dataset.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
        Optional<BoundingBox> oracle = scanBounds(dataset, Predicate.ALWAYS_TRUE);

        assertThat(oracle).as("the collection holds geometry rows to bound").isPresent();
        assertThat(bounds).isPresent();
        assertSameBox2d(bounds.orElseThrow(), oracle.orElseThrow());
    }

    @Test
    void filteredEqualsBruteForce() throws Exception {
        StacDataset dataset = dataset(GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0);
        Optional<BoundingBox> full = dataset.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);

        // Case one: a window whose item box prunes the south-west and north-east parts.
        assertFilteredMatchesOracle(dataset, window(50.0, 50.0, 70.0, 70.0), full.orElseThrow());
        // Case two: a window that prunes a different pair, narrowing a distinct row set.
        assertFilteredMatchesOracle(dataset, window(0.0, 0.0, 20.0, 20.0), full.orElseThrow());
    }

    @Test
    void noMatchIsEmpty() throws Exception {
        StacDataset dataset = dataset(GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0);

        Optional<BoundingBox> bounds = dataset.bounds(window(1_000.0, 1_000.0, 1_010.0, 1_010.0), ReadOptions.DEFAULTS);

        assertThat(bounds).isEmpty();
    }

    @Test
    void boundsLessMetadataStillVisitsFiles() throws Exception {
        StacDataset dataset = dataset(GeoParquetMetadataMode.V2_0_ONLY);

        assertThat(dataset.geoMetadata())
                .as("the parts have native statistics but no geo metadata document")
                .isEmpty();

        Optional<BoundingBox> bounds = dataset.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
        Optional<BoundingBox> oracle = scanBounds(dataset, Predicate.ALWAYS_TRUE);
        BoundingBox itemBoxUnion = itemBoxUnion();

        assertThat(bounds).isPresent();
        assertSameBox2d(bounds.orElseThrow(), oracle.orElseThrow());
        assertThat(bounds.orElseThrow().xmin())
                .as("returning the conservative item-box union would reach past the true geometry extent")
                .isGreaterThan(itemBoxUnion.xmin());
        assertThat(bounds.orElseThrow().xmax()).isLessThan(itemBoxUnion.xmax());
    }

    @Test
    void capabilitiesAdvertiseCheapBoundsOnlyWithGeoMetadata() throws Exception {
        StacDataset withMetadata = dataset(GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0);
        StacDataset withoutMetadata = dataset(GeoParquetMetadataMode.V2_0_ONLY);

        assertThat(withMetadata.capabilities().cheapBounds()).isTrue();
        assertThat(withoutMetadata.capabilities().cheapBounds()).isFalse();
    }

    // --- assertions ---

    private void assertFilteredMatchesOracle(StacDataset dataset, Predicate predicate, BoundingBox full) {
        Optional<BoundingBox> bounds = dataset.bounds(predicate, ReadOptions.DEFAULTS);
        Optional<BoundingBox> oracle = scanBounds(dataset, predicate);

        assertThat(oracle)
                .as("the window must clip a non-empty subset for a meaningful test")
                .isPresent();
        assertThat(bounds).isPresent();
        assertSameBox2d(bounds.orElseThrow(), oracle.orElseThrow());
        assertThat(bounds.orElseThrow().xmax())
                .as("a scan ignoring the predicate would return the full collection extent")
                .isLessThan(full.xmax());
    }

    private static Predicate window(double minX, double minY, double maxX, double maxY) {
        return new Predicate.Spatial.BboxIntersects(GEOMETRY_PATH, Bbox.of2d(minX, minY, maxX, maxY));
    }

    private static void assertSameBox2d(BoundingBox actual, BoundingBox expected) {
        assertThat(actual.xmin()).as("xmin").isEqualTo(expected.xmin());
        assertThat(actual.ymin()).as("ymin").isEqualTo(expected.ymin());
        assertThat(actual.xmax()).as("xmax").isEqualTo(expected.xmax());
        assertThat(actual.ymax()).as("ymax").isEqualTo(expected.ymax());
    }

    // --- oracle ---

    /** The exact bounding box of every {@code predicate}-matching row across the collection, scanned from the WKB. */
    private static Optional<BoundingBox> scanBounds(StacDataset dataset, Predicate predicate) {
        BoundsAccumulator oracle = new BoundsAccumulator();
        try (Stream<ParquetRecordBatch> batches =
                dataset.readBatches(predicate, Projection.ofPhysical(List.of(GEOMETRY_PATH)), ReadOptions.DEFAULTS)) {
            batches.forEach(batch -> {
                foldBatchEnvelopes(batch, oracle);
                batch.close();
            });
        }
        return oracle.snapshot();
    }

    private static void foldBatchEnvelopes(ParquetRecordBatch batch, BoundsAccumulator oracle) {
        BinaryVector cells = (BinaryVector) batch.columns().get(GEOMETRY_PATH);
        for (int row = 0; row < batch.rowCount(); row++) {
            MemorySegment wkb = cells.get(row);
            if (wkb != null) {
                Bbox envelope = WkbEnvelope.compute(wkb);
                oracle.unionXy(envelope.minX(), envelope.minY(), envelope.maxX(), envelope.maxY());
            }
        }
    }

    private static BoundingBox itemBoxUnion() {
        BoundsAccumulator accumulator = new BoundsAccumulator();
        for (double[] box : ITEM_BBOXES) {
            accumulator.unionXy(box[0], box[1], box[2], box[3]);
        }
        return accumulator.snapshot().orElseThrow();
    }

    // --- dataset construction ---

    private StacDataset dataset(GeoParquetMetadataMode mode) throws Exception {
        List<StacItemRef> refs = new ArrayList<>(POINTS_PER_PART.length);
        List<double[]> bboxes = new ArrayList<>(POINTS_PER_PART.length);
        List<ByteRangeSource> sources = new ArrayList<>(POINTS_PER_PART.length);
        for (int part = 0; part < POINTS_PER_PART.length; part++) {
            Path file = tempDir.resolve(mode.name() + "-part-" + part + ".parquet");
            StacPointParquet.writePoints(file, GEOMETRY, mode, POINTS_PER_PART[part]);
            refs.add(new StacItemRef("part-" + part, file.getFileName().toString()));
            bboxes.add(ITEM_BBOXES[part].clone());
            sources.add(ByteRangeSource.ofFile(file));
        }
        return new StacDataset("points", GEOMETRY, refs, bboxes, sources, fanOutOptions());
    }

    private static OpenOptions fanOutOptions() {
        return OpenOptions.builder()
                .runtime(ParquetRuntime.builder().maxConcurrentFiles(4).build())
                .build();
    }
}
