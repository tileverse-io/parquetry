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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.internal.filter.spatial.BoundsAccumulator;
import io.tileverse.parquetry.internal.filter.spatial.WkbEnvelope;
import io.tileverse.parquetry.io.LocalFileSource;
import io.tileverse.parquetry.observe.QueryObserver;
import io.tileverse.parquetry.observe.RowGroupRead;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;
import io.tileverse.parquetry.testsupport.FooterRewrite;
import io.tileverse.parquetry.testsupport.PointParquet;

/**
 * Exercises {@link FilesetDataset#bounds} over a Hive-partitioned GeoParquet dataset: one point file per {@code region}
 * with a distinct extent. Every file records exact native geometry statistics. A brute-force scan of the same predicate
 * is therefore an exact oracle for the dataset's answer. The attribute predicate prunes files by their synthesized
 * partition value; the spatial window narrows the surviving rows.
 */
class FilesetDatasetBoundsTest {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");

    private static final List<String> REGIONS = List.of("west", "east", "north");

    /** Disjoint per-region extents. The union over every point is {@code [0, 0, 110, 110]}. */
    private static final double[][][] POINTS_PER_REGION = {
        {{0.0, 0.0}, {10.0, 10.0}},
        {{100.0, 100.0}, {110.0, 110.0}},
        {{0.0, 100.0}, {10.0, 110.0}},
    };

    @TempDir
    Path root;

    @Test
    void unfilteredEqualsBruteForce() throws Exception {
        try (FilesetCatalog catalog = openCatalog()) {
            ParquetDataset dataset = onlyDataset(catalog);

            Optional<BoundingBox> bounds = dataset.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
            Optional<BoundingBox> oracle = scanBounds(dataset, Predicate.ALWAYS_TRUE);

            assertThat(oracle).as("the dataset holds geometry rows to bound").isPresent();
            assertThat(bounds).isPresent();
            assertSameBox2d(bounds.orElseThrow(), oracle.orElseThrow());
        }
    }

    @Test
    void filteredEqualsBruteForce() throws Exception {
        try (FilesetCatalog catalog = openCatalog()) {
            ParquetDataset dataset = onlyDataset(catalog);
            Optional<BoundingBox> full = dataset.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);

            // An attribute predicate that prunes to the single "east" partition file.
            assertFilteredMatchesOracle(dataset, Pred.col("region").eq("east"), full.orElseThrow());
            // A spatial window that keeps only the "west" points.
            Predicate window = Pred.col("geometry").bboxIntersects(Bbox.of2d(-5.0, -5.0, 15.0, 15.0));
            assertFilteredMatchesOracle(dataset, window, full.orElseThrow());
        }
    }

    @Test
    void unfilteredWithoutMetadataBoxFallsBackToScan() throws Exception {
        // Files with native statistics but no geo metadata document (the same dataset-level outcome as files
        // whose 'geo' metadata declares a covering but no bbox): the aggregated metadata box is absent and
        // the bounds must come from scanning, not report empty.
        for (int i = 0; i < REGIONS.size(); i++) {
            Path dir = Files.createDirectories(root.resolve("region=" + REGIONS.get(i)));
            PointParquet.writePointsWithoutGeoMetadata(dir.resolve("points.parquet"), "geometry", POINTS_PER_REGION[i]);
        }
        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults())) {
            ParquetDataset dataset = onlyDataset(catalog);

            Optional<BoundingBox> bounds = dataset.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
            Optional<BoundingBox> oracle = scanBounds(dataset, Predicate.ALWAYS_TRUE);

            assertThat(oracle).as("the dataset holds geometry rows to bound").isPresent();
            assertThat(bounds).isPresent();
            assertSameBox2d(bounds.orElseThrow(), oracle.orElseThrow());
        }
    }

    @Test
    void estimatedBoundsAnswerFromFooterStatisticsAlone() throws Exception {
        try (FilesetCatalog catalog = openCatalog()) {
            ParquetDataset dataset = onlyDataset(catalog);

            Optional<BoundingBox> unfiltered = dataset.estimatedBounds(Predicate.ALWAYS_TRUE);
            assertThat(unfiltered).isPresent();

            // The attribute predicate prunes to the single "east" partition file; the estimate is that file's
            // footer geometry box, which must cover the exact bounds of its rows.
            Optional<BoundingBox> estimated =
                    dataset.estimatedBounds(Pred.col("region").eq("east"));
            Optional<BoundingBox> exact = dataset.bounds(Pred.col("region").eq("east"), ReadOptions.DEFAULTS);

            assertThat(estimated).isPresent();
            assertThat(exact).isPresent();
            assertCovers(estimated.orElseThrow(), exact.orElseThrow());
        }
    }

    private static void assertCovers(BoundingBox outer, BoundingBox inner) {
        assertThat(outer.xmin()).isLessThanOrEqualTo(inner.xmin());
        assertThat(outer.ymin()).isLessThanOrEqualTo(inner.ymin());
        assertThat(outer.xmax()).isGreaterThanOrEqualTo(inner.xmax());
        assertThat(outer.ymax()).isGreaterThanOrEqualTo(inner.ymax());
    }

    /**
     * A GEOGRAPHY-aware writer declares the extent of points at longitudes 175 and -175 as a box wrapping the
     * antimeridian. The unfiltered answer and its estimate are both the planar enclosure of that declared box.
     */
    @Test
    void aDeclaredBboxWrappingTheAntimeridianIsReportedAsItsPlanarEnclosure() throws Exception {
        double[][] points = {{175.0, -5.0}, {-175.0, 5.0}};
        Path planar = PointParquet.writePoints(
                root.resolve("planar.parquet"), "geometry", GeoParquetMetadataMode.V1_1_ONLY, points);
        BoundingBox wrapping =
                BoundingBox.builder().xmin(175).xmax(-175).ymin(-5).ymax(5).build();
        Path file = FooterRewrite.rewrite(
                planar, root.resolve("wrapping.parquet"), FooterRewrite.geoMetadataBbox("geometry", wrapping));
        BoundingBox enclosure =
                BoundingBox.builder().xmin(-180).xmax(180).ymin(-5).ymax(5).build();
        CatalogOptions options =
                CatalogOptions.builder().datasetName("wrapping").build();
        try (FilesetCatalog catalog = FilesetCatalog.open(LocalFileSource.file(file), options)) {
            ParquetDataset dataset = onlyDataset(catalog);

            Optional<BoundingBox> bounds = dataset.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
            Optional<BoundingBox> estimated = dataset.estimatedBounds(Predicate.ALWAYS_TRUE);

            assertSameBox2d(bounds.orElseThrow(), enclosure);
            assertSameBox2d(estimated.orElseThrow(), enclosure);
        }
    }

    /**
     * Two copies of the corpus file under Hive partitions: the predicate keeps the empty geometries of one copy and the
     * points of the other. The fan-out union is the extent of the points, neither inverted nor widened by the copy that
     * matched only empty geometries.
     */
    @Test
    void aFileMatchingOnlyEmptyGeometriesAddsNothingToTheDatasetBounds() throws Exception {
        for (String kind : List.of("empty", "point")) {
            Path partition = Files.createDirectories(root.resolve("kind=" + kind));
            TestCorpus.extractFile("parquet-testing/data/geospatial/geospatial.parquet", partition);
        }
        Predicate emptiesAndPoints = Pred.or(
                Pred.and(Pred.col("kind").eq("empty"), Pred.col("group").eq("empty-geometries")),
                Pred.and(Pred.col("kind").eq("point"), Pred.col("group").eq("point")));
        BoundingBox points =
                BoundingBox.builder().xmin(30).xmax(40).ymin(10).ymax(20).build();
        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults())) {
            ParquetDataset dataset = onlyDataset(catalog);

            Optional<BoundingBox> bounds = dataset.bounds(emptiesAndPoints, ReadOptions.DEFAULTS);

            assertSameBox2d(bounds.orElseThrow(), points);
        }
    }

    /**
     * A file without a declared bbox could hold geometries anywhere, leaving the dataset without declared bounds. The
     * answer then comes from the per-file boxes, native or covering, and reads no row group.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("undeclaringEdits")
    void aFileWithoutADeclaredBboxLeavesTheDeclaredBoundsUnknown(
            String label, GeoParquetMetadataMode mode, UnaryOperator<FileMetaData> undeclare) throws Exception {
        Path data = Files.createDirectories(root.resolve("data"));
        double[][] westPoints = {{0, 0}, {10, 10}};
        double[][] eastPoints = {{100, 0}, {110, 10}};
        PointParquet.writePoints(data.resolve("west.parquet"), "geometry", mode, westPoints);
        Path east = PointParquet.writePoints(root.resolve("east.parquet"), "geometry", mode, eastPoints);
        FooterRewrite.rewrite(east, data.resolve("east.parquet"), undeclare);
        RowGroupReadCounter counter = new RowGroupReadCounter();
        ReadOptions observed = ReadOptions.builder().queryObserver(counter).build();
        BoundingBox bothFiles =
                BoundingBox.builder().xmin(0).xmax(110).ymin(0).ymax(10).build();
        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(data, "*.parquet"), CatalogOptions.defaults())) {
            ParquetDataset dataset = onlyDataset(catalog);

            Optional<BoundingBox> bounds = dataset.bounds(Predicate.ALWAYS_TRUE, observed);
            Optional<BoundingBox> estimated = dataset.estimatedBounds(Predicate.ALWAYS_TRUE);

            assertSameBox2d(bounds.orElseThrow(), bothFiles);
            assertSameBox2d(estimated.orElseThrow(), bothFiles);
            assertThat(counter.reads())
                    .as("the per-file boxes answer without a scan")
                    .isZero();
        }
    }

    /**
     * The covering case keeps the geo metadata of its file: a GeoParquet 1.x file holds its geometry annotation there,
     * and without it the file would no longer share the schema of the other file.
     */
    static Stream<Arguments> undeclaringEdits() {
        return Stream.of(
                Arguments.of(
                        "native boxes, a file without geo metadata",
                        GeoParquetMetadataMode.V2_0_ONLY,
                        FooterRewrite.geoMetadataRemoved()),
                Arguments.of(
                        "native boxes, a file without a bbox",
                        GeoParquetMetadataMode.V2_0_ONLY,
                        FooterRewrite.geoMetadataBboxRemoved("geometry")),
                Arguments.of(
                        "covering boxes, a file without a bbox",
                        GeoParquetMetadataMode.V1_1_ONLY,
                        FooterRewrite.geoMetadataBboxRemoved("geometry")));
    }

    @Test
    void noMatchIsEmpty() throws Exception {
        try (FilesetCatalog catalog = openCatalog()) {
            ParquetDataset dataset = onlyDataset(catalog);
            Predicate disjoint = Pred.col("geometry").bboxIntersects(Bbox.of2d(1_000.0, 1_000.0, 1_010.0, 1_010.0));

            Optional<BoundingBox> bounds = dataset.bounds(disjoint, ReadOptions.DEFAULTS);

            assertThat(bounds).isEmpty();
        }
    }

    // --- assertions ---

    private void assertFilteredMatchesOracle(ParquetDataset dataset, Predicate predicate, BoundingBox full) {
        Optional<BoundingBox> bounds = dataset.bounds(predicate, ReadOptions.DEFAULTS);
        Optional<BoundingBox> oracle = scanBounds(dataset, predicate);

        assertThat(oracle)
                .as("the predicate must clip a non-empty subset for a meaningful test")
                .isPresent();
        assertThat(bounds).isPresent();
        assertSameBox2d(bounds.orElseThrow(), oracle.orElseThrow());
        assertThat(boxArea(bounds.orElseThrow()))
                .as("a scan ignoring the predicate would return the full dataset extent")
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

    /** The exact bounding box of every {@code predicate}-matching row across the dataset, scanned from the WKB. */
    private static Optional<BoundingBox> scanBounds(ParquetDataset dataset, Predicate predicate) {
        BoundsAccumulator oracle = new BoundsAccumulator();
        try (Stream<ParquetRecordBatch> batches =
                dataset.readBatches(predicate, Projection.ofPhysical(List.of(GEOMETRY)), ReadOptions.DEFAULTS)) {
            batches.forEach(batch -> {
                foldBatchEnvelopes(batch, oracle);
                batch.close();
            });
        }
        return oracle.snapshot();
    }

    private static void foldBatchEnvelopes(ParquetRecordBatch batch, BoundsAccumulator oracle) {
        BinaryVector cells = (BinaryVector) batch.columns().get(GEOMETRY);
        for (int row = 0; row < batch.rowCount(); row++) {
            MemorySegment wkb = cells.get(row);
            if (wkb != null) {
                Bbox envelope = WkbEnvelope.compute(wkb);
                oracle.unionXy(envelope.minX(), envelope.minY(), envelope.maxX(), envelope.maxY());
            }
        }
    }

    // --- dataset construction ---

    private FilesetCatalog openCatalog() throws Exception {
        writeRegionTree();
        return FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults());
    }

    private static ParquetDataset onlyDataset(FilesetCatalog catalog) {
        return catalog.dataset(catalog.datasets().get(0));
    }

    private void writeRegionTree() throws Exception {
        for (int i = 0; i < REGIONS.size(); i++) {
            Path dir = Files.createDirectories(root.resolve("region=" + REGIONS.get(i)));
            Path file = dir.resolve("points.parquet");
            PointParquet.writePoints(file, "geometry", GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0, POINTS_PER_REGION[i]);
        }
    }

    /** Counts the row groups read by the queries observed through it. */
    private static final class RowGroupReadCounter implements QueryObserver {

        private final AtomicInteger reads = new AtomicInteger();

        @Override
        public void onRowGroupRead(RowGroupRead event) {
            reads.incrementAndGet();
        }

        int reads() {
            return reads.get();
        }
    }
}
