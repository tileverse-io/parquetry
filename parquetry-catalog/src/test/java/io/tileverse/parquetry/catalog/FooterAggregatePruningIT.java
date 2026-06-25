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
package io.tileverse.parquetry.catalog;

import static io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0;
import static io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode.V1_1_ONLY;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.dataset.explain.FileExplain;
import io.tileverse.parquetry.dataset.explain.Outcome;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.io.LocalFileSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.FlatLongParquet;
import io.tileverse.parquetry.testsupport.PointParquet;

/**
 * Whole-file pruning over a flat (non-Hive) directory: footer column statistics let a predicate skip a file whose value
 * range cannot match, without opening it. The fixtures are written by the parquetry columnar writer, which emits the
 * column statistics this pruning reads; the oracle is the by-construction matching count plus an explicit skip count.
 *
 * <p>The plan also called for a missing-statistics safety scenario (a column with no statistics must never prune). That
 * cannot be built end to end here: {@link FilesetCatalog} requires schema-equality across files and the parquetry
 * writer always emits column statistics, leaving no way to pair a stats-less file with a stats-bearing same-schema file
 * through the writer. The property is core-tested instead by {@code FooterStatsAggregatorTest}: any row group missing a
 * statistic empties the aggregate, and {@code FilePruner} cannot eliminate on an empty aggregate.
 */
class FooterAggregatePruningIT {

    // Covers the east point cluster (x 100..110); disjoint from the west cluster (x 0..10).
    private static final Bbox EAST_QUERY = Bbox.of2d(95, -5, 115, 15);

    @Test
    void flatDirectoryPrunesByFooterStats(@TempDir Path dir) throws Exception {
        FlatLongParquet.writeIntFile(dir.resolve("a.parquet"), "pop", FlatLongParquet.longRange(0, 10)); // 0..9
        FlatLongParquet.writeIntFile(dir.resolve("b.parquet"), "pop", FlatLongParquet.longRange(100, 10)); // 100..109

        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(dir, "*.parquet"), CatalogOptions.defaults())) {
            ParquetDataset dataset = catalog.dataset(catalog.datasets().get(0));
            Predicate onlyHigh = new Predicate.GtEq(ColumnPath.of("pop"), new Value.LongVal(100));

            DatasetExplainPlan plan = dataset.explain(onlyHigh, Projection.ALL, ReadOptions.DEFAULTS);
            assertThat(skipCount(plan)).isEqualTo(1);

            assertThat(dataset.count(onlyHigh, ReadOptions.DEFAULTS)).isEqualTo(10);
        }
    }

    @Test
    void bboxPredicateSkipsSpatiallyDisjointFilesNative(@TempDir Path dir) throws Exception {
        PointParquet.writePoints(
                dir.resolve("west.parquet"), "geometry", DUAL_V1_1_AND_V2_0, new double[][] {{0, 0}, {5, 5}, {10, 10}});
        PointParquet.writePoints(dir.resolve("east.parquet"), "geometry", DUAL_V1_1_AND_V2_0, new double[][] {
            {100, 0}, {105, 5}, {110, 10}
        });

        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(dir, "*.parquet"), CatalogOptions.defaults())) {
            ParquetDataset dataset = catalog.dataset(catalog.datasets().get(0));
            Predicate eastOnly = new Predicate.Spatial.BboxIntersects(ColumnPath.of("geometry"), EAST_QUERY);

            DatasetExplainPlan plan = dataset.explain(eastOnly, Projection.ALL, ReadOptions.DEFAULTS);
            assertThat(skipCount(plan)).isEqualTo(1);
            assertThat(dataset.count(eastOnly, ReadOptions.DEFAULTS)).isEqualTo(3);
        }
    }

    /**
     * Pins the current behavior of the GeoParquet 1.1-only encoding: it does NOT prune files spatially because the
     * parquetry writer omits the file-level bbox in that mode. In {@code V1_1_ONLY} the writer leaves the geometry
     * column without a {@code LogicalType.Geometry} (to avoid misleading a v1-only reader into reading the file as v2
     * native), which means no per-chunk geospatial statistics are accumulated and the {@code "geo"} JSON {@code bbox}
     * stays absent. {@code GeoJsonFileBoundsSource} then has no bounds to read and {@code FilePruner} cannot eliminate
     * the disjoint file. The query result is still correct (record-level evaluation matches the east points only); only
     * the whole-file skip is unavailable. Native (GeoParquet 2.0) statistics, exercised by
     * {@link #bboxPredicateSkipsSpatiallyDisjointFilesNative}, are the encoding that drives file-level spatial pruning.
     */
    @Test
    void bboxOverGeoParquet11DoesNotPruneButStillMatches(@TempDir Path dir) throws Exception {
        PointParquet.writePoints(
                dir.resolve("west.parquet"), "geometry", V1_1_ONLY, new double[][] {{0, 0}, {5, 5}, {10, 10}});
        PointParquet.writePoints(
                dir.resolve("east.parquet"), "geometry", V1_1_ONLY, new double[][] {{100, 0}, {105, 5}, {110, 10}});

        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(dir, "*.parquet"), CatalogOptions.defaults())) {
            ParquetDataset dataset = catalog.dataset(catalog.datasets().get(0));
            Predicate eastOnly = new Predicate.Spatial.BboxIntersects(ColumnPath.of("geometry"), EAST_QUERY);

            DatasetExplainPlan plan = dataset.explain(eastOnly, Projection.ALL, ReadOptions.DEFAULTS);
            assertThat(skipCount(plan)).isZero();
            assertThat(dataset.count(eastOnly, ReadOptions.DEFAULTS)).isEqualTo(3);
        }
    }

    @Test
    void isNotNullSkipsAllNullFiles(@TempDir Path dir) throws Exception {
        FlatLongParquet.writeNullableIntFile(dir.resolve("allnull.parquet"), "c", new Long[] {null, null, null, null});
        FlatLongParquet.writeNullableIntFile(dir.resolve("present.parquet"), "c", new Long[] {1L, 2L, 3L});

        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(dir, "*.parquet"), CatalogOptions.defaults())) {
            ParquetDataset dataset = catalog.dataset(catalog.datasets().get(0));
            Predicate present = new Predicate.IsNotNull(ColumnPath.of("c"));

            DatasetExplainPlan plan = dataset.explain(present, Projection.ALL, ReadOptions.DEFAULTS);
            assertThat(skipCount(plan)).isEqualTo(1);
            assertThat(dataset.count(present, ReadOptions.DEFAULTS)).isEqualTo(3);
        }
    }

    @Test
    void isNullSkipsAllNonNullFiles(@TempDir Path dir) throws Exception {
        FlatLongParquet.writeNullableIntFile(dir.resolve("allnull.parquet"), "c", new Long[] {null, null, null, null});
        FlatLongParquet.writeNullableIntFile(dir.resolve("present.parquet"), "c", new Long[] {1L, 2L, 3L});

        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(dir, "*.parquet"), CatalogOptions.defaults())) {
            ParquetDataset dataset = catalog.dataset(catalog.datasets().get(0));
            Predicate absent = new Predicate.IsNull(ColumnPath.of("c"));

            DatasetExplainPlan plan = dataset.explain(absent, Projection.ALL, ReadOptions.DEFAULTS);
            assertThat(skipCount(plan)).isEqualTo(1);
            assertThat(dataset.count(absent, ReadOptions.DEFAULTS)).isEqualTo(4);
        }
    }

    private static long skipCount(DatasetExplainPlan plan) {
        return plan.files().stream()
                .map(FileExplain::outcome)
                .filter(Outcome.SKIP::equals)
                .count();
    }
}
