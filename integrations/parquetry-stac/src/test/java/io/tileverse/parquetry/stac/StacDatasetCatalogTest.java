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

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

import io.tileverse.stac.JsonStacReader;
import io.tileverse.stac.StacAsset;
import io.tileverse.stac.StacCatalog;
import io.tileverse.stac.StacCollection;
import io.tileverse.stac.StacItem;

class StacDatasetCatalogTest {

    @Test
    void skipsCollectionsWithNoParquetPartsAndExposesTheRest(@TempDir Path tempDir) throws Exception {
        Path parts = Files.createDirectories(tempDir.resolve("parts"));
        Path parquetPart = parts.resolve("data.parquet");
        StacPointParquet.writePoints(parquetPart, "geometry", new double[][] {{0, 0}, {5, 5}});

        StacCollection withParquet = collectionWithItem(
                "buildings", stacItem("b1", new double[] {0, 0, 5, 5}, parquetAsset(parquetPart.toUri())));
        StacCollection withoutParquet =
                collectionWithItem("basemap-tiles", stacItem("t1", new double[] {0, 0, 5, 5}, pmtilesAsset()));

        URI catalogRoot = tempDir.resolve("catalog.json").toUri();
        Storage storage = StorageFactory.open(tempDir.toUri());
        try (StacDatasetCatalog catalog = StacDatasetCatalog.open(
                catalogRoot,
                storage,
                (root, store) -> twoCollectionCatalog(withParquet, withoutParquet),
                StacCatalogOptions.defaults())) {

            assertThat(catalog.datasets()).containsExactly("buildings");

            try (Stream<ParquetRecord> rows =
                    catalog.dataset("buildings").read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                assertThat(rows.count()).isEqualTo(2);
            }
        }
    }

    private static StacCatalog twoCollectionCatalog(StacCollection first, StacCollection second) {
        return new StacCatalog("root", null, List.of(), () -> List.of(first, second), List::of);
    }

    private static StacCollection collectionWithItem(String id, StacItem item) {
        return new StacCollection(id, null, Optional.empty(), List.of(), () -> List.of(item));
    }

    private static StacItem stacItem(String id, double[] bbox, StacAsset asset) {
        return new StacItem(id, bbox, Optional.empty(), List.of(asset), List.of());
    }

    private static StacAsset parquetAsset(URI href) {
        return new StacAsset(href.toString(), "application/vnd.apache.parquet", null, List.of("data"));
    }

    private static StacAsset pmtilesAsset() {
        return new StacAsset("tiles.pmtiles", "application/vnd.pmtiles", null, List.of("data"));
    }

    @Test
    void exposesOneDatasetPerCollectionAndPrunesByItemBbox(@TempDir Path tempDir) throws Exception {
        Path root = copyFixtureTree(tempDir);
        // The fixture items point at ../parts/west.parquet and ../parts/east.parquet; write them with disjoint points.
        Path parts = Files.createDirectories(root.resolve("building/parts"));
        StacPointParquet.writePoints(parts.resolve("west.parquet"), "geometry", new double[][] {{0, 0}, {5, 5}});
        StacPointParquet.writePoints(parts.resolve("east.parquet"), "geometry", new double[][] {{100, 0}, {105, 5}});

        Storage storage = StorageFactory.open(root.toUri());
        try (StacDatasetCatalog catalog = StacDatasetCatalog.open(
                root.resolve("catalog.json").toUri(), storage, new JsonStacReader(), StacCatalogOptions.defaults())) {

            assertThat(catalog.datasets()).containsExactly("building");
            assertThat(catalog.capabilities().enumeratesDatasets()).isTrue();

            ParquetDataset building = catalog.dataset("building");
            Predicate eastOnly =
                    new Predicate.Spatial.BboxIntersects(ColumnPath.of("geometry"), Bbox.of2d(95, -5, 115, 15));

            DatasetExplainPlan plan = building.explain(eastOnly, Projection.ALL, ReadOptions.DEFAULTS);
            assertThat(plan.totals().filesSkipped()).isEqualTo(1);

            try (Stream<ParquetRecord> rows =
                    building.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                assertThat(rows.count()).isEqualTo(4);
            }
            try (Stream<ParquetRecord> rows = building.read(eastOnly, Projection.ALL, ReadOptions.DEFAULTS)) {
                assertThat(rows.count()).isEqualTo(2);
            }
        }
    }

    private static Path copyFixtureTree(Path tempDir) throws Exception {
        Path source = locateFixtureRoot();
        try (Stream<Path> walk = Files.walk(source)) {
            walk.forEach(path -> copyInto(source, path, tempDir));
        }
        return tempDir;
    }

    private static Path locateFixtureRoot() {
        java.net.URL url = StacDatasetCatalogTest.class.getClassLoader().getResource("stac/overture-mini/catalog.json");
        if (url == null) {
            throw new IllegalStateException("fixture stac/overture-mini/catalog.json not on the classpath");
        }
        return Path.of(URI.create(url.toString())).getParent();
    }

    private static void copyInto(Path source, Path path, Path destRoot) {
        try {
            Path relative = source.relativize(path);
            Path target = destRoot.resolve(relative.toString());
            if (Files.isDirectory(path)) {
                Files.createDirectories(target);
            } else {
                Files.createDirectories(target.getParent());
                Files.copy(path, target);
            }
        } catch (Exception copyFailure) {
            throw new IllegalStateException("copying fixture " + path, copyFailure);
        }
    }
}
