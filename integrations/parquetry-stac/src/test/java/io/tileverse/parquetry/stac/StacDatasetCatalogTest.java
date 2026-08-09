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
package io.tileverse.parquetry.stac;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
    void exposesEveryCollectionAndFailsAParquetlessOneAtResolution(@TempDir Path tempDir) throws Exception {
        Path parts = Files.createDirectories(tempDir.resolve("parts"));
        Path parquetPart = parts.resolve("data.parquet");
        StacPointParquet.writePoints(parquetPart, "geometry", new double[][] {{0, 0}, {5, 5}});

        StacCollection withParquet = collectionWithItem(
                "buildings", stacItem("b1", new double[] {0, 0, 5, 5}, parquetAsset(parquetPart.toUri())));
        StacCollection withoutParquet =
                collectionWithItem("basemap-tiles", stacItem("t1", new double[] {0, 0, 5, 5}, pmtilesAsset()));

        URI catalogRoot = tempDir.resolve("catalog.json").toUri();
        try (ContainerStorages storages = new ContainerStorages(new Properties());
                StacDatasetCatalog catalog = StacDatasetCatalog.open(
                        catalogRoot,
                        storages,
                        (root, store) -> twoCollectionCatalog(withParquet, withoutParquet),
                        StacCatalogOptions.defaults())) {

            assertThat(catalog.datasets()).containsExactly("buildings", "basemap-tiles");

            try (Stream<ParquetRecord> rows =
                    catalog.dataset("buildings").read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                assertThat(rows.count()).isEqualTo(2);
            }

            assertThatThrownBy(() -> catalog.dataset("basemap-tiles"))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("basemap-tiles")
                    .hasMessageContaining("no GeoParquet");
        }
    }

    @Test
    void itemEnumerationDeferredToDatasetResolution(@TempDir Path tempDir) throws Exception {
        Path parts = Files.createDirectories(tempDir.resolve("parts"));
        Path parquetPart = parts.resolve("data.parquet");
        StacPointParquet.writePoints(parquetPart, "geometry", new double[][] {{0, 0}, {5, 5}});

        AtomicInteger itemEnumerations = new AtomicInteger();
        StacItem item = stacItem("b1", new double[] {0, 0, 5, 5}, parquetAsset(parquetPart.toUri()));
        StacCollection collection = new StacCollection("buildings", null, Optional.empty(), List.of(), () -> {
            itemEnumerations.incrementAndGet();
            return List.of(item);
        });

        URI catalogRoot = tempDir.resolve("catalog.json").toUri();
        try (ContainerStorages storages = new ContainerStorages(new Properties());
                StacDatasetCatalog catalog = StacDatasetCatalog.open(
                        catalogRoot,
                        storages,
                        (root, store) -> new StacCatalog("root", null, List.of(), () -> List.of(collection), List::of),
                        StacCatalogOptions.defaults())) {

            assertThat(catalog.datasets()).containsExactly("buildings");
            assertThat(itemEnumerations).hasValue(0);

            ParquetDataset first = catalog.dataset("buildings");
            assertThat(itemEnumerations).hasValue(1);

            assertThat(catalog.dataset("buildings")).isSameAs(first);
            assertThat(itemEnumerations).hasValue(1);
        }
    }

    @Test
    void opensNoAssetIoAtRegistration(@TempDir Path tempDir) throws Exception {
        Files.createDirectories(tempDir.resolve("data"));
        // The collection points at a part that does not exist. Registration must still succeed: nothing opens it.
        StacCollection collection = collectionWithItem(
                "buildings",
                stacItem(
                        "b1",
                        new double[] {0, 0, 5, 5},
                        parquetAsset(tempDir.resolve("data/missing.parquet").toUri())));
        URI catalogRoot = tempDir.resolve("catalog.json").toUri();

        try (ContainerStorages storages = new ContainerStorages(new Properties());
                StacDatasetCatalog catalog = StacDatasetCatalog.open(
                        catalogRoot,
                        storages,
                        (root, store) -> new StacCatalog("root", null, List.of(), () -> List.of(collection), List::of),
                        StacCatalogOptions.defaults())) {

            assertThat(catalog.datasets()).containsExactly("buildings");
        }
    }

    @Test
    void resolvesAssetsOnADifferentContainerThanTheCatalog(@TempDir Path tempDir) throws Exception {
        // The catalog lives under catalog/, the parquet part under data/ - different containers, absolute file hrefs.
        Path catalogDir = Files.createDirectories(tempDir.resolve("catalog"));
        Path dataDir = Files.createDirectories(tempDir.resolve("data"));
        Path part = dataDir.resolve("buildings.parquet");
        StacPointParquet.writePoints(part, "geometry", new double[][] {{0, 0}, {5, 5}});

        StacCollection collection =
                collectionWithItem("buildings", stacItem("b1", new double[] {0, 0, 5, 5}, parquetAsset(part.toUri())));
        URI catalogRoot = catalogDir.resolve("catalog.json").toUri();

        try (ContainerStorages storages = new ContainerStorages(new Properties());
                StacDatasetCatalog catalog = StacDatasetCatalog.open(
                        catalogRoot,
                        storages,
                        (root, store) -> new StacCatalog("root", null, List.of(), () -> List.of(collection), List::of),
                        StacCatalogOptions.defaults())) {

            assertThat(catalog.datasets()).containsExactly("buildings");
            try (Stream<ParquetRecord> rows =
                    catalog.dataset("buildings").read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                assertThat(rows.count()).isEqualTo(2);
            }
        }
    }

    @Test
    void latestPropertyRestrictsTheWalkToThatRelease(@TempDir Path tempDir) throws Exception {
        Path parts = Files.createDirectories(tempDir.resolve("parts"));
        Path parquetPart = parts.resolve("data.parquet");
        StacPointParquet.writePoints(parquetPart, "geometry", new double[][] {{0, 0}, {5, 5}});

        AtomicInteger oldReleaseWalks = new AtomicInteger();
        StacCollection currentBuildings = collectionWithItem(
                "buildings", stacItem("b1", new double[] {0, 0, 5, 5}, parquetAsset(parquetPart.toUri())));
        StacCatalog oldRelease = new StacCatalog(
                "2026-06-17.0",
                null,
                List.of(),
                () -> {
                    oldReleaseWalks.incrementAndGet();
                    return List.of();
                },
                List::of);
        StacCatalog newRelease =
                new StacCatalog("2026-07-22.0", null, List.of(), () -> List.of(currentBuildings), List::of);
        StacCatalog root = new StacCatalog(
                "releases", null, "2026-07-22.0", List.of(), List::of, () -> List.of(oldRelease, newRelease));

        URI catalogRoot = tempDir.resolve("catalog.json").toUri();
        try (ContainerStorages storages = new ContainerStorages(new Properties());
                StacDatasetCatalog catalog =
                        StacDatasetCatalog.open(catalogRoot, storages, (r, s) -> root, StacCatalogOptions.defaults())) {

            assertThat(catalog.datasets()).containsExactly("buildings");
            assertThat(oldReleaseWalks).hasValue(0);

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

        try (ContainerStorages storages = new ContainerStorages(new Properties());
                StacDatasetCatalog catalog = StacDatasetCatalog.open(
                        root.resolve("catalog.json").toUri(),
                        storages,
                        new JsonStacReader(),
                        StacCatalogOptions.defaults())) {

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
