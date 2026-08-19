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
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.schema.PrimitiveKind;

import io.tileverse.stac.StacAsset;
import io.tileverse.stac.StacCatalog;
import io.tileverse.stac.StacCollection;
import io.tileverse.stac.StacFormatException;
import io.tileverse.stac.StacItem;

/**
 * Reads item-tables written in the stac-geoparquet shape: a nested bbox struct, an assets struct with one sub-struct
 * per asset key, and the collection metadata the footer embeds.
 */
class GeoParquetStacReaderTest {

    private static StacItemTableParquet.Item item(String id, String collection, double x0, String href) {
        return new StacItemTableParquet.Item(
                id,
                collection,
                x0,
                0.0,
                x0 + 10.0,
                10.0,
                "2026-07-22T00:00:00Z",
                List.of(StacItemTableParquet.Asset.data(href)));
    }

    /** Opens the catalog and closes the storage: {@code open} reads every row before it returns. */
    private static StacCatalog open(Path dir, Path itemTable) throws Exception {
        try (Storage storage = StorageFactory.open(dir.toUri())) {
            return new GeoParquetStacReader().open(itemTable.toUri(), storage);
        }
    }

    @Test
    void mapsSpecRowsToTheModel(@TempDir Path dir) throws Exception {
        Path table = StacItemTableParquet.write(
                dir.resolve("items.parquet"),
                List.of(
                        item("west", "building", 0.0, "parts/west.parquet"),
                        item("east", "building", 100.0, "parts/east.parquet"),
                        item("division-0", "division", 50.0, "parts/div.parquet")));

        StacCatalog catalog = open(dir, table);

        assertThat(catalog.collections()).extracting(StacCollection::id).containsExactly("building", "division");
        StacItem west = catalog.collections().get(0).items().get(0);
        assertThat(west.id()).isEqualTo("west");
        assertThat(west.bbox()).containsExactly(0.0, 0.0, 10.0, 10.0);
        assertThat(west.datetime()).contains("2026-07-22T00:00:00Z");
        assertThat(west.assets()).singleElement().satisfies(asset -> {
            assertThat(asset.href())
                    .isEqualTo(dir.toUri().resolve("parts/west.parquet").toString());
            assertThat(asset.type()).isEqualTo("application/vnd.apache.parquet");
        });
    }

    @Test
    void absoluteHrefsPassThroughAndAlternatesAreIgnored(@TempDir Path dir) throws Exception {
        StacItemTableParquet.Asset withAlternate = new StacItemTableParquet.Asset(
                "data",
                "https://example.com/parts/west.parquet",
                "application/vnd.apache.parquet",
                null,
                List.of("data"),
                "s3://bucket/west.parquet");
        Path table = StacItemTableParquet.write(
                dir.resolve("items.parquet"),
                List.of(new StacItemTableParquet.Item(
                        "west", "building", 0.0, 0.0, 10.0, 10.0, null, List.of(withAlternate))));

        StacCatalog catalog = open(dir, table);

        List<StacAsset> assets = catalog.collections().get(0).items().get(0).assets();
        assertThat(assets).singleElement().satisfies(asset -> {
            assertThat(asset.href()).isEqualTo("https://example.com/parts/west.parquet");
            assertThat(asset.roles()).containsExactly("data");
        });
    }

    @Test
    void skipsAssetKeysWithNullHref(@TempDir Path dir) throws Exception {
        StacItemTableParquet.Asset mirror =
                new StacItemTableParquet.Asset("mirror", "parts/mirror.parquet", null, null, null, null);
        Path table = StacItemTableParquet.write(
                dir.resolve("items.parquet"),
                List.of(
                        new StacItemTableParquet.Item(
                                "a",
                                "c",
                                0.0,
                                0.0,
                                1.0,
                                1.0,
                                null,
                                List.of(StacItemTableParquet.Asset.data("parts/a.parquet"), mirror)),
                        new StacItemTableParquet.Item(
                                "b",
                                "c",
                                0.0,
                                0.0,
                                1.0,
                                1.0,
                                null,
                                List.of(StacItemTableParquet.Asset.data("parts/b.parquet")))));

        StacCatalog catalog = open(dir, table);

        List<StacItem> items = catalog.collections().get(0).items();
        assertThat(items.get(0).assets()).hasSize(2);
        assertThat(items.get(1).assets())
                .as("the mirror asset key is null on the second row")
                .hasSize(1);
    }

    @Test
    void readsFloat32BboxCornersAsDoubles(@TempDir Path dir) throws Exception {
        Path table = StacItemTableParquet.write(
                dir.resolve("items.parquet"),
                List.of(item("west", "building", 0.5, "parts/west.parquet")),
                StacItemTableParquet.Options.defaults().withBboxCornerKind(PrimitiveKind.FLOAT));

        StacCatalog catalog = open(dir, table);

        StacCollection building = catalog.collections().get(0);
        assertThat(building.items().get(0).bbox()).containsExactly(0.5, 0.0, 10.5, 10.0);
        assertThat(building.extent().orElseThrow().bbox().orElseThrow()).containsExactly(0.5, 0.0, 10.5, 10.0);
    }

    @Test
    void failsLoudOnANonNumericBboxCorner(@TempDir Path dir) throws Exception {
        Path table = StacItemTableParquet.write(
                dir.resolve("text-bbox.parquet"),
                List.of(item("a", "c", 0.0, "parts/a.parquet")),
                StacItemTableParquet.Options.defaults().withBboxCornerKind(PrimitiveKind.BYTE_ARRAY));

        try (Storage storage = StorageFactory.open(dir.toUri())) {
            GeoParquetStacReader reader = new GeoParquetStacReader();
            URI uri = table.toUri();
            assertThatThrownBy(() -> reader.open(uri, storage))
                    .isInstanceOf(StacFormatException.class)
                    .hasMessageContaining("text-bbox.parquet")
                    .hasMessageContaining("bbox.xmin");
        }
    }

    @Test
    void embeddedCollectionsMappingSuppliesTitleAndExtent(@TempDir Path dir) throws Exception {
        String kv = """
                {"version": "1.1.0", "collections": {
                  "building": {"id": "building", "title": "Buildings",
                    "extent": {"spatial": {"bbox": [[-10.0, -10.0, 20.0, 20.0]]}}},
                  "empty": {"id": "empty", "title": "No rows yet"}}}
                """;
        Path table = StacItemTableParquet.write(
                dir.resolve("items.parquet"),
                List.of(item("west", "building", 0.0, "parts/west.parquet")),
                StacItemTableParquet.Options.defaults().withKv(kv));

        StacCatalog catalog = open(dir, table);

        assertThat(catalog.collections()).extracting(StacCollection::id).containsExactly("building", "empty");
        StacCollection building = catalog.collections().get(0);
        assertThat(building.title()).isEqualTo("Buildings");
        assertThat(building.extent().orElseThrow().bbox().orElseThrow()).containsExactly(-10.0, -10.0, 20.0, 20.0);
        assertThat(catalog.collections().get(1).items()).isEmpty();
    }

    @Test
    void derivesExtentsByAggregatingItemBboxesWithoutEmbeddedMetadata(@TempDir Path dir) throws Exception {
        Path table = StacItemTableParquet.write(
                dir.resolve("items.parquet"),
                List.of(
                        item("west", "building", 0.0, "parts/west.parquet"),
                        item("east", "building", 100.0, "parts/east.parquet")),
                StacItemTableParquet.Options.defaults().withKv("{\"version\": \"1.0.0\"}"));

        StacCatalog catalog = open(dir, table);

        StacCollection building = catalog.collections().get(0);
        assertThat(building.title()).isNull();
        assertThat(building.extent().orElseThrow().bbox().orElseThrow()).containsExactly(0.0, 0.0, 110.0, 10.0);
    }

    @Test
    void missingCollectionColumnFallsBackToTheSingleEmbeddedId(@TempDir Path dir) throws Exception {
        String kv = "{\"collections\": {\"only\": {\"id\": \"only\"}}}";
        Path table = StacItemTableParquet.write(
                dir.resolve("items.parquet"),
                List.of(item("a", null, 0.0, "parts/a.parquet")),
                StacItemTableParquet.Options.defaults()
                        .withoutCollectionColumn()
                        .withKv(kv));

        StacCatalog catalog = open(dir, table);

        assertThat(catalog.collections()).extracting(StacCollection::id).containsExactly("only");
    }

    @Test
    void missingCollectionColumnWithoutMetadataUsesTheFileStem(@TempDir Path dir) throws Exception {
        Path table = StacItemTableParquet.write(
                dir.resolve("catalog-index.parquet"),
                List.of(item("a", null, 0.0, "parts/a.parquet")),
                StacItemTableParquet.Options.defaults().withoutCollectionColumn());

        StacCatalog catalog = open(dir, table);

        assertThat(catalog.collections()).extracting(StacCollection::id).containsExactly("catalog-index");
    }

    @Test
    void failsLoudOnMissingIdColumn(@TempDir Path dir) throws Exception {
        Path table = StacItemTableParquet.write(
                dir.resolve("items.parquet"),
                List.of(item("a", "c", 0.0, "parts/a.parquet")),
                StacItemTableParquet.Options.defaults().withoutIdColumn());

        try (Storage storage = StorageFactory.open(dir.toUri())) {
            GeoParquetStacReader reader = new GeoParquetStacReader();
            URI uri = table.toUri();
            assertThatThrownBy(() -> reader.open(uri, storage))
                    .isInstanceOf(StacFormatException.class)
                    .hasMessageContaining("id");
        }
    }

    @Test
    void failsLoudOnNullIdAndNullBbox(@TempDir Path dir) throws Exception {
        Path nullId = StacItemTableParquet.write(
                dir.resolve("null-id.parquet"), List.of(item(null, "c", 0.0, "parts/a.parquet")));
        Path nullBbox = StacItemTableParquet.write(
                dir.resolve("null-bbox.parquet"),
                List.of(new StacItemTableParquet.Item(
                        "a",
                        "c",
                        null,
                        null,
                        null,
                        null,
                        null,
                        List.of(StacItemTableParquet.Asset.data("parts/a.parquet")))));

        try (Storage storage = StorageFactory.open(dir.toUri())) {
            GeoParquetStacReader reader = new GeoParquetStacReader();
            URI idUri = nullId.toUri();
            URI bboxUri = nullBbox.toUri();
            assertThatThrownBy(() -> reader.open(idUri, storage)).isInstanceOf(StacFormatException.class);
            assertThatThrownBy(() -> reader.open(bboxUri, storage))
                    .isInstanceOf(StacFormatException.class)
                    .hasMessageContaining("bbox");
        }
    }

    @Test
    void aMalformedAssetHrefFailsLoudNamingTheItem(@TempDir Path dir) throws Exception {
        Path table = StacItemTableParquet.write(
                dir.resolve("items.parquet"), List.of(item("broken", "building", 0.0, "parts/a b.parquet")));

        try (Storage storage = StorageFactory.open(dir.toUri())) {
            GeoParquetStacReader reader = new GeoParquetStacReader();
            URI uri = table.toUri();
            assertThatThrownBy(() -> reader.open(uri, storage))
                    .isInstanceOf(StacFormatException.class)
                    .hasMessageContaining("broken");
        }
    }
}
