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

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.stac.StacCatalog;
import io.tileverse.stac.StacCollection;
import io.tileverse.stac.StacItem;

class GeoParquetStacReaderTest {

    @Test
    void mapsItemTableRowsToTheSameModel(@TempDir Path dir) throws Exception {
        Path itemTable = StacItemTableParquet.write(
                dir.resolve("items.parquet"),
                List.of(
                        new StacItemTableParquet.Row("west", "building", 0, 0, 10, 10, "parts/west.parquet"),
                        new StacItemTableParquet.Row("east", "building", 100, 0, 110, 10, "parts/east.parquet")));

        try (Storage storage = StorageFactory.open(dir.toUri())) {
            StacCatalog catalog = new GeoParquetStacReader().open(itemTable.toUri(), storage);

            List<StacCollection> collections = catalog.collections();
            assertThat(collections).extracting(StacCollection::id).containsExactly("building");

            List<StacItem> items = collections.get(0).items();
            assertThat(items).extracting(StacItem::id).containsExactlyInAnyOrder("west", "east");

            StacItem west = items.stream()
                    .filter(i -> i.id().equals("west"))
                    .findFirst()
                    .orElseThrow();
            assertThat(west.bbox()).containsExactly(0, 0, 10, 10);
            assertThat(west.assets()).anyMatch(a -> a.href().endsWith("west.parquet"));
        }
    }
}
