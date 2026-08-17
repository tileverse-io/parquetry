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

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

import io.tileverse.stac.StacAsset;
import io.tileverse.stac.StacCatalog;
import io.tileverse.stac.StacCollection;
import io.tileverse.stac.StacItem;

/**
 * Reads Overture Maps' published {@code collections.parquet} as an oracle for the stac-geoparquet specification: a real
 * item-table written by another implementation, vendored under {@code src/test/resources/stac/overture} (see the README
 * there for provenance).
 */
class OvertureCollectionsParquetIT {

    private static final String RESOURCE = "/stac/overture/collections.parquet";

    static Path copyVendoredFile(Path dir) throws IOException {
        Path target = dir.resolve("collections.parquet");
        try (InputStream in = OvertureCollectionsParquetIT.class.getResourceAsStream(RESOURCE)) {
            assertThat(in).as("vendored resource " + RESOURCE).isNotNull();
            Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
        }
        return target;
    }

    @Test
    void vendoredFileIsIntactAndSpecShaped(@TempDir Path dir) throws IOException {
        Path file = copyVendoredFile(dir);
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            ParquetSource source = ParquetSource.open(channel);
            assertThat(source.schema().find(ColumnPath.of("id"))).isPresent();
            assertThat(source.schema().find(ColumnPath.of("bbox", "xmin"))).isPresent();
            assertThat(source.schema().find(ColumnPath.of("assets"))).isPresent();
            assertThat(source.keyValueMetadata()).containsKey("stac-geoparquet");
            try (Stream<ParquetRecord> rows = source.read()) {
                assertThat(rows.count()).isEqualTo(974L);
            }
        }
    }

    @Test
    void readsOverturesOfficialCollectionsParquet(@TempDir Path dir) throws Exception {
        Path file = copyVendoredFile(dir);
        try (Storage storage = StorageFactory.open(dir.toUri())) {
            StacCatalog catalog = new GeoParquetStacReader().open(file.toUri(), storage);
            List<StacCollection> collections = catalog.collections();
            assertThat(collections).hasSize(15);
            assertThat(collections.stream()
                            .mapToInt(collection -> collection.items().size())
                            .sum())
                    .isEqualTo(974);

            StacCollection building = collections.stream()
                    .filter(collection -> collection.id().equals("building"))
                    .findFirst()
                    .orElseThrow();
            assertThat(building.items()).hasSize(512);
            assertThat(building.title())
                    .as("no embedded collections mapping in the Overture file")
                    .isNull();
            assertThat(building.extent().orElseThrow().bbox().orElseThrow())
                    .containsExactly(-180.0, -84.29460906982422, 179.9996795654297, 83.09400939941406);

            StacCollection division = collections.stream()
                    .filter(collection -> collection.id().equals("division"))
                    .findFirst()
                    .orElseThrow();
            StacItem first = division.items().stream()
                    .filter(item -> item.id().equals("00000"))
                    .findFirst()
                    .orElseThrow();
            assertThat(first.datetime()).contains("2026-07-22T00:00:00Z");
            assertThat(first.assets())
                    .extracting(StacAsset::href)
                    .contains(
                            "https://overturemaps-us-west-2.s3.us-west-2.amazonaws.com/release/2026-07-22.0/theme=divisions/type=division/part-00000-3d9695ce-2282-56c2-9d25-622b1ec4727f-c000.zstd.parquet");
        }
    }
}
