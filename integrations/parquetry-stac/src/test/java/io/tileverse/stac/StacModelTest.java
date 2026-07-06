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
package io.tileverse.stac;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

class StacModelTest {

    @Test
    void assetRetainsHrefTypeAndRoles() {
        StacAsset asset = new StacAsset(
                "s3://bucket/building.parquet", "application/vnd.apache.parquet", "Buildings", List.of("data"));
        assertThat(asset.href()).isEqualTo("s3://bucket/building.parquet");
        assertThat(asset.type()).isEqualTo("application/vnd.apache.parquet");
        assertThat(asset.roles()).containsExactly("data");
    }

    @Test
    void linkRetainsNonStandardRel() {
        StacLink pmtiles = new StacLink("pmtiles", "s3://bucket/theme.pmtiles", "application/vnd.pmtiles", "Tiles");
        assertThat(pmtiles.rel()).isEqualTo("pmtiles");
        assertThat(pmtiles.href()).isEqualTo("s3://bucket/theme.pmtiles");
    }

    @Test
    void itemKeepsBboxAssetsAndLinks() {
        StacAsset data = new StacAsset("part-0.parquet", "application/vnd.apache.parquet", "data", List.of("data"));
        StacItem item = new StacItem(
                "item-1", new double[] {0, 0, 10, 10}, Optional.of("2024-01-01T00:00:00Z"), List.of(data), List.of());
        assertThat(item.bbox()).containsExactly(0, 0, 10, 10);
        assertThat(item.assets()).hasSize(1);
        assertThat(item.datetime()).contains("2024-01-01T00:00:00Z");
    }

    @Test
    void collectionExposesExtentAndItemSupplier() {
        StacExtent extent = new StacExtent(Optional.of(new double[] {0, 0, 10, 10}), Optional.empty());
        StacItem item = new StacItem("i", new double[] {1, 1, 2, 2}, Optional.empty(), List.of(), List.of());
        StacCollection collection =
                new StacCollection("building", "Buildings", Optional.of(extent), List.of(), () -> List.of(item));
        assertThat(collection.id()).isEqualTo("building");
        assertThat(collection.items()).containsExactly(item);
        assertThat(collection.extent()).isPresent();
    }

    @Test
    void catalogRetainsChildTreeLazily() {
        StacCollection collection = new StacCollection("place", "Places", Optional.empty(), List.of(), List::of);
        StacCatalog catalog = new StacCatalog("root", "Overture", List.of(), () -> List.of(collection), List::of);
        assertThat(catalog.id()).isEqualTo("root");
        assertThat(catalog.collections()).containsExactly(collection);
        assertThat(catalog.childCatalogs()).isEmpty();
    }
}
