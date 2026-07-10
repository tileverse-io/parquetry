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

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.ColumnPath;

import io.tileverse.stac.StacAsset;
import io.tileverse.stac.StacItem;

class StacFileStatsTest {

    @Test
    void mapsItemBboxToGeometryBoundsXPairFirst() {
        StacItem item = new StacItem("i", new double[] {1, 2, 3, 4}, Optional.empty(), List.of(), List.of());

        FileStats stats = StacFileStats.from(item, "geometry");

        assertThat(stats.recordCount()).isEqualTo(-1L);
        BoundingBox box = stats.geometryBounds().get(ColumnPath.of("geometry"));
        assertThat(box).isNotNull();
        assertThat(box.xmin()).isEqualTo(1);
        assertThat(box.ymin()).isEqualTo(2);
        assertThat(box.xmax()).isEqualTo(3);
        assertThat(box.ymax()).isEqualTo(4);
    }

    @Test
    void missingBboxContributesNoBound() {
        StacAsset data = new StacAsset("part.parquet", "application/vnd.apache.parquet", "data", List.of("data"));
        StacItem item = new StacItem("i", null, Optional.empty(), List.of(data), List.of());

        FileStats stats = StacFileStats.from(item, "geometry");

        assertThat(stats.geometryBounds()).isEmpty();
        assertThat(stats.recordCount()).isEqualTo(-1L);
    }

    @Test
    void threeDimensionalBboxKeepsXyAndReportsZ() {
        StacItem item = new StacItem("i", new double[] {1, 2, 10, 3, 4, 20}, Optional.empty(), List.of(), List.of());

        FileStats stats = StacFileStats.from(item, "geometry");

        BoundingBox box = stats.geometryBounds().get(ColumnPath.of("geometry"));
        assertThat(box.xmin()).isEqualTo(1);
        assertThat(box.ymin()).isEqualTo(2);
        assertThat(box.xmax()).isEqualTo(3);
        assertThat(box.ymax()).isEqualTo(4);
        assertThat(box.zmin()).hasValue(10);
        assertThat(box.zmax()).hasValue(20);
    }
}
