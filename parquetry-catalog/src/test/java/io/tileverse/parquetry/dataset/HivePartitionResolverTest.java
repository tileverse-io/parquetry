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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.FileEntry;

class HivePartitionResolverTest {

    private static FileEntry entry(String relativePath) {
        return new FileEntry() {
            @Override
            public String relativePath() {
                return relativePath;
            }

            @Override
            public long sizeBytes() {
                return -1L;
            }

            @Override
            public ByteRangeSource open() {
                throw new UnsupportedOperationException("not opened in discovery tests");
            }
        };
    }

    @Test
    void layerPerFileForNonPartitionedSiblings() {
        List<DatasetUnit> units = HivePartitionResolver.resolve(
                List.of(entry("address.parquet"), entry("building.parquet"), entry("place.parquet")), null);
        assertThat(units).extracting(DatasetUnit::name).containsExactlyInAnyOrder("address", "building", "place");
        assertThat(units).allSatisfy(unit -> {
            assertThat(unit.files()).hasSize(1);
            assertThat(unit.partitionValues()).isEmpty();
        });
    }

    @Test
    void fragmentsAcrossOneHiveLeafGroupTogether() {
        List<DatasetUnit> units = HivePartitionResolver.resolve(
                List.of(
                        entry("theme=buildings/type=building/part-0.parquet"),
                        entry("theme=buildings/type=building/part-1.parquet"),
                        entry("theme=buildings/type=building_part/part-0.parquet")),
                null);
        assertThat(units)
                .extracting(DatasetUnit::name)
                .containsExactlyInAnyOrder("buildings_building", "buildings_building_part");
        DatasetUnit building = units.stream()
                .filter(u -> u.name().equals("buildings_building"))
                .findFirst()
                .orElseThrow();
        assertThat(building.files()).hasSize(2);
        assertThat(building.partitionValues())
                .containsEntry("theme", "buildings")
                .containsEntry("type", "building");
    }

    @Test
    void maxDepthOneFoldsDeeperLevels() {
        List<DatasetUnit> units = HivePartitionResolver.resolve(
                List.of(
                        entry("theme=buildings/type=building/part-0.parquet"),
                        entry("theme=buildings/type=building_part/part-0.parquet")),
                1);
        assertThat(units).extracting(DatasetUnit::name).containsExactly("buildings");
        assertThat(units.get(0).files()).hasSize(2);
        assertThat(units.get(0).partitionValues())
                .containsExactly(org.assertj.core.api.Assertions.entry("theme", "buildings"));
    }

    @Test
    void maxDepthZeroFoldsAllPartitionLevelsIntoOneUnit() {
        List<DatasetUnit> units = HivePartitionResolver.resolve(
                List.of(
                        entry("theme=buildings/type=building/part-0.parquet"),
                        entry("theme=transportation/type=segment/part-0.parquet")),
                0);
        assertThat(units).hasSize(1);
        assertThat(units.get(0).files()).hasSize(2);
        assertThat(units.get(0).partitionValues()).isEmpty();
    }

    @Test
    void maxDepthZeroFoldsDistinctStemsIntoOneUnit() {
        List<DatasetUnit> units = HivePartitionResolver.resolve(
                List.of(
                        entry("theme=buildings/type=building/part-0.parquet"),
                        entry("theme=transportation/type=segment/part-1.parquet")),
                0);
        assertThat(units).hasSize(1);
        assertThat(units.get(0).files()).hasSize(2);
        assertThat(units.get(0).partitionValues()).isEmpty();
    }

    @Test
    void nestedSameStemDifferentSubdirsAreDistinctDatasets() {
        List<DatasetUnit> units =
                HivePartitionResolver.resolve(List.of(entry("a/data.parquet"), entry("b/data.parquet")), null);
        assertThat(units).extracting(DatasetUnit::name).containsExactlyInAnyOrder("a", "b");
        assertThat(units).allSatisfy(unit -> {
            assertThat(unit.files()).hasSize(1);
            assertThat(unit.partitionValues()).isEmpty();
        });
    }

    @Test
    void subdirOfFragmentsIsOneDataset() {
        List<DatasetUnit> units =
                HivePartitionResolver.resolve(List.of(entry("bar/part-0.parquet"), entry("bar/part-1.parquet")), null);
        assertThat(units).extracting(DatasetUnit::name).containsExactly("bar");
        assertThat(units.get(0).files()).hasSize(2);
        assertThat(units.get(0).partitionValues()).isEmpty();
    }

    @Test
    void distinctSeparatorValuesStayDistinct() {
        List<DatasetUnit> units =
                HivePartitionResolver.resolve(List.of(entry("k=a--b/f.parquet"), entry("k=a__b/f.parquet")), null);
        assertThat(units).hasSize(2);
        List<String> names = units.stream().map(DatasetUnit::name).toList();
        assertThat(names.get(0)).isNotEqualTo(names.get(1));
    }

    @Test
    void emptyValueHiveSegmentIsNotPartition() {
        List<DatasetUnit> units = HivePartitionResolver.resolve(List.of(entry("theme=/f.parquet")), null);
        assertThat(units).hasSize(1);
        assertThat(units.get(0).partitionValues()).isEmpty();
    }

    @Test
    void negativeMaxDepthRejected() {
        List<FileEntry> files = List.of(entry("a.parquet"));
        assertThatThrownBy(() -> HivePartitionResolver.resolve(files, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxHiveDepth");
    }
}
