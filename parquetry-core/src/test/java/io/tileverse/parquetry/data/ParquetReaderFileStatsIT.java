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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.prune.ColumnStatistics;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;

class ParquetReaderFileStatsIT {

    @Test
    void fileStatsAggregatesAcrossTheFile(@TempDir Path dir) throws Exception {
        Path file = FileStatsFixtures.writeIntColumn(dir, "pop", new Long[] {10L, 30L, null, 20L});

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            FileStats stats = reader.fileStats();

            assertThat(stats.recordCount()).isEqualTo(4);
            ColumnStatistics pop = stats.columns().get(ColumnPath.of("pop"));
            assertThat(pop).isNotNull();
            assertThat(pop.min()).contains(new Value.LongVal(10));
            assertThat(pop.max()).contains(new Value.LongVal(30));
            assertThat(pop.nullCount()).hasValue(1);
            assertThat(stats.geometryBounds()).isEmpty();
        }
    }

    @Test
    void fileStatsExposesGeometryBounds(@TempDir Path dir) throws Exception {
        Path file = FileStatsFixtures.writePoints(dir, "geometry", new double[][] {{0, 0}, {10, 20}});

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            FileStats stats = reader.fileStats();

            assertThat(stats.geometryBounds()).containsKey(ColumnPath.of("geometry"));
            BoundingBox box = stats.geometryBounds().get(ColumnPath.of("geometry"));
            assertThat(box.xmin()).isEqualTo(0.0);
            assertThat(box.xmax()).isEqualTo(10.0);
            assertThat(box.ymin()).isEqualTo(0.0);
            assertThat(box.ymax()).isEqualTo(20.0);
            assertThat(stats.columns()).doesNotContainKey(ColumnPath.of("geometry"));
        }
    }
}
