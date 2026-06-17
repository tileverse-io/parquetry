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

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.dataset.DatasetCapabilities.FileSpatialBounds;
import io.tileverse.parquetry.dataset.DatasetCapabilities.FileStatsSource;
import io.tileverse.parquetry.dataset.DatasetCapabilities.PartitionModel;

class DatasetCapabilitiesTest {

    @Test
    void builderDefaultsAreSafeFalseAndNone() {
        DatasetCapabilities caps = DatasetCapabilities.builder().build();
        assertThat(caps.mergeOnRead()).isFalse();
        assertThat(caps.fieldIdResolved()).isFalse();
        assertThat(caps.fileStats()).isEqualTo(FileStatsSource.NONE);
        assertThat(caps.fileSpatialBounds()).isEqualTo(FileSpatialBounds.NONE);
        assertThat(caps.partitionModel()).isEqualTo(PartitionModel.NONE);
        assertThat(caps.cheapCount()).isFalse();
        assertThat(caps.cheapBounds()).isFalse();
    }

    @Test
    void requireMergeOnReadThrowsWhenAbsent() {
        DatasetCapabilities caps = DatasetCapabilities.builder().build();
        assertThatThrownBy(caps::requireMergeOnRead)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("mergeOnRead");
    }

    @Test
    void pureParquetPreset() {
        DatasetCapabilities caps = DatasetCapabilities.builder()
                .fileStats(FileStatsSource.FOOTER_AGGREGATE)
                .partitionModel(PartitionModel.HIVE_PATH)
                .cheapCount(true)
                .build();
        assertThat(caps.fileStats()).isEqualTo(FileStatsSource.FOOTER_AGGREGATE);
        assertThat(caps.partitionModel()).isEqualTo(PartitionModel.HIVE_PATH);
        assertThat(caps.cheapCount()).isTrue();
    }
}
