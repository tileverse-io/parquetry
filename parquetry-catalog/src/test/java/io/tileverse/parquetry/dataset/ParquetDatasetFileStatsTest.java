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

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.FlatLongParquet;

class ParquetDatasetFileStatsTest {

    @Test
    void singleFileDatasetExposesFooterStats(@TempDir Path dir) throws Exception {
        Path file = FlatLongParquet.writeIntFile(dir.resolve("a.parquet"), "pop", new long[] {10, 30, 20});
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetDataset dataset = ParquetDataset.open(source);

            FileStats stats = dataset.fileStats();

            assertThat(stats.recordCount()).isEqualTo(3);
            assertThat(stats.columns()).containsKey(ColumnPath.of("pop"));
        }
    }

    @Test
    void multiFileDatasetRejectsFileStats(@TempDir Path dir) throws Exception {
        Path a = FlatLongParquet.writeIntFile(dir.resolve("a.parquet"), "pop", new long[] {1, 2});
        Path b = FlatLongParquet.writeIntFile(dir.resolve("b.parquet"), "pop", new long[] {3, 4});
        try (ByteRangeSource sa = ByteRangeSource.ofFile(a);
                ByteRangeSource sb = ByteRangeSource.ofFile(b)) {
            FilesetReader fileset = TestFilesets.of(List.of(sa, sb));
            ParquetDataset dataset = ParquetDataset.open(fileset);

            assertThatThrownBy(dataset::fileStats).isInstanceOf(UnsupportedOperationException.class);
        }
    }
}
