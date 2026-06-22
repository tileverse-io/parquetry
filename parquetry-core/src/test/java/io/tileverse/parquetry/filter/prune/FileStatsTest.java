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
package io.tileverse.parquetry.filter.prune;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;

class FileStatsTest {

    private static ColumnStatistics exact(long value) {
        return new ColumnStatistics(
                Optional.of(new Value.LongVal(value)), Optional.of(new Value.LongVal(value)), OptionalLong.of(0));
    }

    private static ColumnStatistics range(long min, long max, long nulls) {
        return new ColumnStatistics(
                Optional.of(new Value.LongVal(min)), Optional.of(new Value.LongVal(max)), OptionalLong.of(nulls));
    }

    @Test
    void withOverridesUnionsDisjointColumnsAndKeepsTheReceiverRecordCount() {
        FileStats footer = FileStats.builder()
                .recordCount(100)
                .column(ColumnPath.of("pop"), range(1, 9, 2))
                .build();
        FileStats partition = FileStats.builder()
                .recordCount(7)
                .column(ColumnPath.of("year"), exact(2024))
                .build();

        FileStats merged = footer.withOverrides(partition);

        assertThat(merged.recordCount()).isEqualTo(100);
        assertThat(merged.columns()).containsOnlyKeys(ColumnPath.of("pop"), ColumnPath.of("year"));
        assertThat(merged.columns()).containsEntry(ColumnPath.of("pop"), range(1, 9, 2));
        assertThat(merged.columns()).containsEntry(ColumnPath.of("year"), exact(2024));
    }

    @Test
    void withOverridesReplacesAColumnOfTheSamePath() {
        FileStats footer = FileStats.builder()
                .recordCount(50)
                .column(ColumnPath.of("year"), range(2000, 2024, 3))
                .build();
        FileStats partition = FileStats.builder()
                .recordCount(50)
                .column(ColumnPath.of("year"), exact(2024))
                .build();

        FileStats merged = footer.withOverrides(partition);

        assertThat(merged.columns()).containsEntry(ColumnPath.of("year"), exact(2024));
    }
}
