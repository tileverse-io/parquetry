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
package io.tileverse.parquetry.dataset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Builds a {@link FilesetDataset} directly over borrowed byte sources, mirroring what the catalog assembles (schema
 * from the first file, footer statistics per file, Hive overrides), which lets a test inject the pool, budget and
 * fan-out width that the catalog's default-runtime open does not expose. The caller owns and closes the sources.
 */
final class TestFilesetDatasets {

    private TestFilesetDatasets() {}

    /** A dataset over {@code sources} with no Hive partitioning, bound to {@code openOptions}. */
    static FilesetDataset plain(List<ByteRangeSource> sources, OpenOptions openOptions) {
        List<Map<String, String>> noPartitions = Collections.nCopies(sources.size(), Map.of());
        List<String> locations = new ArrayList<>(sources.size());
        for (int index = 0; index < sources.size(); index++) {
            locations.add("file-" + index + ".parquet");
        }
        return over("plain", sources, noPartitions, locations, openOptions);
    }

    /** A dataset over {@code sources} with the given per-file Hive partition values and relative locations. */
    static FilesetDataset over(
            String name,
            List<ByteRangeSource> sources,
            List<Map<String, String>> perFilePartitions,
            List<String> locations,
            OpenOptions openOptions) {
        ParquetSchema unifiedSchema = null;
        List<FileStats> footerStats = new ArrayList<>(sources.size());
        for (ByteRangeSource source : sources) {
            ParquetSource one = ParquetSource.open(source, openOptions);
            if (unifiedSchema == null) {
                unifiedSchema = one.schema();
            }
            footerStats.add(one.fileStats());
        }
        HivePartitioning partitioning = HivePartitioning.bind(perFilePartitions, unifiedSchema);
        ParquetSchema augmentedSchema = unifiedSchema.withAppendedLeaves(partitioning.syntheticLeaves());
        List<FileStats> stats = new ArrayList<>(sources.size());
        for (int index = 0; index < sources.size(); index++) {
            FileStats footer = footerStats.get(index);
            FileStats hive = partitioning.fileStats(perFilePartitions.get(index), footer.recordCount());
            stats.add(footer.withOverrides(hive));
        }
        FilesetDataset.PartitionContext context =
                new FilesetDataset.PartitionContext(augmentedSchema, partitioning, perFilePartitions, stats);
        DatasetCapabilities.PartitionModel partitionModel = partitioning.hasSynthetic()
                ? DatasetCapabilities.PartitionModel.HIVE_PATH
                : DatasetCapabilities.PartitionModel.NONE;
        DatasetCapabilities capabilities = DatasetCapabilities.builder()
                .fileStats(DatasetCapabilities.FileStatsSource.FOOTER_AGGREGATE)
                .partitionModel(partitionModel)
                .build();
        Optional<ParquetSource> singleFile =
                sources.size() == 1 ? Optional.of(ParquetSource.open(sources.get(0), openOptions)) : Optional.empty();
        return new FilesetDataset(
                name,
                unifiedSchema,
                singleFile,
                context,
                sources,
                locations,
                capabilities,
                Optional.empty(),
                openOptions);
    }
}
