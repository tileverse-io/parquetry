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
package io.tileverse.parquetry.filter.prune;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * The prunable statistics for one file: typed per-column min/max/null-count, geometry bounding boxes per geometry
 * column, and the file's record count. A caller (an Iceberg manifest reader, a footer aggregator) populates it; the
 * pruning facade reads it.
 */
public record FileStats(
        Map<ColumnPath, ColumnStatistics> columns, Map<ColumnPath, BoundingBox> geometryBounds, long recordCount) {

    public FileStats {
        columns = Map.copyOf(Objects.requireNonNull(columns, "columns"));
        geometryBounds = Map.copyOf(Objects.requireNonNull(geometryBounds, "geometryBounds"));
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns these statistics with {@code overrides} layered on top: an overriding column or geometry bound replaces
     * the one of the same path, while this file's record count is kept. The overriding entry is treated as exact - for
     * example a Hive partition-path value, which holds for every row in the partition - and therefore wins on any key
     * collision. The two are usually disjoint (a footer aggregate plus path-only partition columns); the override rule
     * only matters when a partition key also names a physical column.
     */
    public FileStats withOverrides(FileStats overrides) {
        Builder builder = builder().recordCount(recordCount);
        columns.forEach(builder::column);
        geometryBounds.forEach(builder::geometryBounds);
        overrides.columns().forEach(builder::column);
        overrides.geometryBounds().forEach(builder::geometryBounds);
        return builder.build();
    }

    public static final class Builder {
        private final Map<ColumnPath, ColumnStatistics> columns = new LinkedHashMap<>();
        private final Map<ColumnPath, BoundingBox> geometryBounds = new LinkedHashMap<>();
        private long recordCount = -1L;

        public Builder column(ColumnPath path, ColumnStatistics stats) {
            columns.put(Objects.requireNonNull(path, "path"), Objects.requireNonNull(stats, "stats"));
            return this;
        }

        public Builder geometryBounds(ColumnPath path, BoundingBox bounds) {
            geometryBounds.put(Objects.requireNonNull(path, "path"), Objects.requireNonNull(bounds, "bounds"));
            return this;
        }

        public Builder recordCount(long recordCount) {
            this.recordCount = recordCount;
            return this;
        }

        public FileStats build() {
            return new FileStats(columns, geometryBounds, recordCount);
        }
    }
}
