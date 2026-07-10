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

import java.util.Objects;

/**
 * Per-dataset capabilities, built once at open time. Enums (rather than a bare boolean per source) let a caller ask how
 * to get a thing here and branch once; {@code require*} guards turn an unsupported request into a precise exception.
 */
public record DatasetCapabilities(
        boolean mergeOnRead,
        boolean fieldIdResolved,
        FileStatsSource fileStats,
        FileSpatialBounds fileSpatialBounds,
        PartitionModel partitionModel,
        boolean cheapCount,
        boolean cheapBounds) {

    /** How per-file column statistics are obtained without opening a footer. */
    public enum FileStatsSource {
        NONE,
        MANIFEST,
        FOOTER_AGGREGATE,
        STAC_ITEM
    }

    /** How a file's spatial bounds are obtained. */
    public enum FileSpatialBounds {
        NONE,
        NATIVE_GEO,
        COVERING_COLUMN
    }

    /** How partitioning is expressed. */
    public enum PartitionModel {
        NONE,
        HIVE_PATH,
        PARTITION_SPEC
    }

    public DatasetCapabilities {
        Objects.requireNonNull(fileStats, "fileStats");
        Objects.requireNonNull(fileSpatialBounds, "fileSpatialBounds");
        Objects.requireNonNull(partitionModel, "partitionModel");
    }

    public void requireMergeOnRead() {
        if (!mergeOnRead) {
            throw new UnsupportedOperationException("dataset capability not supported: mergeOnRead");
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder with safe-false / NONE defaults. */
    public static final class Builder {
        private boolean mergeOnRead;
        private boolean fieldIdResolved;
        private FileStatsSource fileStats = FileStatsSource.NONE;
        private FileSpatialBounds fileSpatialBounds = FileSpatialBounds.NONE;
        private PartitionModel partitionModel = PartitionModel.NONE;
        private boolean cheapCount;
        private boolean cheapBounds;

        private Builder() {}

        public Builder mergeOnRead(boolean value) {
            this.mergeOnRead = value;
            return this;
        }

        public Builder fieldIdResolved(boolean value) {
            this.fieldIdResolved = value;
            return this;
        }

        public Builder fileStats(FileStatsSource value) {
            this.fileStats = Objects.requireNonNull(value, "fileStats");
            return this;
        }

        public Builder fileSpatialBounds(FileSpatialBounds value) {
            this.fileSpatialBounds = Objects.requireNonNull(value, "fileSpatialBounds");
            return this;
        }

        public Builder partitionModel(PartitionModel value) {
            this.partitionModel = Objects.requireNonNull(value, "partitionModel");
            return this;
        }

        public Builder cheapCount(boolean value) {
            this.cheapCount = value;
            return this;
        }

        public Builder cheapBounds(boolean value) {
            this.cheapBounds = value;
            return this;
        }

        public DatasetCapabilities build() {
            return new DatasetCapabilities(
                    mergeOnRead,
                    fieldIdResolved,
                    fileStats,
                    fileSpatialBounds,
                    partitionModel,
                    cheapCount,
                    cheapBounds);
        }
    }
}
