/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.data.read;

import java.util.List;
import java.util.Map;
import java.util.Set;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Immutable per-read carrier that switches {@link ParallelDecodeCoordinator} onto the late-materializing decode path.
 *
 * <p>The constant fields ({@link #predicate}, {@link #predicateLeaves}, {@link #outputSchema}) apply to every row
 * group; {@link #perRowGroup} holds the data that differs per row group, indexed parallel to the coordinator's survivor
 * list. The coordinator hands each row group's {@link PerRowGroup} to a {@link LateMaterializingRowGroupReader}, which
 * decodes the predicate columns first and materializes the output columns only for the matching rows.
 *
 * @param predicate the normalized predicate evaluated per row in phase one
 * @param predicateLeaves the leaf columns the predicate references
 * @param outputSchema the schema rows are materialized through (the caller's projection)
 * @param perRowGroup per-row-group decode inputs, parallel to the coordinator's survivors
 */
public record LateMaterialization(
        @NonNull Predicate predicate,
        @NonNull Set<ColumnPath> predicateLeaves,
        @NonNull ParquetSchema outputSchema,
        @NonNull List<PerRowGroup> perRowGroup) {

    public LateMaterialization {
        predicateLeaves = Set.copyOf(predicateLeaves);
        perRowGroup = List.copyOf(perRowGroup);
    }

    /**
     * One row group's late-materialization inputs.
     *
     * @param outputOffsetIndexes the offset index per output leaf, needed by phase-two skip-decode
     * @param numRows the row group's total row count, used when there is no page-skip mask
     */
    public record PerRowGroup(@NonNull Map<ColumnPath, OffsetIndex> outputOffsetIndexes, long numRows) {

        public PerRowGroup {
            outputOffsetIndexes = Map.copyOf(outputOffsetIndexes);
        }
    }
}
