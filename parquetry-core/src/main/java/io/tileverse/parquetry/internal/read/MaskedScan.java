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
package io.tileverse.parquetry.internal.read;

import java.util.List;
import java.util.Set;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Immutable per-read inputs that switch {@link ParallelDecodeCoordinator} onto the masked scan decode path.
 *
 * <p>{@link #predicate} and {@link #filterLeaves} apply to every row group; {@link #rowsToScan} holds the rows each row
 * group's masked walk must cover, indexed parallel to the coordinator's survivor list, which the driver proves its
 * window walk against.
 *
 * @param predicate the normalized predicate evaluated per window
 * @param filterLeaves the physical leaf columns the predicate reads
 * @param outputSchema the schema the emitted batches expose (the caller's projection)
 * @param rowsToScan the rows each row group's masked walk must cover - the surviving rows where a column-index row mask
 *     narrows the group, the row group's own row count otherwise - parallel to the coordinator's survivors
 */
public record MaskedScan(
        @NonNull Predicate predicate,
        @NonNull Set<ColumnPath> filterLeaves,
        @NonNull ParquetSchema outputSchema,
        @NonNull List<Long> rowsToScan) {

    public MaskedScan {
        filterLeaves = Set.copyOf(filterLeaves);
        rowsToScan = List.copyOf(rowsToScan);
    }
}
