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

import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.internal.read.BatchForm;
import io.tileverse.parquetry.internal.read.MaskedScan;
import io.tileverse.parquetry.internal.read.RowPositionColumn;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * What the entry point of a query asks of its decode. The {@link ReadPlan} of the query holds the outcome of the filter
 * pipeline, and {@link ReadPlan#decode} assembles the decode from that outcome and this shape.
 *
 * @param maskedScan the masked scan, present when the predicate is applied during decode
 * @param batchForm the form of the decoded batches
 * @param matchedEqualsDecoded whether each decoded row reaches the output. If it does, the decode observation reports
 *     the decoded rows as matched. If not, the batch pipeline tallies the matched rows as it emits or folds them.
 * @param rowPositionColumns the row-position columns to synthesize
 * @param spatialPlanning the spatial planning of a read that produces rows. It is empty for a count and for a bounds
 *     query.
 */
record DecodeShape(
        @NonNull Optional<MaskedScan> maskedScan,
        @NonNull BatchForm batchForm,
        boolean matchedEqualsDecoded,
        @NonNull List<RowPositionColumn> rowPositionColumns,
        @NonNull Optional<SpatialPlanning> spatialPlanning) {

    DecodeShape {
        rowPositionColumns = List.copyOf(rowPositionColumns);
    }

    /**
     * The inputs of the spatial row-group planner of a read that produces rows.
     * {@link SpatialReadGates#rowGroupPlanner} states when the planner is sound.
     *
     * @param rawPredicate the predicate of the query, before any covering rewrite
     * @param outputSchema the physical schema of the batches seen by the per-row gate
     */
    record SpatialPlanning(
            @NonNull Predicate rawPredicate, @NonNull ParquetSchema outputSchema) {}
}
