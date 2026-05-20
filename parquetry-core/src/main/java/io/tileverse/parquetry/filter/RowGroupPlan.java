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
package io.tileverse.parquetry.filter;

import java.util.List;
import java.util.Optional;

/**
 * Per-row-group output of {@code FilterPipeline}: the index within the file, the row count, the ordered list of
 * per-tier {@link PruningDecision}s (one per tier that was applied), the final {@link RowGroupOutcome}, and the
 * surviving {@link RowRanges} when the outcome is {@link RowGroupOutcome#PARTIAL}.
 *
 * @param index zero-based ordinal of this row group within the file's row-group list
 * @param rowCount total rows in this row group (independent of which ones survive pruning)
 * @param tiers ordered per-tier decisions, in the order the pipeline applied them; defensively copied
 * @param outcome the final pipeline decision (eliminated, partial, full) after running every applicable tier
 * @param survivingRows the rows that survive when {@link #outcome} is {@link RowGroupOutcome#PARTIAL}; empty for
 *     {@link RowGroupOutcome#ELIMINATED} and {@link RowGroupOutcome#FULL}
 */
public record RowGroupPlan(
        int index,
        long rowCount,
        List<PruningDecision> tiers,
        RowGroupOutcome outcome,
        Optional<RowRanges> survivingRows) {

    public RowGroupPlan {
        tiers = List.copyOf(tiers);
    }
}
