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
package io.tileverse.parquetry.read;

import java.util.Optional;

import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.format.RowGroup;

import lombok.NonNull;

/**
 * One row group that the filter pipeline has cleared for reading, optionally narrowed to a set of row ranges.
 *
 * <p>The {@link io.tileverse.parquetry.filter.FilterPipeline} runs each row group through the stats, dictionary,
 * column-index, bloom-filter, and record-level tiers; the row groups that aren't entirely eliminated come out as
 * survivors. The pipeline that orchestrates reads (see {@link RowGroupPipeline}) receives this already-pruned list and
 * does not re-run the filter.
 *
 * <p>{@code survivingRows} carries the narrowing produced by the column-index / bloom-filter tiers. An empty
 * {@link Optional} means "no narrowing": read every row of the row group. A present value means "read only the rows in
 * these ranges". Record-level filtering still runs inline during assembly per the streaming memory contract.
 *
 * @param rowGroup the on-disk row group, including the per-column chunk metadata
 * @param survivingRows the surviving row ranges within the row group, or empty for "read all rows"
 */
public record RowGroupSurvivor(
        @NonNull RowGroup rowGroup, @NonNull Optional<RowRanges> survivingRows) {

    /** Convenience for callers that have no row-range narrowing to apply. */
    public static RowGroupSurvivor full(@NonNull RowGroup rowGroup) {
        return new RowGroupSurvivor(rowGroup, Optional.empty());
    }
}
