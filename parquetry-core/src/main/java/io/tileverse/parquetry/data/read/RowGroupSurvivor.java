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
package io.tileverse.parquetry.data.read;

import java.util.Optional;

import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.format.RowGroup;

import lombok.NonNull;

/**
 * One row group the filter pipeline cleared for reading, optionally narrowed to a set of row ranges.
 *
 * <p>The pipeline runs each row group through the stats, dictionary, column-index, bloom-filter, and record-level
 * tiers; row groups that are not entirely eliminated come out as survivors. {@link BatchPipeline} receives the
 * already-pruned list and does not re-run the filter.
 *
 * <p>{@code survivingRows} carries the narrowing from the column-index tier. An empty {@link Optional} means "no
 * narrowing": read every row. A present value means "read only the rows in these ranges". Record-level filtering still
 * runs inline during assembly per the streaming memory contract.
 *
 * <p>The survivor carries its {@link RowGroupChunks} view, built once during filtering and reused by mask building and
 * the fetcher, which keeps each index section read at most once per call.
 *
 * @param chunks the row group's chunk view (chunk index plus memoized index sections)
 * @param survivingRows the surviving row ranges within the row group, or empty for "read all rows"
 */
public record RowGroupSurvivor(
        @NonNull RowGroupChunks chunks, @NonNull Optional<RowRanges> survivingRows) {

    /** The on-disk row group, including the per-column chunk metadata. */
    public RowGroup rowGroup() {
        return chunks.rowGroup();
    }

    /** Convenience for callers that have no row-range narrowing to apply. */
    public static RowGroupSurvivor full(@NonNull RowGroupChunks chunks) {
        return new RowGroupSurvivor(chunks, Optional.empty());
    }
}
