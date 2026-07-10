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
 * <p>The survivor exposes its {@link RowGroupChunks} view, built once during filtering and reused by mask building and
 * the fetcher, which keeps each index section read at most once per call.
 *
 * <p>{@code recordEvalRequired} states whether the read still has to test each decoded row against the predicate. It is
 * {@code true} for the FULL and PARTIAL outcomes (the a-priori tiers cleared the row group or a subset of it, but did
 * not prove every surviving row matches) and {@code false} for the MATCHED outcome (statistics already proved every row
 * matches, hence per-row evaluation is wasted work).
 *
 * @param chunks the row group's chunk view (chunk index plus memoized index sections)
 * @param survivingRows the surviving row ranges within the row group, or empty for "read all rows"
 * @param recordEvalRequired whether per-row predicate evaluation is still needed for this row group
 */
public record RowGroupSurvivor(
        @NonNull RowGroupChunks chunks, @NonNull Optional<RowRanges> survivingRows, boolean recordEvalRequired) {

    /** The on-disk row group, including the per-column chunk metadata. */
    public RowGroup rowGroup() {
        return chunks.rowGroup();
    }

    /**
     * A survivor read in full whose rows still need per-row predicate evaluation (the FULL outcome, or a PARTIAL
     * survivor once narrowed to its surviving ranges).
     */
    public static RowGroupSurvivor full(@NonNull RowGroupChunks chunks) {
        return new RowGroupSurvivor(chunks, Optional.empty(), true);
    }

    /**
     * A survivor whose statistics proved every row matches the predicate (the MATCHED outcome). Read in full with no
     * per-row evaluation.
     */
    public static RowGroupSurvivor matched(@NonNull RowGroupChunks chunks) {
        return new RowGroupSurvivor(chunks, Optional.empty(), false);
    }
}
