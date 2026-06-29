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
package io.tileverse.parquetry.internal.read;

import java.util.ArrayList;
import java.util.List;

import io.tileverse.parquetry.filter.RowRanges;

/**
 * The predicate-matched rows from a single row-group scan, expressed as a coalesced {@link RowRanges}.
 *
 * <p>Built incrementally by {@link Builder#accept(long, boolean)} as phase-1 decode walks surviving rows in ascending
 * absolute-row order, recording which rows satisfied the predicate.
 */
final class MatchedRows {

    private final RowRanges rows;

    private MatchedRows(RowRanges rows) {
        this.rows = rows;
    }

    /** The rows that satisfied the predicate, coalesced into disjoint ascending ranges. */
    public RowRanges rows() {
        return rows;
    }

    /** True when no row satisfied the predicate. */
    public boolean isEmpty() {
        return rows.isEmpty();
    }

    /** Returns a new builder for accumulating predicate results in ascending absolute-row order. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Accumulates per-row predicate outcomes in ascending absolute-row order and coalesces consecutive passed rows into
     * compact {@link RowRanges.Range} intervals.
     *
     * <p>Callers must feed rows strictly in ascending absolute-row order. Coalescing is valid only because consecutive
     * indices arrive in order: extending the current run is a single comparison rather than a sort or merge.
     */
    public static final class Builder {

        private static final long NO_RUN = -1L;

        private final List<RowRanges.Range> completedRanges = new ArrayList<>();

        /** First absolute row of the current open run, or {@link #NO_RUN} when no run is open. */
        private long runStart = NO_RUN;

        /** Last absolute row of the current open run. */
        private long runEnd = NO_RUN;

        /**
         * Records whether {@code absoluteRow} passed the predicate.
         *
         * <p>Rows must be fed in strictly ascending order; the builder does not validate this contract because the
         * caller (phase-1 decode) guarantees it by construction.
         *
         * @param absoluteRow row index within the row group (zero-based, ascending)
         * @param passed true when the row satisfied the predicate
         */
        public void accept(long absoluteRow, boolean passed) {
            if (!passed) {
                closeCurrentRun();
                return;
            }
            if (runStart == NO_RUN) {
                // Start a fresh run.
                runStart = absoluteRow;
                runEnd = absoluteRow;
            } else if (absoluteRow == runEnd + 1) {
                // Extend the current contiguous run.
                runEnd = absoluteRow;
            } else {
                // Gap detected: close the previous run and start a new one.
                closeCurrentRun();
                runStart = absoluteRow;
                runEnd = absoluteRow;
            }
        }

        /**
         * Closes the final open run (if any) and returns the accumulated {@link MatchedRows}. When no rows passed, the
         * returned selection is empty.
         */
        public MatchedRows build() {
            closeCurrentRun();
            RowRanges rowRanges = new RowRanges(completedRanges);
            return new MatchedRows(rowRanges);
        }

        private void closeCurrentRun() {
            if (runStart == NO_RUN) {
                return;
            }
            completedRanges.add(new RowRanges.Range(runStart, runEnd));
            runStart = NO_RUN;
            runEnd = NO_RUN;
        }
    }
}
