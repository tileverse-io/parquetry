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
package io.tileverse.parquetry.observe;

import java.util.Optional;

/**
 * Aggregate decode result for one row group, flushed once after the row group finishes. Mergeable across row groups and
 * files via {@link #combine}; the merged index keeps the lowest.
 *
 * <p>{@code rowsDecoded} is the rows actually decoded for the group, not its metadata row count. With a streaming or
 * prefetching reader, an early-terminated stream may have decoded ahead of consumption by up to the hand-off depth: the
 * figure is rows decoded, not rows consumed.
 */
public record RowGroupRead(
        int rowGroupIndex,
        long rowsDecoded,
        long rowsMatched,
        int pagesDecoded,
        int pagesPruned,
        FetchStats fetch,
        Optional<PhaseTimings> timings) {

    public RowGroupRead combine(RowGroupRead other) {
        return new RowGroupRead(
                Math.min(rowGroupIndex, other.rowGroupIndex),
                rowsDecoded + other.rowsDecoded,
                rowsMatched + other.rowsMatched,
                pagesDecoded + other.pagesDecoded,
                pagesPruned + other.pagesPruned,
                fetch.combine(other.fetch),
                combineTimings(other.timings));
    }

    private Optional<PhaseTimings> combineTimings(Optional<PhaseTimings> other) {
        if (timings.isPresent() && other.isPresent()) {
            return Optional.of(timings.get().combine(other.get()));
        }
        if (timings.isPresent()) {
            return timings;
        }
        return other;
    }
}
