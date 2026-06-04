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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Pure planner that merges a row group's projected column chunks into a minimal set of coalesced byte ranges.
 *
 * <p>Chunks are sorted by file offset, then merged greedily: a chunk joins the current range when the gap to it is at
 * most {@code maxCoalesceGap} and the resulting span stays at most {@code maxCoalescedSpan}; otherwise it opens a new
 * range. A single chunk larger than the span becomes its own range, because chunks are never split.
 */
final class CoalescingFetchPlanner {

    private CoalescingFetchPlanner() {}

    static FetchPlan plan(List<ColumnRange> columns, int maxCoalesceGap, int maxCoalescedSpan) {
        if (columns.isEmpty()) {
            return new FetchPlan(List.of(), Map.of());
        }
        List<ColumnRange> ordered = new ArrayList<>(columns);
        ordered.sort(Comparator.comparingLong(ColumnRange::fileOffset));

        List<CoalescedRange> ranges = new ArrayList<>();
        Map<ColumnPath, ColumnSlice> slices = new LinkedHashMap<>();

        ColumnRange first = ordered.get(0);
        long rangeStart = first.fileOffset();
        long rangeEnd = first.fileOffset() + first.length();
        boolean rangeOpen = false;
        for (ColumnRange column : ordered) {
            long columnEnd = column.fileOffset() + column.length();
            if (rangeOpen && !fitsInCurrentRange(column, rangeStart, rangeEnd, maxCoalesceGap, maxCoalescedSpan)) {
                ranges.add(new CoalescedRange(rangeStart, Math.toIntExact(rangeEnd - rangeStart)));
                rangeStart = column.fileOffset();
                rangeEnd = columnEnd;
            } else {
                rangeEnd = Math.max(rangeEnd, columnEnd);
            }
            rangeOpen = true;
            int rangeIndex = ranges.size();
            int offsetWithinRange = Math.toIntExact(column.fileOffset() - rangeStart);
            slices.put(column.path(), new ColumnSlice(rangeIndex, offsetWithinRange, column.length()));
        }
        ranges.add(new CoalescedRange(rangeStart, Math.toIntExact(rangeEnd - rangeStart)));
        return new FetchPlan(ranges, slices);
    }

    private static boolean fitsInCurrentRange(
            ColumnRange column, long rangeStart, long rangeEnd, int maxCoalesceGap, int maxCoalescedSpan) {
        long gap = column.fileOffset() - rangeEnd;
        long spanIfAdded = (column.fileOffset() + column.length()) - rangeStart;
        return gap <= maxCoalesceGap && spanIfAdded <= maxCoalescedSpan;
    }
}
