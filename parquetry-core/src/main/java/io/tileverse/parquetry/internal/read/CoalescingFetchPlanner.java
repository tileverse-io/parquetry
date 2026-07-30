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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Pure planner that merges a row group's projected byte ranges into a minimal set of coalesced range reads.
 *
 * <p>{@link FetchUnit}s are sorted by file offset, then merged greedily: a unit joins the current range when the gap to
 * it is at most {@code maxCoalesceGap} and the resulting span stays at most {@code maxCoalescedSpan}; otherwise it
 * opens a new range. A single unit larger than the span becomes its own range, because a unit is never split.
 *
 * <p>Each unit records where it landed: a dictionary prefix fills its column's prefix slot, and every other unit
 * appends a {@link RunSlice} to its column's run list. The offset ordering of the merge walk makes that list
 * file-ordered, which for data pages is data-page ordinal order.
 */
final class CoalescingFetchPlanner {

    private CoalescingFetchPlanner() {}

    static FetchPlan plan(List<FetchUnit> units, int maxCoalesceGap, int maxCoalescedSpan) {
        if (units.isEmpty()) {
            return new FetchPlan(List.of(), Map.of());
        }
        List<FetchUnit> ordered = new ArrayList<>(units);
        ordered.sort(Comparator.comparingLong(FetchUnit::fileOffset));

        List<CoalescedRange> ranges = new ArrayList<>();
        Map<ColumnPath, ColumnSlice> dictionaryPrefixes = new HashMap<>();
        Map<ColumnPath, List<RunSlice>> runSlices = new HashMap<>();

        FetchUnit first = ordered.get(0);
        long rangeStart = first.fileOffset();
        long rangeEnd = first.fileOffset() + first.length();
        boolean rangeOpen = false;
        for (FetchUnit unit : ordered) {
            long unitEnd = unit.fileOffset() + unit.length();
            if (rangeOpen && !fitsInCurrentRange(unit, rangeStart, rangeEnd, maxCoalesceGap, maxCoalescedSpan)) {
                ranges.add(new CoalescedRange(rangeStart, Math.toIntExact(rangeEnd - rangeStart)));
                rangeStart = unit.fileOffset();
                rangeEnd = unitEnd;
            } else {
                rangeEnd = Math.max(rangeEnd, unitEnd);
            }
            rangeOpen = true;
            int rangeIndex = ranges.size();
            int offsetWithinRange = Math.toIntExact(unit.fileOffset() - rangeStart);
            if (unit.dictionaryPrefix()) {
                dictionaryPrefixes.put(unit.path(), new ColumnSlice(rangeIndex, offsetWithinRange, unit.length()));
            } else {
                runSlices
                        .computeIfAbsent(unit.path(), _ -> new ArrayList<>())
                        .add(new RunSlice(rangeIndex, offsetWithinRange, unit.length(), unit.firstPageOrdinal()));
            }
        }
        ranges.add(new CoalescedRange(rangeStart, Math.toIntExact(rangeEnd - rangeStart)));
        return new FetchPlan(ranges, mergeSlicesByColumn(dictionaryPrefixes, runSlices));
    }

    private static Map<ColumnPath, ColumnSlices> mergeSlicesByColumn(
            Map<ColumnPath, ColumnSlice> dictionaryPrefixes, Map<ColumnPath, List<RunSlice>> runSlices) {
        Map<ColumnPath, ColumnSlices> slices = new HashMap<>();
        Set<ColumnPath> paths = new HashSet<>(runSlices.keySet());
        paths.addAll(dictionaryPrefixes.keySet());
        for (ColumnPath path : paths) {
            slices.put(
                    path,
                    new ColumnSlices(
                            Optional.ofNullable(dictionaryPrefixes.get(path)),
                            runSlices.getOrDefault(path, List.of())));
        }
        return slices;
    }

    private static boolean fitsInCurrentRange(
            FetchUnit unit, long rangeStart, long rangeEnd, int maxCoalesceGap, int maxCoalescedSpan) {
        long gap = unit.fileOffset() - rangeEnd;
        long spanIfAdded = (unit.fileOffset() + unit.length()) - rangeStart;
        return gap <= maxCoalesceGap && spanIfAdded <= maxCoalescedSpan;
    }
}
