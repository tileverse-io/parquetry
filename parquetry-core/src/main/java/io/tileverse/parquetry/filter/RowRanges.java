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

import java.util.ArrayList;
import java.util.List;

/**
 * A disjoint, sorted set of inclusive row index ranges within a single row group. Produced by the
 * {@code ColumnIndex}/{@code OffsetIndex} tier and refined by the bloom-filter tier; consumed by the record-level tier
 * to decide which rows to materialize.
 *
 * @param ranges disjoint sorted intervals; defensively copied on construction
 */
public record RowRanges(List<Range> ranges) {

    /**
     * Inclusive {@code [first, last]} row interval relative to the start of the row group.
     *
     * @param first first row index covered (inclusive, zero-based within the row group)
     * @param last last row index covered (inclusive); must satisfy {@code last >= first}
     */
    public record Range(long first, long last) {
        public Range {
            if (last < first) {
                throw new IllegalArgumentException("Range end " + last + " precedes start " + first);
            }
        }

        /** Number of rows covered (inclusive). */
        public long count() {
            return last - first + 1;
        }
    }

    public RowRanges {
        ranges = List.copyOf(ranges);
    }

    /** Empty set of ranges: no rows survive. */
    public static RowRanges empty() {
        return new RowRanges(List.of());
    }

    /** Single contiguous range covering rows {@code [0, totalRows-1]}. */
    public static RowRanges all(long totalRows) {
        if (totalRows <= 0) {
            return empty();
        }
        return new RowRanges(List.of(new Range(0, totalRows - 1)));
    }

    /** Whether the union of the contained ranges covers zero rows. */
    public boolean isEmpty() {
        return ranges.isEmpty();
    }

    /** Sum of the row counts across all contained ranges. */
    public long totalRows() {
        long sum = 0;
        for (Range r : ranges) {
            sum += r.count();
        }
        return sum;
    }

    /**
     * Returns the intersection of this row-range set with {@code other}. Both inputs must already be sorted and
     * disjoint; the result preserves those invariants.
     */
    public RowRanges intersect(RowRanges other) {
        List<Range> result = new ArrayList<>();
        int i = 0;
        int j = 0;
        while (i < ranges.size() && j < other.ranges.size()) {
            Range a = ranges.get(i);
            Range b = other.ranges.get(j);
            long start = Math.max(a.first(), b.first());
            long end = Math.min(a.last(), b.last());
            if (start <= end) {
                result.add(new Range(start, end));
            }
            if (a.last() < b.last()) {
                i++;
            } else {
                j++;
            }
        }
        return new RowRanges(result);
    }
}
