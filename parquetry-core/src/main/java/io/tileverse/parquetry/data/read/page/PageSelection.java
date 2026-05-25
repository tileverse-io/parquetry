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
package io.tileverse.parquetry.data.read.page;

import java.util.List;

import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;

/**
 * Per-data-page decision derived from a column's {@link OffsetIndex} page locations and a row group's surviving
 * {@link RowRanges}: which pages overlap the surviving rows, and where each page starts. Page ordinals match the order
 * in which {@link PageCursor} encounters data pages, which is the {@code OffsetIndex} order.
 */
public final class PageSelection {

    private final boolean[] surviving;
    private final long[] firstRowIndex;
    private final int survivingPageCount;

    private PageSelection(boolean[] surviving, long[] firstRowIndex, int survivingPageCount) {
        this.surviving = surviving;
        this.firstRowIndex = firstRowIndex;
        this.survivingPageCount = survivingPageCount;
    }

    /**
     * Computes the selection for one column. A page survives when its row span overlaps any surviving range. The last
     * page's span ends at {@code rowGroupRowCount - 1}; earlier pages end one row before the next page begins.
     */
    public static PageSelection forRanges(
            List<PageLocation> pageLocations, long rowGroupRowCount, RowRanges surviving) {
        int pages = pageLocations.size();
        boolean[] survivingFlags = new boolean[pages];
        long[] firstRows = new long[pages];
        int kept = 0;
        for (int i = 0; i < pages; i++) {
            long first = pageLocations.get(i).firstRowIndex();
            long last = (i + 1 < pages) ? pageLocations.get(i + 1).firstRowIndex() - 1 : rowGroupRowCount - 1;
            firstRows[i] = first;
            survivingFlags[i] = overlapsAny(surviving, first, last);
            if (survivingFlags[i]) {
                kept++;
            }
        }
        return new PageSelection(survivingFlags, firstRows, kept);
    }

    public boolean isSurviving(int pageOrdinal) {
        return surviving[pageOrdinal];
    }

    public long firstRowIndex(int pageOrdinal) {
        return firstRowIndex[pageOrdinal];
    }

    public int pageCount() {
        return surviving.length;
    }

    public int survivingPageCount() {
        return survivingPageCount;
    }

    private static boolean overlapsAny(RowRanges ranges, long first, long last) {
        for (RowRanges.Range range : ranges.ranges()) {
            if (range.first() <= last && range.last() >= first) {
                return true;
            }
        }
        return false;
    }
}
