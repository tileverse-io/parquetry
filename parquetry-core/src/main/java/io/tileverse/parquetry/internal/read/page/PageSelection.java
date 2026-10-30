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
package io.tileverse.parquetry.internal.read.page;

import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.internal.filter.PageIndex;

/**
 * Per-data-page decision derived from the {@link PageIndex} of a column and the surviving {@link RowRanges} of a row
 * group: which pages overlap the surviving rows, and where each page starts. Page ordinals follow the page index.
 * {@link PageCursor} encounters the data pages in the same order.
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
     * The one derivation of a column's page survival, shared by the fetch planner and the column reader with identical
     * inputs: the page index of the column and the surviving rows of the row group. A page survives when its rows
     * overlap a surviving range. One call site on each side keeps fetch and decode structurally incapable of drifting.
     */
    public static PageSelection forColumn(PageIndex pages, RowRanges survivingRows) {
        int pageCount = pages.pageCount();
        boolean[] survivingFlags = new boolean[pageCount];
        long[] firstRows = new long[pageCount];
        int kept = 0;
        for (int page = 0; page < pageCount; page++) {
            long first = pages.firstRow(page);
            firstRows[page] = first;
            survivingFlags[page] = overlapsAny(survivingRows, first, pages.lastRow(page));
            if (survivingFlags[page]) {
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
