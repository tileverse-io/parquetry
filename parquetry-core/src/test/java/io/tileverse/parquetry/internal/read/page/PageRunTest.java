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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;

class PageRunTest {

    /** Pages 0:[100,150) 1:[150,200) 2:[200,250) 3:[260,310) - page 3 does not abut page 2. */
    private static List<PageLocation> locations() {
        return List.of(
                new PageLocation(100, 50, 0),
                new PageLocation(150, 50, 10),
                new PageLocation(200, 50, 20),
                new PageLocation(260, 50, 30));
    }

    private static PageSelection selectionFor(RowRanges surviving) {
        return PageSelection.forRanges(locations(), 40, surviving);
    }

    private static RowRanges rows(long first, long last) {
        return new RowRanges(List.of(new RowRanges.Range(first, last)));
    }

    @Test
    void adjacentSurvivingPagesFormOneRun() {
        List<PageRun> runs = PageRun.runsFor(selectionFor(rows(0, 19)), locations());

        assertThat(runs).containsExactly(new PageRun(100, 100, 0));
    }

    @Test
    void holeBetweenSurvivingPagesSplitsRuns() {
        RowRanges surviving = new RowRanges(List.of(new RowRanges.Range(0, 9), new RowRanges.Range(20, 29)));

        List<PageRun> runs = PageRun.runsFor(selectionFor(surviving), locations());

        assertThat(runs).containsExactly(new PageRun(100, 50, 0), new PageRun(200, 50, 2));
    }

    @Test
    void byteGapBetweenAdjacentSurvivingOrdinalsSplitsRuns() {
        List<PageRun> runs = PageRun.runsFor(selectionFor(rows(20, 39)), locations());

        assertThat(runs).containsExactly(new PageRun(200, 50, 2), new PageRun(260, 50, 3));
    }

    @Test
    void allPagesSurvivingYieldsRunsCoveringEveryPage() {
        List<PageRun> runs = PageRun.runsFor(selectionFor(rows(0, 39)), locations());

        assertThat(runs).containsExactly(new PageRun(100, 150, 0), new PageRun(260, 50, 3));
    }

    @Test
    void noSurvivingPageYieldsNoRuns() {
        PageSelection none = PageSelection.forRanges(locations(), 40, RowRanges.empty());

        assertThat(PageRun.runsFor(none, locations())).isEmpty();
    }

    @Test
    void forColumnMatchesForRangesWithIdenticalInputs() {
        OffsetIndex offsetIndex = new OffsetIndex(locations(), Optional.empty());

        PageSelection viaColumn = PageSelection.forColumn(offsetIndex, 40, rows(0, 19));
        PageSelection viaRanges = PageSelection.forRanges(locations(), 40, rows(0, 19));

        assertThat(viaColumn.survivingPageCount()).isEqualTo(viaRanges.survivingPageCount());
        for (int ordinal = 0; ordinal < viaRanges.pageCount(); ordinal++) {
            assertThat(viaColumn.isSurviving(ordinal)).isEqualTo(viaRanges.isSurviving(ordinal));
            assertThat(viaColumn.firstRowIndex(ordinal)).isEqualTo(viaRanges.firstRowIndex(ordinal));
        }
    }
}
