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
package io.tileverse.parquetry.internal.read.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.format.PageLocation;

class PageSelectionTest {

    /** Four pages of two rows each: [0,1] [2,3] [4,5] [6,7]. */
    private static final List<PageLocation> FOUR_PAGES = List.of(
            new PageLocation(100, 10, 0),
            new PageLocation(110, 10, 2),
            new PageLocation(120, 10, 4),
            new PageLocation(130, 10, 6));

    @Test
    void marksOnlyPagesThatOverlapTheSurvivingRanges() {
        RowRanges surviving = new RowRanges(List.of(new Range(4, 7)));

        PageSelection selection = PageSelection.forRanges(FOUR_PAGES, 8, surviving);

        assertThat(selection.pageCount()).isEqualTo(4);
        assertThat(selection.isSurviving(0)).isFalse();
        assertThat(selection.isSurviving(1)).isFalse();
        assertThat(selection.isSurviving(2)).isTrue();
        assertThat(selection.isSurviving(3)).isTrue();
        assertThat(selection.survivingPageCount()).isEqualTo(2);
    }

    @Test
    void exposesEachPagesFirstRowIndex() {
        PageSelection selection = PageSelection.forRanges(FOUR_PAGES, 8, RowRanges.all(8));

        assertThat(selection.firstRowIndex(0)).isZero();
        assertThat(selection.firstRowIndex(2)).isEqualTo(4L);
        assertThat(selection.firstRowIndex(3)).isEqualTo(6L);
        assertThat(selection.survivingPageCount()).isEqualTo(4);
    }

    @Test
    void aRangeTouchingOneRowOfALastPageKeepsThatPage() {
        RowRanges surviving = new RowRanges(List.of(new Range(7, 7)));

        PageSelection selection = PageSelection.forRanges(FOUR_PAGES, 8, surviving);

        assertThat(selection.isSurviving(3)).isTrue();
        assertThat(selection.survivingPageCount()).isEqualTo(1);
    }
}
