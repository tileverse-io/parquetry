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
package io.tileverse.parquetry.columnar;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import org.junit.jupiter.api.Test;

class LevelsTest {

    // levels for a repeated+optional leaf: rows start at 0-markers; maxLevel here is 2
    private static final int[] SAMPLE = {0, 1, 1, 0, 2, 0, 2, 2};

    @Test
    void arrayBackedReadsThrough() {
        Levels levels = Levels.of(SAMPLE.clone());
        assertThat(levels.size()).isEqualTo(8);
        assertThat(levels.get(0)).isZero();
        assertThat(levels.get(4)).isEqualTo(2);
        assertThat(levels.get(7)).isEqualTo(2);
    }

    @Test
    void segmentBackedMatchesArrayBackedOnEveryOperation() {
        Levels onHeap = Levels.of(SAMPLE.clone());
        Levels offHeap = Levels.ofSegment(segmentOf(SAMPLE), SAMPLE.length);

        assertThat(offHeap.size()).isEqualTo(onHeap.size());
        for (int i = 0; i < SAMPLE.length; i++) {
            assertThat(offHeap.get(i)).as("get(%d)", i).isEqualTo(onHeap.get(i));
        }
        assertThat(offHeap.countOf(0)).isEqualTo(onHeap.countOf(0)).isEqualTo(3);
        assertThat(offHeap.valuesForRows(0, 2)).isEqualTo(onHeap.valuesForRows(0, 2));
        assertThat(offHeap.rowsInRange(0, 8)).isEqualTo(onHeap.rowsInRange(0, 8));
        assertThat(offHeap.validityAt(2).nullCount())
                .isEqualTo(onHeap.validityAt(2).nullCount())
                .isEqualTo(5);
        int[] keep = {0, 4, 7};
        Levels gatheredOffHeap = offHeap.gather(keep);
        Levels gatheredOnHeap = onHeap.gather(keep);
        assertThat(gatheredOffHeap.size()).isEqualTo(gatheredOnHeap.size()).isEqualTo(keep.length);
        for (int j = 0; j < keep.length; j++) {
            assertThat(gatheredOffHeap.get(j)).isEqualTo(gatheredOnHeap.get(j));
        }
    }

    @Test
    void countOfCountsMatchingLevels() {
        Levels levels = Levels.of(SAMPLE.clone());
        assertThat(levels.countOf(0)).as("logical rows for a rep stream").isEqualTo(3);
        assertThat(levels.countOf(2)).isEqualTo(3);
        assertThat(levels.countOf(7)).isZero();
    }

    @Test
    void valuesForRowsCountsEntriesUntilTheRequestedRowBoundary() {
        Levels levels = Levels.of(SAMPLE.clone());
        // row 0 = entries [0,3) (3 entries), row 1 = [3,5), row 2 = [5,8)
        assertThat(levels.valuesForRows(0, 1)).isEqualTo(3);
        assertThat(levels.valuesForRows(0, 2)).isEqualTo(5);
        assertThat(levels.valuesForRows(3, 1)).isEqualTo(2);
        assertThat(levels.valuesForRows(0, 99))
                .as("more rows than remain returns the tail")
                .isEqualTo(8);
    }

    @Test
    void rowsInRangeCountsZeroMarkers() {
        Levels levels = Levels.of(SAMPLE.clone());
        assertThat(levels.rowsInRange(0, 8)).isEqualTo(3);
        assertThat(levels.rowsInRange(1, 3)).isEqualTo(1);
        assertThat(levels.rowsInRange(4, 2)).isEqualTo(1);
        assertThat(levels.rowsInRange(1, 2)).isZero();
    }

    @Test
    void validityAtMarksRowsThatReachMaxLevel() {
        Levels defLevels = Levels.of(new int[] {1, 0, 1, 0, 1});
        Validity validity = defLevels.validityAt(1);
        assertThat(validity.size()).isEqualTo(5);
        assertThat(validity.isValid(0)).isTrue();
        assertThat(validity.isValid(1)).isFalse();
        assertThat(validity.isValid(2)).isTrue();
        assertThat(validity.isValid(3)).isFalse();
        assertThat(validity.isValid(4)).isTrue();
        assertThat(validity.nullCount()).isEqualTo(2);
    }

    @Test
    void validityAtCollapsesAnAllDefinedStreamToAllValid() {
        Levels defLevels = Levels.of(new int[] {1, 1, 1});
        Validity validity = defLevels.validityAt(1);
        assertThat(validity.hasNulls()).isFalse();
        assertThat(validity.heapBytes())
                .as("Validity.of collapses to all-valid, no bitmap")
                .isZero();
    }

    @Test
    void gatherReindexesToTheKeepPositions() {
        Levels levels = Levels.of(SAMPLE.clone());
        Levels gathered = levels.gather(new int[] {1, 4, 6});
        assertThat(gathered.size()).isEqualTo(3);
        assertThat(gathered.get(0)).isEqualTo(1);
        assertThat(gathered.get(1)).isEqualTo(2);
        assertThat(gathered.get(2)).isEqualTo(2);
    }

    @Test
    void degenerateInputsReturnEmptyResults() {
        Levels levels = Levels.of(SAMPLE.clone());
        assertThat(levels.valuesForRows(levels.size(), 1))
                .as("from at the end has no entries left")
                .isZero();
        assertThat(levels.rowsInRange(2, 0)).as("empty range has no rows").isZero();
        assertThat(levels.gather(new int[0]).size())
                .as("gathering nothing yields an empty sequence")
                .isZero();
        assertThat(Levels.of(new int[0]).size()).as("empty level array").isZero();
    }

    private static MemorySegment segmentOf(int[] levels) {
        MemorySegment segment = MemorySegment.ofArray(new byte[levels.length * Integer.BYTES]);
        for (int i = 0; i < levels.length; i++) {
            segment.setAtIndex(ValueLayout.JAVA_INT_UNALIGNED, i, levels[i]);
        }
        return segment;
    }
}
