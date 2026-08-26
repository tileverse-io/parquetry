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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.BitSet;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.Levels;

/** Which of a page's value slots the surviving rows of one window cover, for flat and repeated columns alike. */
class MaskedValuesTest {

    @Test
    void flatWindowMapsEachSurvivingRowToOneSlot() {
        BitSet surviving = new BitSet();
        surviving.set(0);
        surviving.set(3);
        assertThat(MaskedValues.flatSlots(10, 4, surviving)).containsExactly(10, 13);
    }

    @Test
    void flatWindowWithNoSurvivorsResolvesToNoSlots() {
        assertThat(MaskedValues.flatSlots(10, 4, new BitSet())).isEmpty();
    }

    @Test
    void flatWindowIgnoresSurvivorBitsAtOrBeyondItsRowCount() {
        BitSet surviving = new BitSet();
        surviving.set(1);
        surviving.set(4);
        surviving.set(9);
        assertThat(MaskedValues.flatSlots(10, 4, surviving)).containsExactly(11);
    }

    @Test
    void emptyFlatWindowResolvesToNoSlots() {
        BitSet surviving = new BitSet();
        surviving.set(0);
        assertThat(MaskedValues.flatSlots(10, 0, surviving)).isEmpty();
    }

    @Test
    void repeatedWindowExpandsASurvivingRowToItsWholeRun() {
        // rows: [a] [b b b] [] as rep levels 0 | 0 1 1 | 0 over five slots starting at page slot 5
        Levels repLevels = Levels.of(new int[] {9, 9, 9, 9, 9, 0, 0, 1, 1, 0});
        BitSet surviving = new BitSet();
        surviving.set(1);
        assertThat(MaskedValues.repeatedSlots(5, repLevels, 5, 5, surviving)).containsExactly(6, 7, 8);
    }

    @Test
    void repeatedWindowAnswersRelativeToTheBaseNotToTheWindowStart() {
        Levels repLevels = Levels.of(new int[] {9, 9, 9, 9, 9, 0, 0, 1, 1, 0});
        BitSet surviving = new BitSet();
        surviving.set(1);
        assertThat(MaskedValues.repeatedSlots(0, repLevels, 5, 5, surviving)).containsExactly(1, 2, 3);
    }

    @Test
    void repeatedWindowKeepsSurvivingRunsInPageOrder() {
        Levels repLevels = Levels.of(new int[] {0, 1, 0, 0, 1, 1});
        BitSet surviving = new BitSet();
        surviving.set(0);
        surviving.set(2);
        assertThat(MaskedValues.repeatedSlots(0, repLevels, 0, 6, surviving)).containsExactly(0, 1, 3, 4, 5);
    }

    @Test
    void baseOffsetsEverySlot() {
        Levels repLevels = Levels.of(new int[] {0, 1});
        BitSet surviving = new BitSet();
        surviving.set(0);
        assertThat(MaskedValues.repeatedSlots(100, repLevels, 0, 2, surviving)).containsExactly(100, 101);
    }

    @Test
    void continuationWindowTreatsTheAlreadyOpenRowAsRowZero() {
        // rows: [a a a] [b b] as rep levels 0 1 1 | 0 1, entered mid-row at page slot 1
        Levels repLevels = Levels.of(new int[] {0, 1, 1, 0, 1});
        BitSet surviving = new BitSet();
        surviving.set(0);
        assertThat(MaskedValues.repeatedSlots(1, repLevels, 1, 4, surviving)).containsExactly(1, 2);
    }

    @Test
    void continuationWindowNumbersTheRowAfterTheOpenOneAsRowOne() {
        Levels repLevels = Levels.of(new int[] {0, 1, 1, 0, 1});
        BitSet surviving = new BitSet();
        surviving.set(1);
        assertThat(MaskedValues.repeatedSlots(1, repLevels, 1, 4, surviving)).containsExactly(3, 4);
    }

    @Test
    void continuationWindowWhoseOpenRowIsFilteredOutResolvesToNoSlots() {
        Levels repLevels = Levels.of(new int[] {0, 1, 1});
        assertThat(MaskedValues.repeatedSlots(1, repLevels, 1, 2, new BitSet())).isEmpty();
    }

    @Test
    void emptyRepeatedWindowResolvesToNoSlots() {
        Levels repLevels = Levels.of(new int[] {0, 1});
        BitSet surviving = new BitSet();
        surviving.set(0);
        assertThat(MaskedValues.repeatedSlots(10, repLevels, 0, 0, surviving)).isEmpty();
    }

    @Test
    void repeatedWindowReachingPastTheLevelCountIsRejected() {
        Levels repLevels = Levels.of(new int[] {0, 1});
        BitSet surviving = new BitSet();
        surviving.set(0);
        assertThatThrownBy(() -> MaskedValues.repeatedSlots(0, repLevels, 1, 3, surviving))
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void levelSliceGathersWindowRelativePositions() {
        LevelSlice slice = new LevelSlice(Levels.of(new int[] {7, 8, 9, 10}), 1, 3);
        Levels gathered = slice.gather(new int[] {0, 2});
        assertThat(gathered.size()).isEqualTo(2);
        assertThat(gathered.get(0)).isEqualTo(8);
        assertThat(gathered.get(1)).isEqualTo(10);
    }

    @Test
    void levelSliceRejectsAPositionPastTheWindow() {
        LevelSlice slice = new LevelSlice(Levels.of(new int[] {7, 8, 9, 10}), 1, 2);
        assertThatThrownBy(() -> slice.gather(new int[] {0, 2})).isInstanceOf(IndexOutOfBoundsException.class);
    }
}
