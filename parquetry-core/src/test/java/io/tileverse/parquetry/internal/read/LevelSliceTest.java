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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.Levels;

class LevelSliceTest {

    @Test
    void atReadsThroughTheWindowOffset() {
        Levels levels = Levels.of(new int[] {9, 9, 0, 1, 2, 9});
        LevelSlice slice = new LevelSlice(levels, 2, 3);

        assertThat(slice.length()).as("window length").isEqualTo(3);
        assertThat(slice.at(0)).as("first windowed value").isZero();
        assertThat(slice.at(1)).isEqualTo(1);
        assertThat(slice.at(2)).as("last windowed value").isEqualTo(2);
    }

    @Test
    void ofWholeCoversTheEntireSequence() {
        LevelSlice slice = LevelSlice.ofWhole(Levels.of(new int[] {0, 1, 0}));

        assertThat(slice.length()).isEqualTo(3);
        assertThat(slice.at(0)).isZero();
        assertThat(slice.at(1)).isEqualTo(1);
        assertThat(slice.at(2)).isZero();
    }

    @Test
    void emptyWindowHasZeroLength() {
        LevelSlice slice = new LevelSlice(Levels.of(new int[] {0, 1, 0}), 1, 0);

        assertThat(slice.length()).isZero();
    }

    @Test
    void rejectsAWindowPastTheSequenceEnd() {
        Levels levels = Levels.of(new int[] {0, 1, 0});

        assertThatThrownBy(() -> new LevelSlice(levels, 2, 2)).isInstanceOf(IndexOutOfBoundsException.class);
    }
}
