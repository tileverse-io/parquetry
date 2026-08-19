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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

class WriteLeafLevelsTest {

    @Test
    void requiredFlatLeafHasNoStreams() {
        WriteLeafLevels levels = new WriteLeafLevels(0, 0);
        for (int i = 0; i < 5; i++) {
            levels.append(0, 0);
        }
        assertThat(levels.repetitionBacking()).isNull();
        assertThat(levels.definitionBacking()).isNull();
        assertThat(levels.count()).isEqualTo(5);
    }

    @Test
    void optionalFlatLeafHasDefinitionOnly() {
        WriteLeafLevels levels = new WriteLeafLevels(0, 1);
        levels.append(0, 1);
        levels.append(0, 0);
        assertThat(levels.repetitionBacking()).isNull();
        assertThat(Arrays.copyOf(levels.definitionBacking(), levels.count())).containsExactly(1, 0);
    }

    @Test
    void repeatedLeafKeepsRepAndDefInLockstep() {
        WriteLeafLevels levels = new WriteLeafLevels(1, 2);
        levels.append(0, 2);
        levels.append(1, 2);
        levels.append(0, 1);
        assertThat(Arrays.copyOf(levels.repetitionBacking(), levels.count())).containsExactly(0, 1, 0);
        assertThat(Arrays.copyOf(levels.definitionBacking(), levels.count())).containsExactly(2, 2, 1);
    }

    @Test
    void growsPastInitialCapacityAndClearReuses() {
        WriteLeafLevels levels = new WriteLeafLevels(1, 1);
        for (int i = 0; i < 5000; i++) {
            levels.append(i % 2, 1);
        }
        assertThat(levels.count()).isEqualTo(5000);
        assertThat(levels.repetitionBacking()[4999]).isEqualTo(4999 % 2);
        levels.clear();
        assertThat(levels.count()).isZero();
        levels.append(1, 1);
        assertThat(levels.count()).isEqualTo(1);
        assertThat(levels.repetitionBacking()[0]).isEqualTo(1);
    }
}
