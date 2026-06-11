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

import org.junit.jupiter.api.Test;

class IntScratchTest {

    @Test
    void reusesTheSameArrayWhileCapacitySuffices() {
        IntScratch scratch = new IntScratch();
        int[] first = scratch.array(100);
        assertThat(first.length).isGreaterThanOrEqualTo(100);
        int[] second = scratch.array(64);
        assertThat(second).as("a smaller request reuses the same array").isSameAs(first);
        int[] third = scratch.array(first.length);
        assertThat(third).as("an exact-capacity request reuses the same array").isSameAs(first);
    }

    @Test
    void growsToTheNextPowerOfTwo() {
        IntScratch scratch = new IntScratch();
        int[] small = scratch.array(10);
        assertThat(small.length).as("floor capacity").isEqualTo(1024);
        int[] grown = scratch.array(1500);
        assertThat(grown).isNotSameAs(small);
        assertThat(grown.length).isEqualTo(2048);
        assertThat(scratch.array(2048)).as("growth is sticky").isSameAs(grown);
    }

    @Test
    void zeroLengthRequestIsServed() {
        IntScratch scratch = new IntScratch();
        assertThat(scratch.array(0).length).isGreaterThanOrEqualTo(0);
    }
}
