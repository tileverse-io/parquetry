/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.observe;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SpillStatsTest {

    @Test
    void emptyHasNoActivity() {
        assertThat(SpillStats.EMPTY.hasActivity()).isFalse();
    }

    @Test
    void anyNonZeroCountIsActivity() {
        assertThat(new SpillStats(1, 0, 0, 0, 0).hasActivity()).isTrue();
        assertThat(new SpillStats(0, 0, 0, 0, 1).hasActivity()).isTrue();
    }

    @Test
    void restoreNanosAloneIsNotActivity() {
        // restore time without a restored batch is not a meaningful signal on its own
        assertThat(new SpillStats(0, 0, 0, 500, 0).hasActivity()).isFalse();
    }

    @Test
    void combineSumsEveryField() {
        SpillStats a = new SpillStats(1, 100, 1, 50, 2);
        SpillStats b = new SpillStats(3, 300, 2, 70, 1);

        SpillStats sum = a.combine(b);

        assertThat(sum.batchesSpilled()).isEqualTo(4);
        assertThat(sum.bytesSpilled()).isEqualTo(400);
        assertThat(sum.batchesRestored()).isEqualTo(3);
        assertThat(sum.restoreNanos()).isEqualTo(120);
        assertThat(sum.spillsRejectedDiskFull()).isEqualTo(3);
    }
}
