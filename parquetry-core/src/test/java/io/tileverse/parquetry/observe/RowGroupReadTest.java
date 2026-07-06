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

import java.util.Optional;

import org.junit.jupiter.api.Test;

class RowGroupReadTest {

    @Test
    void combineAddsCountsAndFetchStatsAndKeepsLowestIndex() {
        RowGroupRead a = new RowGroupRead(0, 100, 40, 8, 2, new FetchStats(10, 0, 1, 1, 0, 2), Optional.empty());
        RowGroupRead b = new RowGroupRead(3, 50, 10, 4, 1, new FetchStats(5, 0, 1, 1, 0, 1), Optional.empty());

        RowGroupRead sum = a.combine(b);

        assertThat(sum.rowGroupIndex()).isEqualTo(0);
        assertThat(sum.rowsDecoded()).isEqualTo(150);
        assertThat(sum.rowsMatched()).isEqualTo(50);
        assertThat(sum.pagesDecoded()).isEqualTo(12);
        assertThat(sum.pagesPruned()).isEqualTo(3);
        assertThat(sum.fetch()).isEqualTo(new FetchStats(15, 0, 2, 2, 0, 3));
    }

    @Test
    void combineSumsTimingsWhenBothPresent() {
        PhaseTimings ta = new PhaseTimings(1, 2, 3, 4);
        PhaseTimings tb = new PhaseTimings(10, 20, 30, 40);
        RowGroupRead a = new RowGroupRead(0, 1, 1, 1, 0, FetchStats.EMPTY, Optional.of(ta));
        RowGroupRead b = new RowGroupRead(1, 1, 1, 1, 0, FetchStats.EMPTY, Optional.of(tb));

        RowGroupRead sum = a.combine(b);

        assertThat(sum.timings()).contains(new PhaseTimings(11, 22, 33, 44));
    }

    @Test
    void combineKeepsTimingsWhenOnlyOneSidePresent() {
        PhaseTimings t = new PhaseTimings(1, 2, 3, 4);
        RowGroupRead withTimings = new RowGroupRead(0, 1, 1, 1, 0, FetchStats.EMPTY, Optional.of(t));
        RowGroupRead withoutTimings = new RowGroupRead(1, 1, 1, 1, 0, FetchStats.EMPTY, Optional.empty());

        assertThat(withTimings.combine(withoutTimings).timings()).contains(t);
        assertThat(withoutTimings.combine(withTimings).timings()).contains(t);
    }
}
