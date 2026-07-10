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
package io.tileverse.parquetry.observe;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class FetchAccumulatorTest {

    @Test
    void recordsBytesAndFetchCountPerPurpose() {
        FetchAccumulator acc = FetchAccumulator.active();

        acc.add(FetchPurpose.PAGES, 100);
        acc.add(FetchPurpose.PAGES, 50);
        acc.add(FetchPurpose.COLUMN_INDEX, 20);
        acc.add(FetchPurpose.BLOOM_FILTER, 8);

        FetchStats stats = acc.snapshot();

        assertThat(stats.pageBytes()).isEqualTo(150);
        assertThat(stats.columnIndexBytes()).isEqualTo(20);
        assertThat(stats.bloomFilterBytes()).isEqualTo(8);
        assertThat(stats.fetchCount()).isEqualTo(4);
    }

    @Test
    void activeSnapshotIsZeroBeforeAnyRecord() {
        assertThat(FetchAccumulator.active().snapshot()).isEqualTo(FetchStats.EMPTY);
    }

    @Test
    void noneIsANoOpAndSnapshotsEmpty() {
        FetchAccumulator none = FetchAccumulator.NONE;

        none.add(FetchPurpose.PAGES, 999);

        assertThat(none.snapshot()).isEqualTo(FetchStats.EMPTY);
    }
}
