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
    void recordsBytesCallsRangesAndTransportPerPurpose() {
        FetchAccumulator acc = FetchAccumulator.active();

        acc.add(FetchPurpose.PAGES, 100);
        acc.add(FetchPurpose.PAGES, 50);
        acc.add(FetchPurpose.COLUMN_INDEX, 20);
        acc.add(FetchPurpose.BLOOM_FILTER, 8);
        acc.add(FetchPurpose.PAGES, 300, 6, 2, 512, 128);

        FetchStats stats = acc.snapshot();

        assertThat(stats.pageBytes()).isEqualTo(450);
        assertThat(stats.columnIndexBytes()).isEqualTo(20);
        assertThat(stats.bloomFilterBytes()).isEqualTo(8);
        assertThat(stats.fetchCount())
                .as("one count per call to the byte source")
                .isEqualTo(5);
        assertThat(stats.requestCount())
                .as("four single-range calls plus one batch of six ranges")
                .isEqualTo(10);
        assertThat(stats.backendFetches())
                .as("only a call that reports its cost contributes backend requests")
                .isEqualTo(2);
        assertThat(stats.bytesTransferred()).isEqualTo(512);
        assertThat(stats.bytesFromCache()).isEqualTo(128);
    }

    @Test
    void oneCallSplitAcrossPurposesIsCountedOnce() {
        FetchAccumulator acc = FetchAccumulator.active();

        acc.add(FetchPurpose.COLUMN_INDEX, 20, 2, 1, 44, 0);
        acc.addRangesOfSameCall(FetchPurpose.OFFSET_INDEX, 24, 2);

        FetchStats stats = acc.snapshot();

        assertThat(stats.columnIndexBytes()).isEqualTo(20);
        assertThat(stats.offsetIndexBytes())
                .as("the further ranges land in their own purpose's bytes")
                .isEqualTo(24);
        assertThat(stats.fetchCount())
                .as("two purposes served by one call are one call")
                .isEqualTo(1);
        assertThat(stats.requestCount()).isEqualTo(4);
        assertThat(stats.backendFetches())
                .as("the further ranges report no cost of their own; the call already did")
                .isEqualTo(1);
        assertThat(stats.bytesTransferred()).isEqualTo(44);
    }

    @Test
    void activeSnapshotIsZeroBeforeAnyRecord() {
        assertThat(FetchAccumulator.active().snapshot()).isEqualTo(FetchStats.EMPTY);
    }

    @Test
    void noneIsANoOpAndSnapshotsEmpty() {
        FetchAccumulator none = FetchAccumulator.NONE;

        none.add(FetchPurpose.PAGES, 999);
        none.addRangesOfSameCall(FetchPurpose.OFFSET_INDEX, 999, 3);

        assertThat(none.snapshot()).isEqualTo(FetchStats.EMPTY);
    }
}
