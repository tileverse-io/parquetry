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

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.filter.SpatialReadProbe.Decision;
import io.tileverse.parquetry.observe.QueryObserver;

class ReadOptionsTest {

    @Test
    void defaultsTurnAllFiltersOn() {
        ReadOptions opts = ReadOptions.DEFAULTS;
        assertThat(opts.useStatsFilter()).isTrue();
        assertThat(opts.useDictionaryFilter()).isTrue();
        assertThat(opts.useColumnIndexFilter()).isTrue();
        assertThat(opts.useBloomFilter()).isTrue();
        assertThat(opts.useRecordLevelFilter()).isTrue();
    }

    @Test
    void builderOverridesIndividualFlags() {
        ReadOptions opts = ReadOptions.builder().useStatsFilter(false).build();
        assertThat(opts.useStatsFilter()).isFalse();
        assertThat(opts.useDictionaryFilter()).isTrue(); // unchanged
    }

    @Test
    void defaultQueryObserverIsNone() {
        assertThat(ReadOptions.DEFAULTS.queryObserver()).isSameAs(QueryObserver.NONE);
    }

    @Test
    void builderSetsQueryObserver() {
        QueryObserver observer = new QueryObserver() {};
        ReadOptions options = ReadOptions.builder().queryObserver(observer).build();
        assertThat(options.queryObserver()).isSameAs(observer);
    }

    @Test
    void zeroBatchSizeRejected() {
        ReadOptions.Builder builder = ReadOptions.builder();
        assertThatThrownBy(() -> builder.batchSize(0)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toBuilderRoundTripsEveryField() {
        QueryObserver observer = new QueryObserver() {};
        ReadOptions original = ReadOptions.builder()
                .useStatsFilter(false)
                .useDictionaryFilter(false)
                .useColumnIndexFilter(false)
                .useBloomFilter(false)
                .useRecordLevelFilter(false)
                .usePageNarrowedFetch(false)
                .queryObserver(observer)
                .batchSize(64)
                .build();

        ReadOptions copy = original.toBuilder().build();

        assertThat(copy).isEqualTo(original);
    }

    @Test
    void toBuilderSwappingObserverPreservesEveryOtherField() {
        ReadOptions original = ReadOptions.builder()
                .useStatsFilter(false)
                .useColumnIndexFilter(false)
                .batchSize(32)
                .build();
        QueryObserver other = new QueryObserver() {};

        ReadOptions swapped = original.toBuilder().queryObserver(other).build();

        assertThat(swapped.queryObserver()).isSameAs(other);
        assertThat(swapped.useStatsFilter()).isFalse();
        assertThat(swapped.useColumnIndexFilter()).isFalse();
        assertThat(swapped.useDictionaryFilter()).isTrue();
        assertThat(swapped.useBloomFilter()).isTrue();
        assertThat(swapped.useRecordLevelFilter()).isTrue();
        assertThat(swapped.usePageNarrowedFetch()).isTrue();
        assertThat(swapped.batchSize()).isEqualTo(original.batchSize());
    }

    @Test
    void pageNarrowedFetchDefaultsOnAndRoundTripsThroughToBuilder() {
        assertThat(ReadOptions.DEFAULTS.usePageNarrowedFetch()).isTrue();
        ReadOptions off = ReadOptions.builder().usePageNarrowedFetch(false).build();
        assertThat(off.usePageNarrowedFetch()).isFalse();
        assertThat(off.toBuilder().build().usePageNarrowedFetch()).isFalse();
    }

    @Test
    void defaultsHoldNoSpatialReadProbe() {
        assertThat(ReadOptions.DEFAULTS.spatialReadProbe()).isEmpty();
    }

    @Test
    void builderAndToBuilderRoundTripTheSpatialReadProbe() {
        SpatialReadProbe probe = (minX, minY, maxX, maxY) -> Decision.keep();
        ReadOptions options = ReadOptions.builder().spatialReadProbe(probe).build();
        assertThat(options.spatialReadProbe()).contains(probe);
        assertThat(options.toBuilder().build().spatialReadProbe()).contains(probe);
    }
}
