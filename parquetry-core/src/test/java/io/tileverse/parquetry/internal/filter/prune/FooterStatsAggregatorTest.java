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
package io.tileverse.parquetry.internal.filter.prune;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.prune.ColumnStatistics;
import io.tileverse.parquetry.internal.filter.StatsEvaluator.ColumnSummary;
import io.tileverse.parquetry.schema.PrimitiveKind;

class FooterStatsAggregatorTest {

    private static ColumnSummary summary(long min, long max, long nulls) {
        return new ColumnSummary(
                PrimitiveKind.INT64,
                Optional.of(new Value.LongVal(min)),
                Optional.of(new Value.LongVal(max)),
                OptionalLong.of(nulls));
    }

    @Test
    void aggregatesLeastMinGreatestMaxSummedNulls() {
        List<Optional<ColumnSummary>> perRowGroup = List.of(
                Optional.of(summary(10, 20, 1)), Optional.of(summary(5, 15, 2)), Optional.of(summary(8, 30, 0)));

        Optional<ColumnStatistics> result = FooterStatsAggregator.aggregate(perRowGroup);

        assertThat(result).isPresent();
        assertThat(result.orElseThrow().min()).contains(new Value.LongVal(5));
        assertThat(result.orElseThrow().max()).contains(new Value.LongVal(30));
        assertThat(result.orElseThrow().nullCount()).hasValue(3);
    }

    @Test
    void anyRowGroupMissingTheColumnYieldsNoStats() {
        List<Optional<ColumnSummary>> perRowGroup = List.of(Optional.of(summary(10, 20, 1)), Optional.empty());

        assertThat(FooterStatsAggregator.aggregate(perRowGroup)).isEmpty();
    }

    @Test
    void missingMinInOneRowGroupDropsMinButKeepsNullCount() {
        ColumnSummary noMin = new ColumnSummary(
                PrimitiveKind.INT64, Optional.empty(), Optional.of(new Value.LongVal(25)), OptionalLong.of(4));
        List<Optional<ColumnSummary>> perRowGroup = List.of(Optional.of(summary(10, 20, 1)), Optional.of(noMin));

        Optional<ColumnStatistics> result = FooterStatsAggregator.aggregate(perRowGroup);

        assertThat(result).isPresent();
        assertThat(result.orElseThrow().min()).isEmpty();
        assertThat(result.orElseThrow().max()).contains(new Value.LongVal(25));
        assertThat(result.orElseThrow().nullCount()).hasValue(5);
    }

    @Test
    void emptyRowGroupListYieldsNoStats() {
        assertThat(FooterStatsAggregator.aggregate(List.of())).isEmpty();
    }
}
