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
package io.tileverse.parquetry.internal.filter.prune;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.prune.ColumnStatistics;
import io.tileverse.parquetry.internal.filter.StatsEvaluator.ColumnSummary;
import io.tileverse.parquetry.internal.filter.ValueComparison;

/**
 * Combines one column's per-row-group {@link ColumnSummary} into a single file-level {@link ColumnStatistics}: the
 * least of the row-group minima, the greatest of the maxima, and the sum of the null counts. Each of the three
 * aggregates is evaluated independently and drops to empty as soon as one row group lacks it (a row group with no value
 * for a statistic could hold anything, and the file cannot bound it). The result is empty when no statistic is known at
 * all, which simply means the column does not prune.
 *
 * <p>All summaries are expected to come from the same column of one file, and therefore share a
 * {@link io.tileverse.parquetry.schema.PrimitiveKind}; min/max ordering uses
 * {@link io.tileverse.parquetry.internal.filter.ValueComparison#compareValues} which assumes comparable, same-kind
 * values.
 */
public final class FooterStatsAggregator {

    private FooterStatsAggregator() {}

    public static Optional<ColumnStatistics> aggregate(List<Optional<ColumnSummary>> perRowGroup) {
        if (perRowGroup.isEmpty()) {
            return Optional.empty();
        }
        Optional<Value> min = leastMin(perRowGroup);
        Optional<Value> max = greatestMax(perRowGroup);
        OptionalLong nullCount = summedNullCount(perRowGroup);
        if (min.isEmpty() && max.isEmpty() && nullCount.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new ColumnStatistics(min, max, nullCount));
    }

    private static Optional<Value> leastMin(List<Optional<ColumnSummary>> perRowGroup) {
        return extreme(perRowGroup, ColumnSummary::min, -1);
    }

    private static Optional<Value> greatestMax(List<Optional<ColumnSummary>> perRowGroup) {
        return extreme(perRowGroup, ColumnSummary::max, 1);
    }

    /**
     * The bound across all row groups in the direction given by {@code keepSign}: negative keeps the lesser value,
     * positive the greater. Empty as soon as one row group lacks the bound, since an unbounded row group leaves the
     * file unbounded.
     */
    private static Optional<Value> extreme(
            List<Optional<ColumnSummary>> perRowGroup, Function<ColumnSummary, Optional<Value>> bound, int keepSign) {
        Value extreme = null;
        for (Optional<ColumnSummary> group : perRowGroup) {
            Optional<Value> groupBound = group.flatMap(bound);
            if (groupBound.isEmpty()) {
                return Optional.empty();
            }
            Value candidate = groupBound.orElseThrow();
            if (extreme == null || Integer.signum(ValueComparison.compareValues(candidate, extreme)) == keepSign) {
                extreme = candidate;
            }
        }
        return Optional.ofNullable(extreme);
    }

    private static OptionalLong summedNullCount(List<Optional<ColumnSummary>> perRowGroup) {
        long sum = 0L;
        for (Optional<ColumnSummary> group : perRowGroup) {
            OptionalLong groupNulls = group.map(ColumnSummary::nullCount).orElse(OptionalLong.empty());
            if (groupNulls.isEmpty()) {
                return OptionalLong.empty();
            }
            sum += groupNulls.orElseThrow();
        }
        return OptionalLong.of(sum);
    }
}
