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
package io.tileverse.parquetry.internal.filter;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * The decoded statistics of one column within a unit. A unit is a column chunk of a row group, a page, or a whole file.
 * The statistics hold the kind of the column, its decoded minimum and maximum, its null count, its value count, and
 * what is known of its NaN cells. The value count includes the nulls. For a repeated leaf it counts one value per
 * element and one per empty or null list.
 */
public record ColumnSummary(
        PrimitiveKind kind,
        Optional<Value> min,
        Optional<Value> max,
        OptionalLong nullCount,
        OptionalLong valueCount,
        NaNCells nans) {

    public ColumnSummary {
        Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(min, "min");
        Objects.requireNonNull(max, "max");
        Objects.requireNonNull(nullCount, "nullCount");
        Objects.requireNonNull(valueCount, "valueCount");
        Objects.requireNonNull(nans, "nans");
    }

    /**
     * The summary of a unit from its decoded bounds and its recorded counts. The NaN cells of a FLOAT or DOUBLE column
     * are judged from {@code recordedNans} checked against the bounds. Nothing is known of the NaN cells of other
     * kinds. A NaN bound is left out, and a minimum above its maximum leaves both out; see
     * {@link ValueComparison#inverted}. A null count contradicting the value count or a recorded bound is left out too.
     */
    public static ColumnSummary of(
            PrimitiveKind kind,
            Optional<Value> min,
            Optional<Value> max,
            OptionalLong nullCount,
            OptionalLong valueCount,
            NaNCells recordedNans) {
        NaNCells nans = NaNCells.comparesAsNumbers(kind) ? recordedNans.checkedAgainst(min, max) : NaNCells.POSSIBLE;
        boolean recordsABound = min.isPresent() || max.isPresent();
        OptionalLong nulls = consistentNullCount(nullCount, valueCount, recordsABound);
        Optional<Value> usableMin = min.filter(ColumnSummary::usable);
        Optional<Value> usableMax = max.filter(ColumnSummary::usable);
        if (inverted(usableMin, usableMax)) {
            return new ColumnSummary(kind, Optional.empty(), Optional.empty(), nulls, valueCount, nans);
        }
        return new ColumnSummary(kind, usableMin, usableMax, nulls, valueCount, nans);
    }

    /** Whether each value of the unit is null. Its null count then equals its value count. */
    public boolean onlyNulls() {
        if (nullCount.isEmpty() || valueCount.isEmpty()) {
            return false;
        }
        return nullCount.getAsLong() == valueCount.getAsLong();
    }

    /** Whether the unit is known to hold no null. */
    public boolean noNulls() {
        return nullCount.isPresent() && nullCount.getAsLong() == 0L;
    }

    /** Whether both bounds are known and neither is NaN. A NaN bound orders no number. */
    public boolean bounded() {
        return min.filter(ColumnSummary::usable).isPresent()
                && max.filter(ColumnSummary::usable).isPresent();
    }

    /**
     * The null count, or empty when it contradicts the value count or a recorded bound. A unit cannot hold more nulls
     * than values. A recorded bound proves a value, and the null count of such a unit stays below its value count.
     */
    private static OptionalLong consistentNullCount(
            OptionalLong nullCount, OptionalLong valueCount, boolean recordsABound) {
        if (nullCount.isEmpty() || valueCount.isEmpty()) {
            return nullCount;
        }
        long nulls = nullCount.getAsLong();
        long values = valueCount.getAsLong();
        if (nulls > values || (nulls == values && recordsABound)) {
            return OptionalLong.empty();
        }
        return nullCount;
    }

    private static boolean usable(Value bound) {
        return !ValueComparison.isNaN(bound);
    }

    private static boolean inverted(Optional<Value> min, Optional<Value> max) {
        if (min.isEmpty() || max.isEmpty()) {
            return false;
        }
        return ValueComparison.inverted(min.orElseThrow(), max.orElseThrow());
    }
}
