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

import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * What the statistics of a column chunk or of a page tell about its NaN cells. NaN cells lie outside the min and max of
 * a floating-point column holding numbers, and a pruning tier rules on them from the recorded NaN count: a count of
 * zero rules them out, and a chunk or page holds nothing else when its NaN and null counts add up to its values.
 */
public enum NaNCells {

    /** NaN cells may be present. The bounds cover the other cells. */
    POSSIBLE,

    /** No cell is NaN: the bounds cover the non-null cells. */
    ABSENT,

    /** The non-null cells are all NaN: there is no number, and no bound over numbers. */
    ALL;

    /**
     * Whether the reader compares the cells of a column of {@code kind} as numbers: FLOAT and DOUBLE. It compares the
     * cells of a FLOAT16 column as bytes, and a byte literal matches a NaN cell.
     */
    static boolean comparesAsNumbers(PrimitiveKind kind) {
        return kind == PrimitiveKind.FLOAT || kind == PrimitiveKind.DOUBLE;
    }

    /**
     * What the NaN and null counts of a page tell about its NaN cells, in a column holding one value per row.
     *
     * @param nanCount the NaN count of the page, empty when the index records none
     * @param nullCount the null count of the page, empty when the index records none
     * @param rows the number of rows of the page
     */
    static NaNCells ofPageCounts(OptionalLong nanCount, OptionalLong nullCount, long rows) {
        if (nanCount.isEmpty()) {
            return POSSIBLE;
        }
        long nans = nanCount.getAsLong();
        if (nans == 0L) {
            return ABSENT;
        }
        boolean onlyNaN = nullCount.isPresent() && makeUpThePage(nans, nullCount.getAsLong(), rows);
        return onlyNaN ? ALL : POSSIBLE;
    }

    /**
     * Whether {@code nans} NaN cells and {@code nulls} null cells fill the {@code rows} rows of a page. Negative counts
     * describe no page.
     */
    private static boolean makeUpThePage(long nans, long nulls, long rows) {
        if (nans < 0L || nulls < 0L || nans > rows) {
            return false;
        }
        return rows - nans == nulls;
    }

    /**
     * This knowledge checked against the bounds recorded next to it. A NaN bound contradicts {@link #ABSENT} and a
     * number bound contradicts {@link #ALL}; statistics contradicting each other are trusted for neither claim.
     */
    NaNCells checkedAgainst(Optional<Value> min, Optional<Value> max) {
        boolean nanBound = min.filter(ValueComparison::isNaN).isPresent()
                || max.filter(ValueComparison::isNaN).isPresent();
        boolean numberBound = min.filter(NaNCells::isNumber).isPresent()
                || max.filter(NaNCells::isNumber).isPresent();
        return switch (this) {
            case ABSENT -> nanBound ? POSSIBLE : ABSENT;
            case ALL -> numberBound ? POSSIBLE : ALL;
            case POSSIBLE -> POSSIBLE;
        };
    }

    private static boolean isNumber(Value bound) {
        return !ValueComparison.isNaN(bound);
    }
}
