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

/**
 * What the statistics of a column chunk or of a page tell about its NaN cells. NaN cells lie outside the min and max of
 * a floating-point column, and a pruning tier decides for them from the recorded NaN count: a count of zero rules them
 * out, and a chunk holds nothing else when its NaN and null counts add up to its values.
 */
public enum NaNCells {

    /** NaN cells may be present. The bounds cover the other cells. */
    POSSIBLE,

    /** No cell is NaN: the bounds cover the non-null cells. */
    ABSENT,

    /** The non-null cells are all NaN: there is no number, and no bound over numbers. */
    ALL;

    /**
     * What the column index tells about the NaN cells of one page.
     *
     * @param nanCount the NaN count of the page, empty when the index records none
     */
    static NaNCells ofPage(OptionalLong nanCount, Value min, Value max) {
        boolean zeroCount = nanCount.isPresent() && nanCount.getAsLong() == 0L;
        NaNCells counted = zeroCount ? ABSENT : POSSIBLE;
        return counted.checkedAgainst(Optional.of(min), Optional.of(max));
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
