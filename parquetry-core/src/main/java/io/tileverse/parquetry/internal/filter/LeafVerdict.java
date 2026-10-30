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

/**
 * A verdict of {@link LeafVerdicts} on the rows of a unit for a predicate leaf. It tells how many rows match, and names
 * the fact behind it for the explain plan.
 */
enum LeafVerdict {
    NO_STATISTICS(Matches.UNKNOWN, "no statistics"),
    NO_BOUNDS(Matches.UNKNOWN, "min/max missing"),
    NO_NULL_COUNT(Matches.UNKNOWN, "nullCount missing"),
    LITERAL_OF_ANOTHER_TYPE(Matches.UNKNOWN, "literal of another type than the bounds"),

    NOTHING_LISTED(Matches.NONE, "no value listed"),
    ONLY_NULLS(Matches.NONE, "only nulls"),
    ONLY_NAN(Matches.NONE, "the non-null cells are NaN"),
    NO_NAN(Matches.NONE, "no cell is NaN"),
    NAN_ORDERS_NOTHING(Matches.NONE, "no value is ordered against NaN"),
    OUTSIDE_BOUNDS(Matches.NONE, "value outside [min, max]"),
    EACH_CELL_EQUAL(Matches.NONE, "each non-null cell equals the value"),
    NO_NULLS(Matches.NONE, "no nulls"),

    WITHIN_BOUNDS(Matches.SOME, "value within [min, max]"),
    BOUNDS_MATCH_PARTLY(Matches.SOME, "[min, max] matches but may not cover each row"),
    NAN_OUTSIDE_BOUNDS(Matches.SOME, "NaN cells lie outside [min, max]"),
    NAN_BESIDE_NULLS(Matches.SOME, "the non-null cells are NaN, beside nulls"),
    NO_NAN_BESIDE_NULLS(Matches.SOME, "no cell is NaN, beside nulls"),
    SOME_NULLS(Matches.SOME, "some nulls"),

    BOUNDS_MATCH(Matches.ALL, "[min, max] matches, no nulls"),
    SINGLE_VALUE_MATCHES(Matches.ALL, "single distinct value matches"),
    EACH_CELL_NAN(Matches.ALL, "the cells are NaN, no nulls"),
    NO_NAN_NOR_NULL(Matches.ALL, "no cell is NaN, no nulls"),
    ONLY_NULLS_MATCH(Matches.ALL, "only nulls"),
    NO_NULLS_MATCH(Matches.ALL, "no nulls");

    /** How many rows of a unit match a leaf. */
    enum Matches {
        /** No row of the unit matches. */
        NONE,
        /** Some rows may match, and others may not. */
        SOME,
        /** Each row of the unit matches. */
        ALL,
        /** The statistics of the unit cannot tell. */
        UNKNOWN
    }

    private final Matches matches;

    private final String reason;

    LeafVerdict(Matches matches, String reason) {
        this.matches = matches;
        this.reason = reason;
    }

    Matches matches() {
        return matches;
    }

    String reason() {
        return reason;
    }
}
