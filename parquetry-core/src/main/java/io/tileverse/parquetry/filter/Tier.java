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
package io.tileverse.parquetry.filter;

/**
 * Filter pipeline tiers, in the order the engine applies them per row group.
 *
 * <p>Each tier produces a {@link PruningDecision} that either eliminates the row group, narrows the set of surviving
 * rows, signals "no opinion" (NotApplied), or passes the row group through unchanged. Tiers later in the order
 * generally cost more I/O, so a hit at an earlier tier saves work downstream.
 */
public enum Tier {

    /** Per-column min/max/nullCount carried in {@code ColumnMetaData.statistics}. Trivial CPU, no extra I/O. */
    STATS,

    /**
     * Per-row-group geometry bounding box from native GeoParquet 2.0 {@code GeospatialStatistics}. Same granularity and
     * cost as {@link #STATS}: a numeric box comparison, no extra I/O.
     */
    SPATIAL,

    /** Per-column dictionary pages. One I/O round trip per equality-bearing column. */
    DICTIONARY,

    /** Per-page min/max + null counts from ColumnIndex/OffsetIndex. One I/O for the indexes. */
    COLUMN_INDEX,

    /** Bloom filter sections for equality predicates. One I/O per bloom-filter-bearing column. */
    BLOOM_FILTER,

    /** Per-record evaluation during record assembly. Inline; no extra I/O. */
    RECORD_LEVEL
}
