/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.format;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;

/**
 * Per-page min/max statistics for one column chunk; mirror of {@code ColumnIndex} in {@code parquet.thrift}.
 *
 * <p>One entry per data page, parallel to {@link OffsetIndex}. {@link #nullPages()} flags pages that contain only
 * nulls; {@link #minValues()} and {@link #maxValues()} hold the typed bounds in physical encoding. When
 * {@link #boundaryOrder()} is {@link BoundaryOrder#ASCENDING} or {@link BoundaryOrder#DESCENDING}, a reader can
 * binary-search the bounds instead of scanning every page. Stored at {@link ColumnChunk#columnIndexOffset()}.
 *
 * @param nullPages per-page flags; {@code true} indicates the page contains only null values, so its min/max entries
 *     are meaningless placeholders
 * @param minValues per-page lower bound for non-null values, in the column's physical encoding ({@code min_values} in
 *     the thrift schema)
 * @param maxValues per-page upper bound for non-null values, in the column's physical encoding ({@code max_values} in
 *     the thrift schema)
 * @param boundaryOrder declares whether {@link #minValues()} and {@link #maxValues()} are both monotonically ordered
 *     across pages, enabling binary search
 * @param nullCounts per-page null value count; empty when the writer chose not to record null counts
 * @param repetitionLevelHistograms repetition-level histograms for every page, concatenated in page order
 *     ({@code repetition_level_histograms} in the thrift schema)
 * @param definitionLevelHistograms definition-level histograms for every page, concatenated in page order
 *     ({@code definition_level_histograms} in the thrift schema)
 * @see ParquetFormat#readColumnIndex(io.tileverse.storage.RangeReader, long, int)
 */
public record ColumnIndex(
        List<Boolean> nullPages,
        List<MemorySegment> minValues,
        List<MemorySegment> maxValues,
        BoundaryOrder boundaryOrder,
        Optional<List<Long>> nullCounts,
        Optional<List<Long>> repetitionLevelHistograms,
        Optional<List<Long>> definitionLevelHistograms) {

    public ColumnIndex {
        nullPages = List.copyOf(nullPages);
        minValues = freezeBoundsList(minValues);
        maxValues = freezeBoundsList(maxValues);
        nullCounts = nullCounts.map(List::copyOf);
        repetitionLevelHistograms = repetitionLevelHistograms.map(List::copyOf);
        definitionLevelHistograms = definitionLevelHistograms.map(List::copyOf);
    }

    private static List<MemorySegment> freezeBoundsList(List<MemorySegment> bounds) {
        return bounds.stream().map(ColumnIndex::asReadOnly).toList();
    }

    private static MemorySegment asReadOnly(MemorySegment segment) {
        return segment.isReadOnly() ? segment : segment.asReadOnly();
    }
}
