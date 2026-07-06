/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.internal.write.page;

import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.internal.write.ColumnIndexBuilder;
import io.tileverse.parquetry.internal.write.StatisticsAccumulator;

/**
 * Per-page snapshot emitted by {@link StatisticsAccumulator#finishPage()} for consumption by
 * {@link ColumnIndexBuilder}.
 *
 * <p>{@link #min()} and {@link #max()} hold the page's PLAIN-encoded min/max for the column's physical kind as a
 * read-only {@link MemorySegment}. {@link MemorySegment#NULL} marks "no min/max available" -- either the page produced
 * no non-null observation, or the column kind does not support ordering (geometry / geography / INT96).
 *
 * @param min PLAIN-encoded page minimum; {@link MemorySegment#NULL} when no ordering is defined
 * @param max PLAIN-encoded page maximum; {@link MemorySegment#NULL} when no ordering is defined
 * @param nullCount number of null cells observed during the page's accumulation window
 * @param isNullPage {@code true} when every cell in the page was null
 */
public record PageStatistics(MemorySegment min, MemorySegment max, long nullCount, boolean isNullPage) {

    public PageStatistics {
        if (min == null) {
            min = MemorySegment.NULL;
        }
        if (max == null) {
            max = MemorySegment.NULL;
        }
    }
}
