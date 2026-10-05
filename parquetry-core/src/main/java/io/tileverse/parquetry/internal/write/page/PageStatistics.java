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
package io.tileverse.parquetry.internal.write.page;

import java.lang.foreign.MemorySegment;
import java.util.OptionalLong;

import io.tileverse.parquetry.internal.write.ColumnIndexBuilder;
import io.tileverse.parquetry.internal.write.StatisticsAccumulator;

/**
 * Per-page snapshot emitted by {@link StatisticsAccumulator#finishPage()} for consumption by {@link ColumnIndexBuilder}
 * and by the page header.
 *
 * <p>{@link #min()} and {@link #max()} hold the page's PLAIN-encoded bounds as read-only {@link MemorySegment}s, or
 * {@link MemorySegment#NULL} when the page holds no ordered value: only nulls, only NaN, or a column without a defined
 * order.
 *
 * @param min PLAIN-encoded lower bound of the page; {@link MemorySegment#NULL} without an ordered value
 * @param max PLAIN-encoded upper bound of the page; {@link MemorySegment#NULL} without an ordered value
 * @param nullCount number of null cells observed during the page's accumulation window
 * @param isNullPage {@code true} when every cell in the page was null
 * @param nanCount number of NaN cells observed during the page's accumulation window; empty for a column other than
 *     FLOAT, DOUBLE and FLOAT16
 * @param minExact whether {@code min} is held by a cell of the page; false without a bound, and for a zero written with
 *     the sign required by the format over cells of the other sign
 * @param maxExact the {@code max} counterpart of {@code minExact}
 */
public record PageStatistics(
        MemorySegment min,
        MemorySegment max,
        long nullCount,
        boolean isNullPage,
        OptionalLong nanCount,
        boolean minExact,
        boolean maxExact) {

    public PageStatistics {
        if (min == null) {
            min = MemorySegment.NULL;
        }
        if (max == null) {
            max = MemorySegment.NULL;
        }
        if (nanCount == null) {
            nanCount = OptionalLong.empty();
        }
    }
}
