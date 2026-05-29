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
package io.tileverse.parquetry.data.write.page;

import org.jspecify.annotations.Nullable;

import lombok.NonNull;

/**
 * Input bag for a single page emission via {@link PageWriter}.
 *
 * <p>{@link #payloadValues()} carries only the non-null values; the encoder consumes {@code payloadValueCount() -
 * nullCount()} entries from it. {@link #payloadValueCount()} is the total cell count including nulls -- the value that
 * lands in the page header's {@code num_values}. {@link #rowCount()} is the number of top-level rows the page covers
 * (equal to {@code payloadValueCount} for flat columns; only V2 carries it explicitly). {@link #repetitionLevels()} /
 * {@link #definitionLevels()} are {@code null} when the column has no repetition / definition.
 *
 * @param payloadValues carrier holding the non-null values to encode (primitive specialized for the encoder)
 * @param payloadValueCount total cell count for this page, including nulls
 * @param nullCount number of null cells in this page
 * @param rowCount number of top-level rows in this page; ignored by the V1 writer
 * @param repetitionLevels per-cell repetition levels; {@code null} for columns with {@code maxRepLevel = 0}
 * @param definitionLevels per-cell definition levels; {@code null} for columns with {@code maxDefLevel = 0}
 * @param valuesEncoder encoder for the column's value bytes
 * @param pageStats per-page min/max/null snapshot; may be {@code null} to omit page-level statistics
 */
// S6218: write-side input bag, never compared by equals.
@SuppressWarnings("java:S6218")
public record PageEncodeJob(
        Object payloadValues,
        int payloadValueCount,
        int nullCount,
        int rowCount,
        @Nullable int[] repetitionLevels,
        @Nullable int[] definitionLevels,
        @NonNull Encoder<?> valuesEncoder,
        PageStatistics pageStats) {

    public PageEncodeJob {
        if (payloadValueCount < 0) {
            throw new IllegalArgumentException("payloadValueCount must be non-negative; got " + payloadValueCount);
        }
        if (nullCount < 0 || nullCount > payloadValueCount) {
            throw new IllegalArgumentException(
                    "nullCount must be in [0, payloadValueCount]; got " + nullCount + " of " + payloadValueCount);
        }
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must be non-negative; got " + rowCount);
        }
    }
}
