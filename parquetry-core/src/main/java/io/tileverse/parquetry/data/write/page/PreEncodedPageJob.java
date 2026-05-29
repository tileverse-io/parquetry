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

import java.lang.foreign.MemorySegment;

import org.jspecify.annotations.Nullable;

import io.tileverse.parquetry.format.Encoding;

import lombok.NonNull;

/**
 * Input bag for a single pre-encoded page emission via {@link PageWriter}.
 *
 * <p>Mirror of {@link PageEncodeJob} for callers that produce the encoded value bytes themselves -- typically a
 * page-level encoder like {@link DictionaryAttemptEncoder} that has already serialized the values.
 * {@link #valuesEncoding()} is placed verbatim on the page header; {@link #encodedValueBytes()} is written after the
 * rep/def level bytes (V2) or concatenated with them before compression (V1). {@link #rowCount()} is ignored by the V1
 * writer.
 *
 * @param encodedValueBytes already-encoded value bytes for the page
 * @param valuesEncoding encoding marker to place in the page header
 * @param payloadValueCount total cell count for this page, including nulls
 * @param nullCount number of null cells in this page
 * @param rowCount number of top-level rows in this page; ignored by V1
 * @param repetitionLevels per-cell repetition levels; {@code null} for columns with {@code maxRepLevel = 0}
 * @param definitionLevels per-cell definition levels; {@code null} for columns with {@code maxDefLevel = 0}
 * @param pageStats per-page min/max/null snapshot; may be {@code null} to omit page-level statistics
 */
// S6218: write-side input bag, never compared by equals.
@SuppressWarnings("java:S6218")
public record PreEncodedPageJob(
        @NonNull MemorySegment encodedValueBytes,
        @NonNull Encoding valuesEncoding,
        int payloadValueCount,
        int nullCount,
        int rowCount,
        @Nullable int[] repetitionLevels,
        @Nullable int[] definitionLevels,
        PageStatistics pageStats) {

    public PreEncodedPageJob {
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
