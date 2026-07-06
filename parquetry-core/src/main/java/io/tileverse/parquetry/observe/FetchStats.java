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
package io.tileverse.parquetry.observe;

/**
 * Immutable per-purpose byte tally for one read, mergeable across files via {@link #combine}.
 *
 * <p>{@code pageBytes} counts column-chunk payload fetched for decode (data pages plus any dictionary page coalesced
 * into the chunk). {@code dictionaryBytes} counts bytes fetched specifically for dictionary-based pruning, kept
 * separate from decode payload on purpose.
 */
public record FetchStats(
        long pageBytes,
        long dictionaryBytes,
        long columnIndexBytes,
        long offsetIndexBytes,
        long bloomFilterBytes,
        int fetchCount) {

    public static final FetchStats EMPTY = new FetchStats(0, 0, 0, 0, 0, 0);

    public long totalBytes() {
        return pageBytes + dictionaryBytes + columnIndexBytes + offsetIndexBytes + bloomFilterBytes;
    }

    public FetchStats combine(FetchStats other) {
        return new FetchStats(
                pageBytes + other.pageBytes,
                dictionaryBytes + other.dictionaryBytes,
                columnIndexBytes + other.columnIndexBytes,
                offsetIndexBytes + other.offsetIndexBytes,
                bloomFilterBytes + other.bloomFilterBytes,
                fetchCount + other.fetchCount);
    }
}
