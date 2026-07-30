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
package io.tileverse.parquetry.internal.read;

import io.tileverse.parquetry.schema.ColumnPath;

import lombok.NonNull;

/**
 * One plannable byte range of a column chunk, the {@link CoalescingFetchPlanner}'s input unit: a whole chunk, one run
 * of surviving data pages, or the chunk's dictionary prefix. A unit is never split across coalesced ranges.
 *
 * @param path the leaf column the range belongs to
 * @param fileOffset absolute offset of the range's first byte in the file
 * @param length the range's byte length
 * @param firstPageOrdinal offset-index ordinal of the range's first data page; meaningful only for a non-prefix unit,
 *     and zero for a whole chunk
 * @param dictionaryPrefix whether the range is the chunk's dictionary prefix rather than data pages
 */
record FetchUnit(
        @NonNull ColumnPath path, long fileOffset, int length, int firstPageOrdinal, boolean dictionaryPrefix) {

    FetchUnit {
        if (fileOffset < 0) {
            throw new IllegalArgumentException("fileOffset must be >= 0, got " + fileOffset);
        }
        if (length <= 0) {
            throw new IllegalArgumentException("length must be > 0, got " + length);
        }
        if (firstPageOrdinal < 0) {
            throw new IllegalArgumentException("firstPageOrdinal must be >= 0, got " + firstPageOrdinal);
        }
    }
}
