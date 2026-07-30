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

import java.util.List;
import java.util.Optional;

import lombok.NonNull;

/**
 * Everything one projected column was planned to read: its dictionary prefix when that was planned separately, and one
 * slice per run of data pages, in file order.
 *
 * <p>A chunk fetched whole has an empty prefix and the single run that spans the chunk; a page-narrowed fetch has one
 * run per byte-contiguous stretch of surviving pages, with the dictionary reached through the prefix.
 *
 * @param dictionaryPrefix the chunk's leading bytes up to the first data page, when planned as its own range
 * @param runs one slice per planned run of data pages, ordered by file offset (which is data-page ordinal order)
 */
record ColumnSlices(
        @NonNull Optional<ColumnSlice> dictionaryPrefix,
        @NonNull List<RunSlice> runs) {

    ColumnSlices {
        runs = List.copyOf(runs);
    }
}
