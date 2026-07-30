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
package io.tileverse.parquetry.internal.read.page;

import java.lang.foreign.MemorySegment;

import lombok.NonNull;

/**
 * One byte-contiguous stretch of a column chunk's data pages, as fetched: a read-only zero-copy view into a fetch
 * buffer plus the data-page ordinal of the stretch's first page. A whole chunk is the single run with ordinal zero; a
 * page-narrowed fetch produces one run per surviving-page stretch.
 *
 * @param segment read-only view of the run's compressed page bytes, page headers included
 * @param firstPageOrdinal offset-index ordinal of the run's first data page, which seeds {@link PageCursor} row
 *     alignment
 */
public record DataPageRun(@NonNull MemorySegment segment, int firstPageOrdinal) {

    public DataPageRun {
        if (firstPageOrdinal < 0) {
            throw new IllegalArgumentException("firstPageOrdinal must be >= 0, got " + firstPageOrdinal);
        }
    }
}
