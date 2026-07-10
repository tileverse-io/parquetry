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

import java.lang.foreign.MemorySegment;
import java.util.Optional;

import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ColumnPath;

import lombok.NonNull;

/**
 * One projected column chunk as a read-only view into a coalesced fetch buffer, plus the metadata needed to walk it
 * page by page.
 *
 * <p>This is the unit produced by {@link RowGroupFetcher} and consumed by the per-row-group readers.
 * {@code compressedSegment} covers the chunk's data-page region (after any dictionary page was consumed during the
 * fetch); the data pages remain compressed and are decoded lazily by the column reader, one at a time. The decoded
 * dictionary (small, shared across data pages) is held here directly.
 *
 * <p>This view does not own memory. Its segment is valid only while the owning {@link RowGroupFetch} is open; that
 * fetch returns the pooled segments to the {@link SegmentPool} when it is closed.
 *
 * @param path the leaf column path this chunk belongs to (file schema path)
 * @param metadata the on-disk {@link ColumnMetaData} for the chunk
 * @param maxRepetitionLevel max repetition level computed from the file schema at this leaf
 * @param maxDefinitionLevel max definition level computed from the file schema at this leaf
 * @param compressedSegment read-only view of the chunk's compressed data-page region
 * @param dictionary the decoded dictionary page if the column chunk has one; otherwise empty
 */
record FetchedColumnChunk(
        @NonNull ColumnPath path,
        @NonNull ColumnMetaData metadata,
        int maxRepetitionLevel,
        int maxDefinitionLevel,
        @NonNull MemorySegment compressedSegment,
        @NonNull Optional<Dictionary<?>> dictionary) {

    FetchedColumnChunk {
        if (maxRepetitionLevel < 0) {
            throw new IllegalArgumentException("maxRepetitionLevel must be >= 0, got " + maxRepetitionLevel);
        }
        if (maxDefinitionLevel < 0) {
            throw new IllegalArgumentException("maxDefinitionLevel must be >= 0, got " + maxDefinitionLevel);
        }
    }
}
