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
package io.tileverse.parquetry.data.read;

import java.util.Optional;

import io.tileverse.parquetry.data.read.page.Dictionary;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.schema.ColumnPath;

import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import lombok.NonNull;

/**
 * One column chunk's compressed bytes fetched into a pooled buffer plus the metadata needed to walk it page by page.
 *
 * <p>This is the unit produced by {@link ColumnFetcher} and consumed by the per-row-group readers. The
 * {@code compressedBuffer} holds the entire column chunk (dictionary page if present + all data pages) still
 * compressed. The dictionary - small, shared across data pages - is decoded eagerly during the fetch and held here;
 * data pages are decoded lazily by the column reader, one at a time, so a multi-page chunk never resides whole in
 * decompressed form. This is the streaming memory contract described in the package documentation.
 *
 * <p>The chunk is {@link AutoCloseable}: closing it returns the borrowed compressed buffer to the pool. Closing is
 * idempotent (the underlying {@code PooledByteBuffer.close()} is). The owning row-group reader is expected to call
 * close exactly once per chunk along every success, exhaustion, and exception path.
 *
 * @param path the leaf column path this chunk belongs to (file schema path)
 * @param metadata the on-disk {@link ColumnMetaData} for the chunk
 * @param maxRepetitionLevel max repetition level computed from the file schema at this leaf
 * @param maxDefinitionLevel max definition level computed from the file schema at this leaf
 * @param compressedBuffer the pooled buffer holding the entire compressed column chunk (dict page + data pages),
 *     ready-to-read (flipped); position/limit on {@code .buffer()} represent the slice the caller should slice from
 * @param dictionary the decoded dictionary page if the column chunk has one; otherwise empty
 */
record FetchedColumnChunk(
        @NonNull ColumnPath path,
        @NonNull ColumnMetaData metadata,
        int maxRepetitionLevel,
        int maxDefinitionLevel,
        @NonNull PooledByteBuffer compressedBuffer,
        @NonNull Optional<Dictionary<?>> dictionary)
        implements AutoCloseable {

    public FetchedColumnChunk {
        if (maxRepetitionLevel < 0) {
            throw new IllegalArgumentException("maxRepetitionLevel must be >= 0, got " + maxRepetitionLevel);
        }
        if (maxDefinitionLevel < 0) {
            throw new IllegalArgumentException("maxDefinitionLevel must be >= 0, got " + maxDefinitionLevel);
        }
    }

    /** Returns the borrowed compressed buffer to the pool. Idempotent. */
    @Override
    public void close() {
        compressedBuffer.close();
    }
}
