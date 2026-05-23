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

import java.io.IOException;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import io.tileverse.io.ByteBufferPool;

/**
 * Fetches one column chunk's compressed bytes into a pooled buffer and decodes its dictionary if present.
 *
 * <p>The interface is functional so tests can substitute a stub that returns a pre-built {@link FetchedColumnChunk}
 * backed by an in-memory pooled buffer, without inventing a Parquet write path. The production implementation, obtained
 * via {@link #real(RangeReader, ParquetSchema, ByteBufferPool)}, performs the actual range read against
 * {@code RangeReader} and decodes the (small) dictionary page eagerly. Data pages stay compressed inside the returned
 * chunk's pooled buffer and are decoded lazily by the column reader as it walks pages - the streaming memory contract
 * described in the package documentation.
 *
 * <h2>Lifecycle</h2>
 *
 * <p>A successful {@link #fetch(ColumnChunk, ColumnPath)} call transfers ownership of the borrowed compressed buffer to
 * the returned {@link FetchedColumnChunk}; closing that chunk returns the buffer to the pool. On failure the
 * implementation must close any partially borrowed buffer before propagating the exception.
 */
@FunctionalInterface
public interface ColumnFetcher {

    /**
     * Fetches a column chunk's compressed bytes into a pooled buffer.
     *
     * @param chunk the on-disk {@link ColumnChunk} (must carry inline {@code metaData})
     * @param path the leaf column path (used to resolve max rep/def levels against the file schema)
     * @return a {@link FetchedColumnChunk} whose {@code compressedBuffer} the caller must close
     * @throws IOException if the underlying range read fails or the chunk metadata is malformed
     */
    @MustBeClosed
    FetchedColumnChunk fetch(ColumnChunk chunk, ColumnPath path) throws IOException;

    /**
     * Returns the production {@link ColumnFetcher} that performs real I/O via {@code reader} and borrows compressed
     * buffers from {@code pool}. The returned implementation is a thin wrapper around {@link RealColumnFetcher}: it
     * range-reads the entire compressed column chunk, decodes the dictionary page (if any) eagerly, and resolves the
     * leaf's max rep/def levels against {@code fileSchema}. Data pages remain compressed in the pooled buffer until the
     * column reader walks them.
     */
    static ColumnFetcher real(RangeReader reader, ParquetSchema fileSchema, ByteBufferPool pool) {
        return new RealColumnFetcher(reader, fileSchema, pool);
    }
}
