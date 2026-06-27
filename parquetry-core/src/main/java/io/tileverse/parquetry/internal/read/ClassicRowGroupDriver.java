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
package io.tileverse.parquetry.internal.read;

import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/** Streams one row group through a {@link BatchRowGroupReader}, the standard (non-late-materialized) decode path. */
final class ClassicRowGroupDriver implements RowGroupBatchDriver {

    private final RowGroupFetch fetch;
    private final BatchRowGroupReader reader;

    @SuppressWarnings("java:S107") // the decode inputs plus the form flag; a parameter object would only relocate the
    // arity
    ClassicRowGroupDriver(
            @NonNull DecodeBufferAllocator decodeBufferAllocator,
            @NonNull RowGroupFetch fetch,
            @NonNull ParquetSchema projectedSchema,
            @NonNull ParquetSchema fileSchema,
            @NonNull OptionalInt batchSizeCap,
            @NonNull Optional<RowMask> rowMask,
            @NonNull BatchForm batchForm) {
        this.fetch = fetch;
        this.reader = new BatchRowGroupReader(
                decodeBufferAllocator, fetch.columns(), projectedSchema, fileSchema, batchSizeCap, rowMask, batchForm);
    }

    @Override
    public boolean hasMore() {
        return reader.hasMore();
    }

    @Override
    @SuppressWarnings("MustBeClosed") // the caller owns and closes the returned batch
    public ParquetRecordBatch nextBatch() {
        return reader.nextBatch();
    }

    @Override
    public BatchRowGroupReader.PageCounts pageCounts() {
        return reader.pageCounts();
    }

    @Override
    public long rowsProduced() {
        return reader.rowsProduced();
    }

    @Override
    public void close() {
        try {
            reader.close();
        } finally {
            fetch.close();
        }
    }
}
