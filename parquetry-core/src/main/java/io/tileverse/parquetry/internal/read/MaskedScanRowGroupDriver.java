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

import io.tileverse.parquetry.columnar.ParquetRecordBatch;

import lombok.NonNull;

/** Streams one row group through a {@link MaskedScanRowGroupReader}, the decode path for a filtered read. */
final class MaskedScanRowGroupDriver implements RowGroupBatchDriver {

    private final RowGroupFetch fetch;
    private final MaskedScanRowGroupReader reader;

    MaskedScanRowGroupDriver(@NonNull RowGroupFetch fetch, @NonNull MaskedScanRowGroupReader reader) {
        this.fetch = fetch;
        this.reader = reader;
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

    /** The rows this row group ran through decode: every filter-column row of every window the scan evaluated. */
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
