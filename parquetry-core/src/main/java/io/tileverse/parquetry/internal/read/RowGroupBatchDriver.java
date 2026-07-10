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
import io.tileverse.parquetry.internal.read.BatchRowGroupReader.PageCounts;

/**
 * Pulls one decoded {@link ParquetRecordBatch} at a time from one row group, decoding lazily on the calling thread.
 * Owns the row group's {@link RowGroupFetch}; {@link #close()} closes any open reader and releases the fetch.
 */
// Open (not sealed): tests provide a fixture driver; the production impls are Classic/LateMaterialized.
interface RowGroupBatchDriver extends AutoCloseable {

    /** True while more batches remain. */
    boolean hasMore();

    /** Decodes and returns the next batch; the caller owns it and closes it. */
    ParquetRecordBatch nextBatch();

    /**
     * Data pages decoded and skipped while emitting this row group's output batches, for read observability. The
     * default reports no pages, for fixture drivers that do not decode through a {@link BatchRowGroupReader}; the
     * production drivers override it.
     */
    default PageCounts pageCounts() {
        return PageCounts.ZERO;
    }

    /**
     * Logical rows actually run through decode while emitting this row group's batches, for read observability. The
     * default reports zero, for fixture drivers that do not decode through a {@link BatchRowGroupReader}; the
     * production drivers override it.
     */
    default long rowsProduced() {
        return 0L;
    }

    @Override
    void close();
}
