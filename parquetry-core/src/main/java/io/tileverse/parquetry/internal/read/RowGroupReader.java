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

/**
 * Decodes the batches of one row group on the calling thread. A {@link BatchRowGroupReader} decodes the projected
 * columns over the rows to read. A {@link MaskedScanRowGroupReader} evaluates the predicate per window and decodes the
 * output columns of the surviving rows only.
 */
sealed interface RowGroupReader extends AutoCloseable permits BatchRowGroupReader, MaskedScanRowGroupReader {

    /** True while more batches remain. */
    boolean hasMore();

    /** Decodes and returns the next batch; the caller owns it and closes it. */
    ParquetRecordBatch nextBatch();

    /** Data pages decoded and skipped while emitting the batches, for read observability. */
    BatchRowGroupReader.PageCounts pageCounts();

    /** Logical rows run through decode while emitting the batches, for read observability. */
    long rowsProduced();

    /** Nanoseconds spent evaluating the predicate over the decoded rows, zero for a reader that evaluates none. */
    default long recordFilterNanos() {
        return 0L;
    }

    @Override
    void close();
}
