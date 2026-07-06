/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
 * Yields one row group's batches in order to the consuming read thread. An inline source decodes synchronously on the
 * consumer thread; a streaming source drains a worker-fed hand-off. {@link #close()} releases undelivered batches and
 * the underlying fetch.
 */
sealed interface BatchSource extends AutoCloseable permits InlineBatchSource, StreamingBatchSource {

    /** True while more batches remain; may block in the streaming source until the producer makes progress. */
    boolean hasNext();

    /** Returns the next batch in order; the consumer owns it and closes it. */
    ParquetRecordBatch next();

    /**
     * Data pages decoded and skipped for this row group, for read observability. Read only after the source is drained
     * or closed; the streaming source's producer has then finished and its counters are stable.
     */
    BatchRowGroupReader.PageCounts pageCounts();

    /**
     * Logical rows actually run through decode for this row group, for read observability. Read only after the source
     * is drained or closed; the streaming source's producer has then finished and its counters are stable.
     */
    long rowsProduced();

    /**
     * Nanoseconds spent decoding this row group's batches, or zero when the observer did not opt into timings. Read
     * only after the source is drained or closed, under the same join as {@link #pageCounts()}.
     */
    long decodeNanos();

    @Override
    void close();
}
