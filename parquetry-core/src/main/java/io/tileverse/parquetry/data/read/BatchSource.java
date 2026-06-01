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
package io.tileverse.parquetry.data.read;

import io.tileverse.parquetry.batch.ParquetRecordBatch;

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

    /** Marks this row group as the in-order current one; only the streaming source reacts (stops reserving budget). */
    default void promote() {}

    @Override
    void close();
}
