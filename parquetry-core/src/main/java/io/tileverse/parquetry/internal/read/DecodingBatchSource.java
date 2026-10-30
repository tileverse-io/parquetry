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

/**
 * Decodes one fetched row group on the calling thread, one batch at a time, and releases the fetch when closed. The
 * consumer drains it directly for the in-order row group and for the inline fallback. A {@link StreamingBatchSource}
 * drains it on a worker for a speculative decode.
 *
 * <p>When {@code wantsTimings} is on, each reader call is bracketed with {@code System.nanoTime()} and the elapsed
 * decode time accumulates into {@link #decodeNanos()}. When off, no clock is read.
 */
final class DecodingBatchSource implements BatchSource {

    private final RowGroupReader reader;
    private final RowGroupFetch fetch;
    private final boolean wantsTimings;

    private long decodeNanos;

    DecodingBatchSource(@NonNull RowGroupReader reader, @NonNull RowGroupFetch fetch, boolean wantsTimings) {
        this.reader = reader;
        this.fetch = fetch;
        this.wantsTimings = wantsTimings;
    }

    // hasNext is timed too: the masked scan reader computes its first surviving window on the first hasMore call.
    @Override
    public boolean hasNext() {
        if (!wantsTimings) {
            return reader.hasMore();
        }
        long start = System.nanoTime();
        boolean more = reader.hasMore();
        decodeNanos += System.nanoTime() - start;
        return more;
    }

    @Override
    @SuppressWarnings("MustBeClosed") // the caller owns and closes the returned batch
    public ParquetRecordBatch next() {
        if (!wantsTimings) {
            return reader.nextBatch();
        }
        long start = System.nanoTime();
        ParquetRecordBatch batch = reader.nextBatch();
        decodeNanos += System.nanoTime() - start;
        return batch;
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
    public long decodeNanos() {
        return decodeNanos;
    }

    @Override
    public long recordFilterNanos() {
        return reader.recordFilterNanos();
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
