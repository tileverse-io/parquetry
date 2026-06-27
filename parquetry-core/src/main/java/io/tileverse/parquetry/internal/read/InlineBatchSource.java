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

import io.tileverse.parquetry.columnar.ParquetRecordBatch;

import lombok.NonNull;

/**
 * Decodes one row group synchronously on the consumer thread, one batch at a time. Used for the in-order current row
 * group and the inline fallback: it never reserves decode budget and keeps at most one batch live.
 *
 * <p>When {@code wantsTimings} is on, each driver call is bracketed with {@code System.nanoTime()} and the elapsed
 * decode time accumulates into {@link #decodeNanos()}. When off (the default), no clock is read.
 */
final class InlineBatchSource implements BatchSource {

    private final RowGroupBatchDriver driver;
    private final boolean wantsTimings;

    private long decodeNanos;

    InlineBatchSource(@NonNull RowGroupBatchDriver driver) {
        this(driver, false);
    }

    InlineBatchSource(@NonNull RowGroupBatchDriver driver, boolean wantsTimings) {
        this.driver = driver;
        this.wantsTimings = wantsTimings;
    }

    // hasMore is timed too: the late-materializing driver runs its phase-1 predicate scan on the first hasMore call.
    @Override
    public boolean hasNext() {
        if (!wantsTimings) {
            return driver.hasMore();
        }
        long start = System.nanoTime();
        boolean more = driver.hasMore();
        decodeNanos += System.nanoTime() - start;
        return more;
    }

    @Override
    public ParquetRecordBatch next() {
        if (!wantsTimings) {
            return driver.nextBatch();
        }
        long start = System.nanoTime();
        ParquetRecordBatch batch = driver.nextBatch();
        decodeNanos += System.nanoTime() - start;
        return batch;
    }

    @Override
    public BatchRowGroupReader.PageCounts pageCounts() {
        return driver.pageCounts();
    }

    @Override
    public long rowsProduced() {
        return driver.rowsProduced();
    }

    @Override
    public long decodeNanos() {
        return decodeNanos;
    }

    @Override
    public void close() {
        driver.close();
    }
}
