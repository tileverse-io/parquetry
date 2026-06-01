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

import lombok.NonNull;

/**
 * Decodes one row group synchronously on the consumer thread, one batch at a time. Used for the in-order current row
 * group and the inline fallback: it never reserves decode budget and keeps at most one batch live.
 */
final class InlineBatchSource implements BatchSource {

    private final RowGroupBatchDriver driver;

    InlineBatchSource(@NonNull RowGroupBatchDriver driver) {
        this.driver = driver;
    }

    @Override
    public boolean hasNext() {
        return driver.hasMore();
    }

    @Override
    public ParquetRecordBatch next() {
        return driver.nextBatch();
    }

    @Override
    public void close() {
        driver.close();
    }
}
