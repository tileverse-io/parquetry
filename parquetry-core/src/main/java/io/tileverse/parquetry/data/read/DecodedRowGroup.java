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
 * One row group's batches, emitted in file order from a {@link BatchSource}. An inline source decodes on the consumer
 * thread; a streaming source drains a worker-fed hand-off. {@link #close()} releases undelivered batches and the
 * underlying fetch.
 *
 * <p>{@code recordEvalRequired} mirrors the owning survivor's flag: {@code true} when the read still has to test each
 * decoded row against the predicate, {@code false} when statistics already proved every row matches (the MATCHED
 * outcome) and per-row evaluation can be skipped.
 */
final class DecodedRowGroup implements AutoCloseable {

    private final BatchSource source;
    private final boolean recordEvalRequired;

    DecodedRowGroup(@NonNull BatchSource source, boolean recordEvalRequired) {
        this.source = source;
        this.recordEvalRequired = recordEvalRequired;
    }

    boolean recordEvalRequired() {
        return recordEvalRequired;
    }

    void promote() {
        source.promote();
    }

    boolean hasNext() {
        return source.hasNext();
    }

    ParquetRecordBatch next() {
        return source.next();
    }

    @Override
    public void close() {
        source.close();
    }
}
