/*
 * Copyright (c) 2026 Tileverse.io
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

import java.util.List;
import java.util.NoSuchElementException;

import io.tileverse.parquetry.batch.ParquetRecordBatch;

import lombok.NonNull;

/**
 * The heap-backed batches of one fully decoded row group, emitted in file order. Produced by a decode worker (or the
 * inline fallback) and consumed on the read thread.
 *
 * <p>Batches are emitted via {@link #next()}; an emitted batch is owned by the consumer, which closes it when its rows
 * are drained. {@link #close()} closes only the batches not yet emitted, so closing the row group on advance (after all
 * batches were emitted) is a no-op, while closing it early releases the undelivered batches.
 */
final class DecodedRowGroup implements AutoCloseable {

    private final List<ParquetRecordBatch> batches;
    private int emitted;

    DecodedRowGroup(@NonNull List<ParquetRecordBatch> batches) {
        this.batches = List.copyOf(batches);
    }

    boolean hasNext() {
        return emitted < batches.size();
    }

    ParquetRecordBatch next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more batches in this row group");
        }
        ParquetRecordBatch batch = batches.get(emitted);
        emitted++;
        return batch;
    }

    @Override
    public void close() {
        while (emitted < batches.size()) {
            try {
                batches.get(emitted).close();
            } catch (RuntimeException ignored) {
                // best-effort; close remaining batches even if one throws
            }
            emitted++;
        }
    }
}
