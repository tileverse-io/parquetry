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

import java.util.List;

import io.tileverse.parquetry.io.SegmentPool.Pooled;

import lombok.NonNull;

/**
 * The coalesced segments and column views for one row group. Owns the pooled segments borrowed for its coalesced
 * ranges; {@link #close()} returns them to the pool and releases the budget reservation. The {@link FetchedColumnChunk}
 * views it exposes share these segments and are valid only until close. Close is idempotent.
 */
public final class RowGroupFetch implements AutoCloseable {

    private final List<Pooled> buffers;
    private final List<FetchedColumnChunk> columns;
    private final BudgetReservation reservation;
    private final long fetchNanos;
    private boolean closed;

    RowGroupFetch(
            @NonNull List<Pooled> buffers,
            @NonNull List<FetchedColumnChunk> columns,
            @NonNull BudgetReservation reservation,
            long fetchNanos) {
        this.buffers = List.copyOf(buffers);
        this.columns = List.copyOf(columns);
        this.reservation = reservation;
        this.fetchNanos = fetchNanos;
    }

    /** The projected column chunk views, in projected-leaf order. */
    public List<FetchedColumnChunk> columns() {
        return columns;
    }

    /**
     * Nanoseconds the fetch spent reading and slicing this row group, or zero when the observer did not opt into
     * timings. Final, published to the consumer through the prefetcher's future join.
     */
    public long fetchNanos() {
        return fetchNanos;
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (Pooled buffer : buffers) {
            try {
                buffer.close();
            } catch (RuntimeException _) {
                // best-effort cleanup; a chronic leak shows up in pool accounting
            }
        }
        reservation.release();
    }
}
