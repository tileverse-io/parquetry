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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An idempotent handle that returns a previously reserved number of bytes to a {@link DecodeBudget} exactly once. A
 * decoded batch releases its reservation when it is closed, after the consumer drains its rows.
 */
public final class DecodeReservation {

    /** Sentinel for batches that were never reserved: the mandatory current row group and inline fallbacks. */
    public static final DecodeReservation NONE = new DecodeReservation(null, 0);

    private final DecodeBudget budget;
    private final long bytes;
    private final AtomicBoolean released = new AtomicBoolean(false);

    public DecodeReservation(DecodeBudget budget, long bytes) {
        this.budget = budget;
        this.bytes = bytes;
    }

    public long bytes() {
        return bytes;
    }

    /** Returns the reserved bytes to the budget. Safe to call more than once; only the first call has an effect. */
    public void release() {
        if (budget != null && bytes > 0 && released.compareAndSet(false, true)) {
            budget.release(bytes);
        }
    }
}
