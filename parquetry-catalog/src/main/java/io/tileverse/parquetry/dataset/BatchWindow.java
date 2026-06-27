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
package io.tileverse.parquetry.dataset;

import java.util.OptionalLong;
import java.util.stream.Gatherer;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;

/**
 * Applies an offset/limit window to an ordered stream of exact (already predicate-filtered) batches. Skips whole
 * batches that fall entirely before the offset, slices the one batch the offset straddles, passes whole batches through
 * until the limit, and slices the one batch the limit straddles before finishing. At most two batches are ever sliced
 * per whole read; the rest pass through or are skipped.
 *
 * <p>The window runs as a stateful {@link Gatherer} so it sees the cross-file batch stream as one sequence: applied
 * inside the {@code readBatches(Query)} default, virtual dispatch makes it wrap each multi-file dataset's cross-file
 * concatenation, and a single outermost counter spans every file. Finishing early returns {@code false} from the
 * integrator, which stops pulling; closing the result stream then tears down the in-flight file and the lazy per-file
 * concatenation never opens the next one.
 *
 * <p>A batch skipped before the offset is closed here (the window owns each batch it pulls). A sliced batch's view owns
 * the batch it slices; batches never pulled are closed by the upstream stream's {@code onClose} when the read stream is
 * closed.
 */
final class BatchWindow {

    private BatchWindow() {}

    /** A gatherer that emits only the rows of {@code [offset, offset + limit)} across the batch stream. */
    // S1452: the gatherer's accumulator type is the private State; a wildcard is the only way to keep it hidden.
    @SuppressWarnings("java:S1452")
    static Gatherer<ParquetRecordBatch, ?, ParquetRecordBatch> of(long offset, OptionalLong limit) {
        long initialRemaining = limit.isPresent() ? limit.getAsLong() : UNBOUNDED;
        return Gatherer.<ParquetRecordBatch, State, ParquetRecordBatch>ofSequential(
                () -> new State(offset, initialRemaining),
                (state, batch, downstream) -> integrate(state, batch, downstream));
    }

    private static final long UNBOUNDED = -1L;

    private static final class State {
        private long toSkip;
        private long remaining;

        private State(long toSkip, long remaining) {
            this.toSkip = toSkip;
            this.remaining = remaining;
        }

        private boolean bounded() {
            return remaining != UNBOUNDED;
        }
    }

    private static boolean integrate(
            State state, ParquetRecordBatch batch, Gatherer.Downstream<? super ParquetRecordBatch> downstream) {
        if (state.bounded() && state.remaining == 0) {
            batch.close();
            return false;
        }
        int rowCount = batch.rowCount();
        if (state.toSkip >= rowCount) {
            state.toSkip -= rowCount;
            batch.close();
            return true;
        }
        int from = (int) state.toSkip;
        state.toSkip = 0;
        int available = rowCount - from;
        int take = available;
        boolean finish = false;
        if (state.bounded() && state.remaining <= available) {
            // Finish at the boundary (<=, not <) so the limit-reaching batch is the last one pulled; the next file is
            // never opened. When remaining == available the whole remainder passes with no slice.
            take = (int) state.remaining;
            finish = true;
        }
        ParquetRecordBatch windowed = (from == 0 && take == rowCount) ? batch : batch.slice(from, take);
        if (state.bounded()) {
            state.remaining -= take;
        }
        boolean accepted = downstream.push(windowed);
        return accepted && !finish;
    }
}
