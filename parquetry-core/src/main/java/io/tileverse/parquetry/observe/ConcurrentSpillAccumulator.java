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
package io.tileverse.parquetry.observe;

import java.util.concurrent.atomic.LongAdder;

/** Thread-safe active {@link SpillAccumulator}; a spill is recorded by the producer, a restore by the consumer. */
final class ConcurrentSpillAccumulator implements SpillAccumulator {

    private final LongAdder batchesSpilled = new LongAdder();
    private final LongAdder bytesSpilled = new LongAdder();
    private final LongAdder batchesRestored = new LongAdder();
    private final LongAdder restoreNanos = new LongAdder();
    private final LongAdder spillsRejectedDiskFull = new LongAdder();

    @Override
    public void recordSpill(long bytes) {
        batchesSpilled.increment();
        bytesSpilled.add(bytes);
    }

    @Override
    public void recordSpillRejectedDiskFull() {
        spillsRejectedDiskFull.increment();
    }

    @Override
    public void recordRestore(long nanos) {
        batchesRestored.increment();
        restoreNanos.add(nanos);
    }

    @Override
    public SpillStats snapshot() {
        return new SpillStats(
                batchesSpilled.sum(),
                bytesSpilled.sum(),
                batchesRestored.sum(),
                restoreNanos.sum(),
                spillsRejectedDiskFull.sum());
    }
}
