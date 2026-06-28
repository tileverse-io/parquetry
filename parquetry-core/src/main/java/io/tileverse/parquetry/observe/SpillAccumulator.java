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

/**
 * Decode-time spill tally for one read. Call the record methods unconditionally at the spill store; install
 * {@link #NONE} when no observer is attached and the calls become no-ops the JIT inlines away. The active form is
 * thread-safe: a spill is recorded on the producer thread and a restore on the consumer thread.
 *
 * <p>Unlike {@link FetchAccumulator}, the active form is installed whenever an observer is attached, not only on the
 * analyze drain: spill is a rare event whose accounting cost is negligible against the disk I/O it accompanies, and the
 * spill that matters is the one driven by several concurrent reads contending for one decode budget - which a single
 * analyze drain cannot reproduce.
 */
public sealed interface SpillAccumulator permits ConcurrentSpillAccumulator, SpillAccumulator.NoOp {

    SpillAccumulator NONE = new NoOp();

    /** A batch was written to the spill file; {@code bytes} is its serialized size. */
    void recordSpill(long bytes);

    /** A batch needed to spill but found the disk budget full and parked on heap instead. */
    void recordSpillRejectedDiskFull();

    /** A spilled batch was read back; {@code nanos} is the wall time of the read-and-rebuild. */
    void recordRestore(long nanos);

    SpillStats snapshot();

    static SpillAccumulator active() {
        return new ConcurrentSpillAccumulator();
    }

    /** No-op null object: zero state, zero work, empty stats. */
    final class NoOp implements SpillAccumulator {

        private NoOp() {}

        @Override
        public void recordSpill(long bytes) {
            // intentional no-op: the null object discards every measurement
        }

        @Override
        public void recordSpillRejectedDiskFull() {
            // intentional no-op: the null object discards every measurement
        }

        @Override
        public void recordRestore(long nanos) {
            // intentional no-op: the null object discards every measurement
        }

        @Override
        public SpillStats snapshot() {
            return SpillStats.EMPTY;
        }
    }
}
