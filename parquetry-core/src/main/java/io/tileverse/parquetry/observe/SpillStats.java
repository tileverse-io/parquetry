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
package io.tileverse.parquetry.observe;

/**
 * Immutable tally of the decode-time spill activity for one read, mergeable across files and concurrent reads via
 * {@link #combine}. Spill fires when a decoded batch does not fit the decode budget: the producer writes it to disk
 * rather than holding more heap than the budget allows, and the consumer reads it back when it reaches that position.
 *
 * <p>{@code batchesSpilled} and {@code bytesSpilled} count batches written to the spill file and their serialized size.
 * {@code batchesRestored} and {@code restoreNanos} count the read-back and its wall time.
 * {@code spillsRejectedDiskFull} counts the times a batch needed to spill but found the disk budget full and parked on
 * heap instead - the signal that both heap and disk budgets were saturated at once.
 */
public record SpillStats(
        long batchesSpilled, long bytesSpilled, long batchesRestored, long restoreNanos, long spillsRejectedDiskFull) {

    public static final SpillStats EMPTY = new SpillStats(0, 0, 0, 0, 0);

    /** Whether any spill activity was recorded; the renderers omit the spill line when this is false. */
    public boolean hasActivity() {
        return batchesSpilled > 0 || bytesSpilled > 0 || batchesRestored > 0 || spillsRejectedDiskFull > 0;
    }

    public SpillStats combine(SpillStats other) {
        return new SpillStats(
                batchesSpilled + other.batchesSpilled,
                bytesSpilled + other.bytesSpilled,
                batchesRestored + other.batchesRestored,
                restoreNanos + other.restoreNanos,
                spillsRejectedDiskFull + other.spillsRejectedDiskFull);
    }
}
