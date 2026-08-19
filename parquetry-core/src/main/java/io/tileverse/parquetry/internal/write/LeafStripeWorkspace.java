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
package io.tileverse.parquetry.internal.write;

import java.util.Arrays;

/**
 * One leaf column's striping scratch: the repetition and definition levels of the batch being shredded, and the source
 * ordinals of the values that are present. The backing arrays grow on demand and are reused across batches, which is
 * why no reader may retain a backing past the batch that filled it.
 *
 * <p>Two shapes fill the scratch. A repeated leaf appends occurrence by occurrence through {@link #addEntry}, and
 * {@link #entryCount()} / {@link #valueCount()} then report how much of each backing is live. A non-repeated leaf
 * instead writes one definition level per row straight into the row-sized backings, leaving both counters at zero. The
 * meaningful prefix of every backing is therefore the one the {@link StripedLeaf} built from it declares, not the
 * counters.
 *
 * <p>A workspace belongs to exactly one leaf and is written by whichever thread strips that leaf. Reuse is safe because
 * the writer joins every column's append before starting the next batch.
 */
final class LeafStripeWorkspace {

    private int[] reps = new int[16];
    private int[] defs = new int[16];
    private int[] kept = new int[16];
    private int[] rowOrdinals = new int[16];
    private int entryCount;
    private int valueCount;

    void reset() {
        entryCount = 0;
        valueCount = 0;
    }

    void addEntry(int rep, int def, int keptOrdinal) {
        ensureEntryCapacity();
        reps[entryCount] = rep;
        defs[entryCount] = def;
        entryCount++;
        if (keptOrdinal >= 0) {
            ensureValueCapacity();
            kept[valueCount] = keptOrdinal;
            valueCount++;
        }
    }

    /** The definition-level backing for the non-repeated path, sized for one entry per row. */
    int[] defBackingForRows(int rowCount) {
        if (defs.length < rowCount) {
            defs = new int[rowCount];
        }
        return defs;
    }

    /** The present-row ordinal backing for the non-repeated path, sized for at most one value per row. */
    int[] rowOrdinalBacking(int rowCount) {
        if (rowOrdinals.length < rowCount) {
            rowOrdinals = new int[rowCount];
        }
        return rowOrdinals;
    }

    int[] repBacking() {
        return reps;
    }

    int[] defBacking() {
        return defs;
    }

    int[] keptBacking() {
        return kept;
    }

    int entryCount() {
        return entryCount;
    }

    int valueCount() {
        return valueCount;
    }

    private void ensureEntryCapacity() {
        if (entryCount == reps.length) {
            reps = Arrays.copyOf(reps, reps.length * 2);
            defs = Arrays.copyOf(defs, defs.length * 2);
        }
    }

    private void ensureValueCapacity() {
        if (valueCount == kept.length) {
            kept = Arrays.copyOf(kept, kept.length * 2);
        }
    }
}
