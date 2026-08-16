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
package io.tileverse.parquetry.columnar;

import java.util.Map;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * The batch-constant navigation state shared by every row view over one {@link LevelSource}: a fixed leaf order and,
 * indexed by that order, each leaf's repetition stream, definition stream, and row-start boundary. Built once in the
 * vector factory and read straight by the per-cell navigation, which lets {@code LeafWindows} and
 * {@code MultiLeafCursor} hold nothing but their own int position arrays.
 *
 * <p>Leaves align by ordinal across the parallel arrays: ordinal {@code i} names the same leaf in {@link #pathAt(int)},
 * {@link #repLevels(int)}, {@link #defLevels(int)}, and {@link #rowStarts(int)}. The order is the {@link LeafOrdinals}
 * assignment the instance was built from; navigation engines that share one instance agree on it.
 *
 * <p>The level and offset sequences read batch-owned off-heap memory. An instance is valid only while the owning batch
 * is open; reading any sequence after the batch closes is undefined.
 */
public final class LeafOrder {

    private final LeafOrdinals ordinals;
    private final Levels[] repLevels;
    private final Levels[] defLevels;
    private final IntSequence[] rowStarts;

    private LeafOrder(LeafOrdinals ordinals, Levels[] repLevels, Levels[] defLevels, IntSequence[] rowStarts) {
        this.ordinals = ordinals;
        this.repLevels = repLevels;
        this.defLevels = defLevels;
        this.rowStarts = rowStarts;
    }

    /**
     * Captures each leaf's level streams and row-start boundary at the ordinal {@code ordinals} assigns it.
     * {@code levels} must hold a window for every leaf of the assignment.
     */
    static LeafOrder of(LeafOrdinals ordinals, Map<ColumnPath, LeafLevels> levels) {
        int count = ordinals.leafCount();
        Levels[] repLevels = new Levels[count];
        Levels[] defLevels = new Levels[count];
        IntSequence[] rowStarts = new IntSequence[count];
        for (int ordinal = 0; ordinal < count; ordinal++) {
            ColumnPath path = ordinals.pathAt(ordinal);
            LeafLevels leafLevels = levels.get(path);
            if (leafLevels == null) {
                throw new IllegalArgumentException("no level window for leaf " + path.dot());
            }
            repLevels[ordinal] = leafLevels.repLevels();
            defLevels[ordinal] = leafLevels.defLevels();
            rowStarts[ordinal] = leafLevels.rowStarts();
        }
        return new LeafOrder(ordinals, repLevels, defLevels, rowStarts);
    }

    /** Number of leaves in this order. */
    public int leafCount() {
        return ordinals.leafCount();
    }

    /** The leaf path at ordinal {@code i}. */
    public ColumnPath pathAt(int i) {
        return ordinals.pathAt(i);
    }

    /** The ordinal of {@code path} in this order; the path must be one of this order's leaves. */
    public int ordinalOf(ColumnPath path) {
        return ordinals.ordinalOf(path);
    }

    /** The repetition stream of the leaf at ordinal {@code i}. */
    public Levels repLevels(int i) {
        return repLevels[i];
    }

    /** The definition stream of the leaf at ordinal {@code i}. */
    public Levels defLevels(int i) {
        return defLevels[i];
    }

    /** The row-start boundary of the leaf at ordinal {@code i}. */
    public IntSequence rowStarts(int i) {
        return rowStarts[i];
    }
}
