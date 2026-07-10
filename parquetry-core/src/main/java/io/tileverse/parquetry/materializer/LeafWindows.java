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
package io.tileverse.parquetry.materializer;

import io.tileverse.parquetry.columnar.IntSequence;
import io.tileverse.parquetry.columnar.LeafOrder;
import io.tileverse.parquetry.columnar.LevelSource;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * The {@code [start, end)} entry window of a list span in each of its descendant leaves. Each descendant leaf keeps its
 * own rep/def stream with its own entry count; one logical span (a row's list, or one struct element within it) covers
 * a different entry range in each leaf, and this holds all of them as two int arrays over the vector's batch-constant
 * {@link LeafOrder}.
 *
 * <p>This is a flyweight: it owns only its two position arrays and borrows the leaf order from the vector, which makes
 * building a per-row or per-element window cheap. Openers across leaves align by ordinal: the {@code i}-th opener in
 * any two descendant leaves describes the same element slot, because every leaf shares the ancestor chain that the
 * repetition level steps through.
 */
public final class LeafWindows {

    private final LeafOrder order;
    private final int[] starts;
    private final int[] ends;

    private LeafWindows(LeafOrder order, int[] starts, int[] ends) {
        this.order = order;
        this.starts = starts;
        this.ends = ends;
    }

    /** Every descendant leaf's full window for {@code rowIndex}: {@code [rowStarts[row], rowStarts[row + 1])}. */
    static LeafWindows forRow(LevelSource vec, int rowIndex) {
        LeafOrder order = vec.leafOrder();
        int count = order.leafCount();
        int[] starts = new int[count];
        int[] ends = new int[count];
        for (int i = 0; i < count; i++) {
            IntSequence rowStarts = order.rowStarts(i);
            starts[i] = rowStarts.get(rowIndex);
            ends[i] = rowStarts.get(rowIndex + 1);
        }
        return new LeafWindows(order, starts, ends);
    }

    /**
     * A span over the same leaves with new per-leaf bounds, sharing this instance's leaf order. The caller hands over
     * freshly built arrays aligned to that order and must not mutate them afterwards.
     */
    LeafWindows withSpans(int[] newStarts, int[] newEnds) {
        return new LeafWindows(order, newStarts, newEnds);
    }

    /** The leaf order these spans index into. */
    LeafOrder order() {
        return order;
    }

    /** Number of leaves this span covers. */
    int leafCount() {
        return order.leafCount();
    }

    /**
     * The span start of the leaf at ordinal {@code i} of the shared {@link LeafOrder}. The hot read path uses this with
     * a leaf ordinal resolved once at meta construction, skipping the per-access path lookup that {@link #start} pays.
     */
    public int startAt(int i) {
        return starts[i];
    }

    /** The span end (exclusive) of the leaf at ordinal {@code i} of the shared {@link LeafOrder}. */
    public int endAt(int i) {
        return ends[i];
    }

    /** The span start of the leaf at {@code path}; the leaf must be one of this span's leaves. */
    public int start(ColumnPath path) {
        return starts[order.ordinalOf(path)];
    }

    /** The span end (exclusive) of the leaf at {@code path}; the leaf must be one of this span's leaves. */
    public int end(ColumnPath path) {
        return ends[order.ordinalOf(path)];
    }
}
