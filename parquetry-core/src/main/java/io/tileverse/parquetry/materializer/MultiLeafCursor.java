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
package io.tileverse.parquetry.materializer;

import io.tileverse.parquetry.batch.LeafOrder;
import io.tileverse.parquetry.batch.Levels;

/**
 * A forward cursor over one list span's elements that tracks every descendant leaf's position at once. Sequential
 * iteration over the span's elements scans each leaf's rep stream exactly once; an index at or behind the cursor
 * rescans from the span start, the same policy as the single-leaf cursor in the row view.
 *
 * <p>An element of this list is located by a REAL opener: within a span, the start of element {@code i} is the
 * {@code i}-th entry whose repetition is at most the element repetition level AND whose definition is at least the
 * element definition level. The repetition-only check alone is not enough: in an unannotated structural list a phantom
 * opener (a null element wrapper) sits in the definition gap between the group's and the element's definition levels,
 * opening at the element repetition level yet describing no real element, and real elements can follow it in the same
 * span. Skipping such openers by the definition check keeps each leaf aligned to the same real-element ordinal. The
 * element ends at the next opener of any kind (repetition at most the element repetition level), because a phantom
 * still bounds the element it precedes.
 */
final class MultiLeafCursor {

    private final LeafWindows span;
    private final LeafOrder order;
    private final int elementRepLevel;
    private final int elementDefLevel;

    private int elementIndex = -1;
    private final int[] starts;
    private final int[] ends;

    MultiLeafCursor(LeafWindows span, int elementRepLevel, int elementDefLevel) {
        this.span = span;
        this.order = span.order();
        this.elementRepLevel = elementRepLevel;
        this.elementDefLevel = elementDefLevel;
        int count = span.leafCount();
        this.starts = new int[count];
        this.ends = new int[count];
    }

    /**
     * The per-leaf {@code [start, end)} windows of the element at ordinal {@code index}, sharing the span's leaf order.
     * Each leaf's window starts at its {@code index}-th real opener and ends at the next opener of any kind or the span
     * end; the returned arrays are copies, valid after the cursor moves on.
     */
    LeafWindows windowsAt(int index) {
        if (elementIndex < 0 || index < elementIndex) {
            moveToFirstElement();
        }
        while (elementIndex < index) {
            advanceOneElement();
        }
        return span.withSpans(starts.clone(), ends.clone());
    }

    /** Element 0 starts at the first real opener of every leaf; its end is the next opener of any kind. */
    private void moveToFirstElement() {
        for (int leaf = 0; leaf < starts.length; leaf++) {
            starts[leaf] = nextRealOpener(leaf, span.startAt(leaf));
            ends[leaf] = nextOpener(leaf, starts[leaf] + 1);
        }
        elementIndex = 0;
    }

    /** The next element starts at the next real opener at or after the current element's end. */
    private void advanceOneElement() {
        for (int leaf = 0; leaf < starts.length; leaf++) {
            starts[leaf] = nextRealOpener(leaf, ends[leaf]);
            ends[leaf] = nextOpener(leaf, starts[leaf] + 1);
        }
        elementIndex++;
    }

    /**
     * The first real opener of leaf {@code leaf} at or after {@code from}, or the leaf's span end when none remains.
     */
    private int nextRealOpener(int leaf, int from) {
        int limit = span.endAt(leaf);
        Levels rep = order.repLevels(leaf);
        Levels def = order.defLevels(leaf);
        for (int entry = from; entry < limit; entry++) {
            if (rep.get(entry) <= elementRepLevel && def.get(entry) >= elementDefLevel) {
                return entry;
            }
        }
        return limit;
    }

    /** The first opener of any kind of leaf {@code leaf} at or after {@code from}, or the leaf's span end. */
    private int nextOpener(int leaf, int from) {
        int limit = span.endAt(leaf);
        Levels rep = order.repLevels(leaf);
        for (int entry = from; entry < limit; entry++) {
            if (rep.get(entry) <= elementRepLevel) {
                return entry;
            }
        }
        return limit;
    }
}
