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
package io.tileverse.parquetry.internal.read;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;

import io.tileverse.parquetry.columnar.Levels;

/**
 * Resolves which of a data page's value slots the surviving rows of one window cover.
 *
 * <p>A window is a run of complete logical rows starting at a row boundary. A flat column stores one value slot per
 * row; a repeated column stores a run of slots per row, opened by a repetition level of zero. Both forms answer in
 * ascending slot order, which is the order a page decoder produces values and the order a level gather reindexes them.
 */
final class MaskedValues {

    private MaskedValues() {}

    /**
     * The value slots the surviving rows of a flat window cover, each offset by {@code base}. Row {@code j} of the
     * window is slot {@code base + j}. Bits of {@code survivingRows} at or beyond {@code windowRows} are ignored.
     */
    static int[] flatSlots(int base, int windowRows, BitSet survivingRows) {
        int[] slots = new int[Math.min(windowRows, survivingRows.cardinality())];
        int kept = 0;
        int row = survivingRows.nextSetBit(0);
        while (row >= 0 && row < windowRows) {
            slots[kept++] = base + row;
            row = survivingRows.nextSetBit(row + 1);
        }
        return (kept == slots.length) ? slots : Arrays.copyOf(slots, kept);
    }

    /**
     * The value slots the surviving rows of a repeated window cover, each offset by {@code base}. The window is the
     * {@code windowValues} entries of {@code repLevels} starting at {@code from}; every entry whose repetition level is
     * zero opens the next row, and a surviving row contributes its whole run of slots. The window's first entry is slot
     * {@code base}, hence a caller wanting page-absolute slots passes the window's own page position as {@code base}
     * and a caller wanting window-relative slots passes zero. A window opening mid-row, whose first repetition level is
     * nonzero, treats the already-open row as row zero.
     */
    static int[] repeatedSlots(int base, Levels repLevels, int from, int windowValues, BitSet survivingRows) {
        Objects.checkFromIndexSize(from, windowValues, repLevels.size());
        int[] slots = new int[windowValues];
        int kept = 0;
        int row = openingRow(repLevels, from, windowValues);
        for (int offset = 0; offset < windowValues; offset++) {
            if (repLevels.get(from + offset) == 0) {
                row++;
            }
            if (row >= 0 && survivingRows.get(row)) {
                slots[kept++] = base + offset;
            }
        }
        return (kept == slots.length) ? slots : Arrays.copyOf(slots, kept);
    }

    /**
     * The row index the window's first entry belongs to: zero when the window opens mid-row and its entries continue a
     * row started before {@code from}, otherwise minus one, leaving the first row-start marker to open row zero.
     */
    private static int openingRow(Levels repLevels, int from, int windowValues) {
        boolean opensMidRow = windowValues > 0 && repLevels.get(from) != 0;
        return opensMidRow ? 0 : -1;
    }
}
