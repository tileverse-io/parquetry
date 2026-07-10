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

import java.util.Objects;

/**
 * One leaf column's batch-owned repetition/definition level window plus the row-boundary index built over it. A row
 * view navigates a nested column by stepping the level streams; {@link #rowStarts()} gives O(1) access to where each
 * row's entries begin.
 *
 * <p>The three sequences read batch-owned off-heap memory. The instance is valid only while the owning batch is open;
 * reading any sequence after the batch closes is undefined.
 *
 * @param repLevels the repetition levels of this leaf's batch window
 * @param defLevels the definition levels of the same window
 * @param rowStarts entry positions where each row's first entry sits (the level-0 positions of the rep stream), of
 *     length {@code numRows + 1} with the final entry equal to {@link #entryCount()}
 */
public record LeafLevels(Levels repLevels, Levels defLevels, IntSequence rowStarts) {

    public LeafLevels {
        Objects.requireNonNull(repLevels, "repLevels");
        Objects.requireNonNull(defLevels, "defLevels");
        Objects.requireNonNull(rowStarts, "rowStarts");
        requireMatchingLevelSizes(repLevels, defLevels);
        requireFirstRowStartsAtZero(repLevels, rowStarts);
    }

    /** The number of entries in this leaf's window. */
    public int entryCount() {
        return repLevels.size();
    }

    private static void requireMatchingLevelSizes(Levels repLevels, Levels defLevels) {
        if (repLevels.size() != defLevels.size()) {
            throw new IllegalArgumentException(
                    "rep and def level windows differ in size: " + repLevels.size() + " vs " + defLevels.size());
        }
    }

    private static void requireFirstRowStartsAtZero(Levels repLevels, IntSequence rowStarts) {
        boolean emptyWindow = repLevels.size() == 0;
        if (emptyWindow) {
            return;
        }
        if (rowStarts.size() == 0 || rowStarts.get(0) != 0) {
            throw new IllegalArgumentException("a non-empty window's first row must start at entry 0");
        }
    }
}
