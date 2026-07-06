/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.util.LinkedHashMap;
import java.util.Map;

import io.tileverse.parquetry.columnar.LeafLevels;
import io.tileverse.parquetry.columnar.LevelMapVector;
import io.tileverse.parquetry.columnar.LevelSource;
import io.tileverse.parquetry.columnar.MapMeta;
import io.tileverse.parquetry.record.LevelElementRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Builds a Java {@link Map} from a row of a {@link LevelMapVector} by navigating its batch-owned level streams,
 * mirroring the eager {@link MapMaterializer} contract: a null row yields {@code null}, an empty map yields
 * {@link Map#of()}, and a present non-empty map yields an insertion-ordered {@link LinkedHashMap}. Keys hash and equal
 * by their materialized value; iteration follows the {@code key_value} record order.
 *
 * <p>A map is a repeated {@code key_value} group whose two children read like struct fields. Each pair's per-leaf
 * windows are resolved by a {@link MultiLeafCursor} over the row's {@link LeafWindows}, and the pair's key and value
 * read through a {@link LevelElementRecord} over the {@code key_value} group's two-field tree, which is the same
 * field-read machinery struct elements use.
 */
public final class LevelMapMaterializer {

    private LevelMapMaterializer() {}

    /**
     * Builds a Java {@link Map} from row {@code rowIndex} of {@code vec}. Returns {@code null} when the row's map is
     * null per the vector's validity; a present empty map materializes to {@link Map#of()}.
     *
     * <p>Null is intentional - it distinguishes a null row from an empty map per the empty-vs-null contract.
     */
    @SuppressWarnings({"java:S1452", "java:S1168"})
    public static Map<?, ?> materializeAt(LevelMapVector vec, int rowIndex) {
        if (vec.isNull(rowIndex)) {
            return null;
        }
        return materialize(vec, rowIndex, vec.meta(), LeafWindows.forRow(vec, rowIndex));
    }

    /**
     * Builds the map held in {@code windows} (a top-level row's window, or the window of a map element/field within an
     * enclosing element). A null map yields {@code null} and an empty map yields {@link Map#of()}, determined from the
     * structure leaf's definition level at the window start; the enclosing caller has already settled null-vs-present
     * for a map nested inside a struct field, but determining it here keeps every entry point consistent.
     */
    @SuppressWarnings({"java:S1452", "java:S1168"})
    public static Map<?, ?> materialize(LevelSource vec, int rowIndex, MapMeta meta, LeafWindows windows) {
        ColumnPath structureLeaf = meta.structureLeaf();
        if (structureLeaf == null) {
            return null;
        }
        LeafLevels structureLevels = vec.levels(structureLeaf);
        int windowStart = windows.start(structureLeaf);
        if (windowStart >= structureLevels.entryCount()) {
            return Map.of();
        }
        int def = structureLevels.defLevels().get(windowStart);
        if (def < meta.groupDefLevel()) {
            return null;
        }
        if (def < meta.entryDefLevel()) {
            return Map.of();
        }
        return collectPairs(vec, rowIndex, meta, windows);
    }

    private static Map<Object, Object> collectPairs(LevelSource vec, int rowIndex, MapMeta meta, LeafWindows windows) {
        MultiLeafCursor cursor = new MultiLeafCursor(windows, meta.entryRepLevel(), meta.entryDefLevel());
        LinkedHashMap<Object, Object> result = new LinkedHashMap<>();
        int pairCount = countPairs(vec, meta, windows);
        for (int i = 0; i < pairCount; i++) {
            LeafWindows pairWindows = cursor.windowsAt(i);
            LevelElementRecord pair = new LevelElementRecord(vec, rowIndex, meta.entry(), pairWindows);
            result.put(pair.get(0), pair.get(1));
        }
        return result;
    }

    /**
     * Counts the real key-value pairs in the structure leaf's window: entries that open at the entry repetition level
     * and reach the entry definition level. This matches the cursor, which steps over phantom openers (an absent
     * key_value group below the entry definition level) the same way.
     */
    private static int countPairs(LevelSource vec, MapMeta meta, LeafWindows windows) {
        ColumnPath structureLeaf = meta.structureLeaf();
        LeafLevels levels = vec.levels(structureLeaf);
        int start = windows.start(structureLeaf);
        int end = windows.end(structureLeaf);
        int count = 0;
        for (int entry = start; entry < end; entry++) {
            boolean opensRealPair = levels.repLevels().get(entry) <= meta.entryRepLevel()
                    && levels.defLevels().get(entry) >= meta.entryDefLevel();
            if (opensRealPair) {
                count++;
            }
        }
        return count;
    }
}
