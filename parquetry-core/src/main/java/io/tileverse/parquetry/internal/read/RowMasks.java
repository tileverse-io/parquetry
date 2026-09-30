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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Builds the decode-time {@link RowMask} of one row group and answers whether a scan qualifies for one. A mask maps a
 * set of row ranges onto the pages of every scanned leaf, which is what lets the fetch and the decode skip the pages
 * outside those ranges.
 */
public final class RowMasks {

    private RowMasks() {}

    /** True when every {@code leaf} is non-repeated (max repetition level 0) in {@code schema}. */
    public static boolean allFlat(ParquetSchema schema, List<ColumnPath> leaves) {
        for (ColumnPath leaf : leaves) {
            if (schema.maxLevels(leaf).maxRepetitionLevel() > 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * The mask narrowing {@code scanLeaves} to {@code rows}, or empty when one of those leaves has no offset index. A
     * leaf with no offset index cannot map rows to pages, and its row group is read in full.
     *
     * <p>Every leaf's offset index is named before the first one is asked for, hence they are read together.
     */
    public static Optional<RowMask> maskFor(RowGroupChunks chunks, RowRanges rows, List<ColumnPath> scanLeaves) {
        chunks.warmOffsetIndexes(scanLeaves);
        Map<ColumnPath, OffsetIndex> offsetIndexes = LinkedHashMap.newLinkedHashMap(scanLeaves.size());
        for (ColumnPath leaf : scanLeaves) {
            Optional<OffsetIndex> offsetIndex = chunks.offsetIndex(leaf);
            if (offsetIndex.isEmpty()) {
                return Optional.empty();
            }
            offsetIndexes.put(leaf, offsetIndex.orElseThrow());
        }
        return Optional.of(new RowMask(rows, offsetIndexes));
    }
}
