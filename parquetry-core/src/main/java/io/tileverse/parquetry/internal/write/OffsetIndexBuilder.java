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
package io.tileverse.parquetry.internal.write;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;

/**
 * Per-row-group, per-column footer offset-index builder. Each {@link #appendPage(long, int, long)} call records one
 * data-page entry: its byte offset relative to the start of the file, its compressed byte length (including the page
 * header), and the zero-based row index, relative to the start of the row group, of its first row.
 *
 * <p>{@link #finishChunk()} assembles the {@link OffsetIndex} for the current accumulation window and clears state so
 * the builder can be reused for the next column chunk.
 */
public final class OffsetIndexBuilder {

    private final List<PageLocation> pageLocations = new ArrayList<>();

    /**
     * Appends one page entry.
     *
     * @param byteOffset byte offset of the page (its page header) from the beginning of the file
     * @param compressedPageSize compressed byte size of the page including its header
     * @param firstRowIndex zero-based row index, relative to the start of the row group, of the first row in this page
     */
    public void appendPage(long byteOffset, int compressedPageSize, long firstRowIndex) {
        pageLocations.add(new PageLocation(byteOffset, compressedPageSize, firstRowIndex));
    }

    /**
     * Returns the {@link OffsetIndex} assembled from every {@link #appendPage} call since the last reset, and clears
     * internal state so the builder can be reused for the next column chunk.
     */
    public OffsetIndex finishChunk() {
        OffsetIndex index = new OffsetIndex(List.copyOf(pageLocations), Optional.empty());
        reset();
        return index;
    }

    /** Clears every accumulated page entry; the builder behaves as freshly constructed. */
    public void reset() {
        pageLocations.clear();
    }

    /**
     * Returns an unmodifiable view of the accumulated page entries. Useful when the caller needs to inspect or rewrite
     * the entries before {@link #finishChunk()} -- for example to shift offsets by a fixed byte delta after the column
     * chunk's leading dictionary page is materialized.
     */
    public List<PageLocation> pageLocations() {
        return List.copyOf(pageLocations);
    }

    /**
     * Replaces every accumulated page entry with the supplied list. The list size must match the number of entries
     * currently in the builder.
     */
    public void replaceAll(List<PageLocation> replacement) {
        if (replacement.size() != pageLocations.size()) {
            throw new IllegalArgumentException(
                    "replacement size " + replacement.size() + " does not match builder size " + pageLocations.size());
        }
        pageLocations.clear();
        pageLocations.addAll(replacement);
    }
}
