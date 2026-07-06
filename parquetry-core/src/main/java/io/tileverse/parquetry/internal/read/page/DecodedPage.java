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
package io.tileverse.parquetry.internal.read.page;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.format.Encoding;

import lombok.NonNull;

/**
 * The uniform {@code (repLevels, defLevels, values)} triple produced by a {@link DataPageReader} for one data page.
 *
 * <p>All three byte regions are {@link MemorySegment} views allocated from {@link #pageArena()}. The Arena is owned by
 * this record; {@link #close()} closes it, deterministically releasing native memory for all three segments at once.
 *
 * <p>Empty rep/def regions (column with {@code maxLevel == 0}) are represented as {@link MemorySegment#NULL}, not as
 * zero-length allocated segments, to avoid unnecessary Arena allocation.
 *
 * <p>{@link #valuesEncoding()} is already normalized: {@link Encoding#PLAIN_DICTIONARY} from Parquet 1.x writers is
 * collapsed to {@link Encoding#RLE_DICTIONARY} (see {@link DataPageReader}). Dictionary <em>pages</em> still use
 * {@link Encoding#PLAIN} and are not affected.
 *
 * @param valueCount values yielded by this page (matches the page header's {@code numValues})
 * @param valuesEncoding the value encoding to drive {@code DecoderFactory.decoderFor(...)}; already normalized
 * @param repLevelBytes Arena-allocated segment of rep-level bytes; {@link MemorySegment#NULL} when the column has
 *     {@code maxRep == 0}
 * @param defLevelBytes Arena-allocated segment of def-level bytes; {@link MemorySegment#NULL} when the column has
 *     {@code maxDef == 0}
 * @param valueBytes Arena-allocated segment holding the decompressed value bytes
 * @param pageArena the Arena owning all three segments; closing this {@code DecodedPage} closes the Arena
 */
public record DecodedPage(
        int valueCount,
        @NonNull Encoding valuesEncoding,
        @NonNull MemorySegment repLevelBytes,
        @NonNull MemorySegment defLevelBytes,
        @NonNull MemorySegment valueBytes,
        @NonNull Arena pageArena)
        implements AutoCloseable {

    public DecodedPage {
        if (valueCount < 0) {
            throw new IllegalArgumentException("valueCount must be >= 0, got " + valueCount);
        }
    }

    /** Closes the page Arena, deterministically releasing all native memory for the rep/def/value segments together. */
    @Override
    public void close() {
        pageArena.close();
    }
}
