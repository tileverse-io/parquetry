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
package io.tileverse.parquetry.internal.filter;

import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;

import java.lang.foreign.MemorySegment;
import java.time.LocalDateTime;

/**
 * Chronological comparison of INT96 cells, the legacy timestamp layout, with a timestamp literal. A cell holds eight
 * little-endian bytes of nanoseconds within the day, then four of the Julian day; Julian day 2440588 is 1970-01-01.
 */
public final class Int96Timestamps {

    /** The byte width of an INT96 cell. */
    public static final int CELL_BYTES = 12;

    private static final long JULIAN_DAY_OF_EPOCH = 2_440_588L;
    private static final long NANOS_PER_DAY = 86_400_000_000_000L;

    private Int96Timestamps() {}

    /**
     * Orders {@code cell} against {@code literal}: negative, zero, or positive as the cell is earlier than, equal to,
     * or later than the literal. Counting whole days and nanoseconds apart keeps the comparison exact far beyond the
     * range of a 64-bit count of nanoseconds. Nanoseconds running past a day boundary count as further days, as some
     * writers leave them unnormalized.
     *
     * @param cell an INT96 cell of {@link #CELL_BYTES} bytes
     */
    public static int compare(MemorySegment cell, LocalDateTime literal) {
        long nanos = cell.get(INT64, 0L);
        long epochDay = cell.get(INT32, Long.BYTES) - JULIAN_DAY_OF_EPOCH + Math.floorDiv(nanos, NANOS_PER_DAY);
        long nanoOfDay = Math.floorMod(nanos, NANOS_PER_DAY);
        int byDay = Long.compare(epochDay, literal.toLocalDate().toEpochDay());
        if (byDay != 0) {
            return byDay;
        }
        return Long.compare(nanoOfDay, literal.toLocalTime().toNanoOfDay());
    }
}
