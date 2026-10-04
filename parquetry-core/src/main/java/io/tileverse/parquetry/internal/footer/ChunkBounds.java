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
package io.tileverse.parquetry.internal.footer;

import java.lang.foreign.MemorySegment;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.Statistics;

/**
 * Chooses the statistics bounds kept by a {@link CompactFooter} for the column chunks of one file.
 *
 * <p>The {@code min_value} and {@code max_value} fields follow the column order and are kept as written. The deprecated
 * {@code min} and {@code max} fields predate column orders: their writers compared signed numbers, and binary values
 * byte by byte as signed bytes. The pruning tiers compare binary values as unsigned bytes, and a legacy pair can then
 * exclude a value present in the chunk: a UTF-8 name starting with a non-ASCII letter becomes the signed minimum of a
 * chunk also holding "Bern", and in unsigned order "Bern" falls below that minimum. A legacy pair is therefore kept for
 * the signed numeric types, or when its two bounds are equal and no order tells them apart, as parquet-java does. Equal
 * binary bounds from a parquet-mr release before 1.8.0 stay out too: such a release could record a wrong value in both
 * (PARQUET-251).
 */
final class ChunkBounds {

    private static final String PARQUET_MR = "parquet-mr";
    private static final Pattern PARQUET_MR_RELEASE = Pattern.compile("parquet-mr version (\\d{1,9})\\.(\\d{1,9})");
    private static final int FIXED_MAJOR = 1;
    private static final int FIXED_MINOR = 8;

    private final boolean equalBinaryBoundsReliable;

    private ChunkBounds(boolean equalBinaryBoundsReliable) {
        this.equalBinaryBoundsReliable = equalBinaryBoundsReliable;
    }

    /** The bound choice for the column chunks of {@code footer}. */
    static ChunkBounds of(FileMetaData footer) {
        return new ChunkBounds(binaryStatisticsReliable(footer.createdBy()));
    }

    /**
     * Whether the writer named by {@code createdBy} records reliable binary statistics: any application other than
     * parquet-mr, or a parquet-mr release from 1.8.0 on. A footer naming no writer comes from the releases with the
     * defect (PARQUET-297), and a parquet-mr name without a readable release is not trusted either.
     */
    static boolean binaryStatisticsReliable(Optional<String> createdBy) {
        String writer = createdBy.orElse("").trim();
        if (writer.isEmpty()) {
            return false;
        }
        if (!writer.startsWith(PARQUET_MR)) {
            return true;
        }
        Matcher release = PARQUET_MR_RELEASE.matcher(writer);
        if (!release.lookingAt()) {
            return false;
        }
        int major = Integer.parseInt(release.group(1));
        int minor = Integer.parseInt(release.group(2));
        return major > FIXED_MAJOR || (major == FIXED_MAJOR && minor >= FIXED_MINOR);
    }

    /** The minimum kept for the chunk described by {@code meta}, {@link MemorySegment#NULL} when none is kept. */
    MemorySegment min(ColumnMetaData meta) {
        Optional<Statistics> statistics = meta.statistics();
        if (statistics.isEmpty()) {
            return MemorySegment.NULL;
        }
        Statistics stats = statistics.orElseThrow();
        return chosen(stats.minValue(), stats.min(), stats, meta.type());
    }

    /** The maximum kept for the chunk described by {@code meta}, {@link MemorySegment#NULL} when none is kept. */
    MemorySegment max(ColumnMetaData meta) {
        Optional<Statistics> statistics = meta.statistics();
        if (statistics.isEmpty()) {
            return MemorySegment.NULL;
        }
        Statistics stats = statistics.orElseThrow();
        return chosen(stats.maxValue(), stats.max(), stats, meta.type());
    }

    private MemorySegment chosen(MemorySegment modern, MemorySegment legacy, Statistics stats, PhysicalType type) {
        if (modern != MemorySegment.NULL) {
            return modern;
        }
        if (legacyPairUsable(stats, type)) {
            return legacy;
        }
        return MemorySegment.NULL;
    }

    private boolean legacyPairUsable(Statistics stats, PhysicalType type) {
        if (comparedAsSignedNumbers(type)) {
            return true;
        }
        if (!equalBounds(stats)) {
            return false;
        }
        return !isBinary(type) || equalBinaryBoundsReliable;
    }

    /** Whether the pruning tiers compare the cells of {@code type} in the signed order of the legacy fields. */
    private static boolean comparedAsSignedNumbers(PhysicalType type) {
        return switch (type) {
            case BOOLEAN, INT32, INT64, FLOAT, DOUBLE -> true;
            case INT96, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> false;
        };
    }

    private static boolean isBinary(PhysicalType type) {
        return type == PhysicalType.BYTE_ARRAY || type == PhysicalType.FIXED_LEN_BYTE_ARRAY;
    }

    private static boolean equalBounds(Statistics stats) {
        MemorySegment min = stats.min();
        MemorySegment max = stats.max();
        if (min == MemorySegment.NULL || max == MemorySegment.NULL) {
            return false;
        }
        return min.mismatch(max) == -1;
    }
}
