/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.data;

import java.util.OptionalLong;

import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;

/**
 * Resolves the uncompressed-byte and row-count thresholds at which the dataset writer should seal the current row
 * group, given the configured {@link RowGroupSize} and {@link WriteOptions#expectedRowCount()} hint.
 *
 * <p>Explicit {@link RowGroupSize.Bytes} and {@link RowGroupSize.Rows} variants pin the corresponding threshold and
 * disable the other. {@link RowGroupSize.Adaptive} consults the row-count hint to pick a sensible byte budget:
 *
 * <ul>
 *   <li>Below 50_000 rows: one row group, no bound (returns {@link Long#MAX_VALUE}).
 *   <li>50_000 .. 999_999 rows: 128 MB.
 *   <li>1_000_000 .. 9_999_999 rows: 512 MB.
 *   <li>10_000_000 rows and above: 1 GB.
 *   <li>No hint: 128 MB.
 * </ul>
 */
final class RowGroupSizingPolicy {

    private static final long MB = 1024L * 1024L;
    private static final long ADAPTIVE_SMALL_THRESHOLD = 50_000L;
    private static final long ADAPTIVE_MEDIUM_THRESHOLD = 1_000_000L;
    private static final long ADAPTIVE_LARGE_THRESHOLD = 10_000_000L;
    private static final long DEFAULT_ADAPTIVE_BYTES = 128L * MB;
    private static final long MEDIUM_ADAPTIVE_BYTES = 512L * MB;
    private static final long LARGE_ADAPTIVE_BYTES = 1024L * MB;

    private RowGroupSizingPolicy() {}

    public static long targetBytes(WriteOptions options) {
        return switch (options.rowGroupSize()) {
            case RowGroupSize.Bytes(long bytes) -> bytes;
            case RowGroupSize.Rows _ -> Long.MAX_VALUE;
            case RowGroupSize.Adaptive _ -> adaptiveBytes(options.expectedRowCount());
        };
    }

    public static OptionalLong targetRows(WriteOptions options) {
        if (options.rowGroupSize() instanceof RowGroupSize.Rows(long threshold)) {
            return OptionalLong.of(threshold);
        }
        return OptionalLong.empty();
    }

    private static long adaptiveBytes(OptionalLong expectedRowCount) {
        if (expectedRowCount.isEmpty()) {
            return DEFAULT_ADAPTIVE_BYTES;
        }
        long n = expectedRowCount.getAsLong();
        if (n < ADAPTIVE_SMALL_THRESHOLD) {
            return Long.MAX_VALUE;
        }
        if (n < ADAPTIVE_MEDIUM_THRESHOLD) {
            return DEFAULT_ADAPTIVE_BYTES;
        }
        if (n < ADAPTIVE_LARGE_THRESHOLD) {
            return MEDIUM_ADAPTIVE_BYTES;
        }
        return LARGE_ADAPTIVE_BYTES;
    }
}
