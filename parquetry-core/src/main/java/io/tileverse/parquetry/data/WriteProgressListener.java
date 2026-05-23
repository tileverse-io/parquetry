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

import java.time.Duration;

/**
 * Progress callback surface for the parquetry write engine.
 *
 * <p>All methods default to a no-op so implementations can override only the events they care about. Callbacks fire on
 * the writer thread; implementations should be cheap and non-blocking. The cadence of {@link #onRowsWritten(long)} is
 * controlled by {@code WriteOptions.progressListenerCadenceRows()} (default 100 000 rows).
 */
public interface WriteProgressListener {

    /** Fired every {@code progressListenerCadenceRows} rows with the running total. */
    default void onRowsWritten(long totalRows) {}

    /** Fired exactly once per row group flush with the byte / row totals and elapsed scope time. */
    default void onRowGroupFlushed(RowGroupFlushEvent event) {}

    /** Fired once at writer close with the file-level totals and total elapsed writer time. */
    default void onClose(long totalRows, long totalBytes, Duration totalDuration) {}

    /**
     * Event payload for {@link #onRowGroupFlushed(RowGroupFlushEvent)}.
     *
     * @param rowGroupIndex zero-based index of the flushed row group
     * @param rowCount rows in this group
     * @param uncompressedBytes total uncompressed page bytes across all columns
     * @param compressedBytes total compressed page bytes across all columns
     * @param scopeDuration elapsed wall time of the structured-task-scope that produced the column chunks
     * @param columnsEncoded count of column chunks materialised for this group
     */
    record RowGroupFlushEvent(
            int rowGroupIndex,
            long rowCount,
            long uncompressedBytes,
            long compressedBytes,
            Duration scopeDuration,
            int columnsEncoded) {}
}
