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
package io.tileverse.parquetry.internal.write;

import java.lang.foreign.MemorySegment;
import java.util.List;

import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.GeospatialStatistics;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Sealed receipt returned by {@link RowGroupWriter#flushTo}.
 *
 * <p>Carries the assembled {@link RowGroup} Thrift record together with the side artifacts the dataset-level writer
 * still needs to place in the file (column index, offset index, bloom filter blobs). The {@link RowGroupWriter} appends
 * every column chunk's data pages directly to the caller's output stream. Index and bloom blobs are returned via
 * {@link #columnArtifacts()} so the dataset writer can interleave them between the last row group and the footer per
 * the Parquet file layout.
 *
 * @param rowGroup the assembled row-group descriptor with absolute byte offsets baked into every
 *     {@link io.tileverse.parquetry.format.ColumnMetaData}
 * @param bytesWritten total compressed bytes appended to the caller's output stream during this flush, summed across
 *     every column chunk
 * @param dictionaryBytes total compressed bytes contributed by dictionary pages within this row group (subset of
 *     {@link #bytesWritten()}); useful for telemetry and for sizing the next row group's adaptive budget
 * @param columnArtifacts one entry per leaf column in schema order, carrying the per-column index / offset-index /
 *     bloom-filter blobs that have not yet been written to the file
 */
public record RowGroupFlushResult(
        RowGroup rowGroup, long bytesWritten, long dictionaryBytes, List<ColumnArtifacts> columnArtifacts) {

    public RowGroupFlushResult {
        columnArtifacts = List.copyOf(columnArtifacts);
    }

    /**
     * Per-column side artifacts that the dataset-level writer must place in the file after every row group's data pages
     * have been written.
     *
     * <p>The {@link ColumnArtifacts#columnIndex()} and {@link #offsetIndex()} are emitted in V2 mode only; both are
     * {@code null} when the row group writer is configured for V1 page format. {@link #bloomFilterBytes()} carries the
     * SBBF bitset without its Thrift header; the dataset writer frames the header at placement time so it can record
     * the absolute file offset and length back into the corresponding {@link io.tileverse.parquetry.format.ColumnChunk}
     * before footer assembly.
     *
     * <p>{@link #geospatialStatistics()} is already inlined into the
     * {@link io.tileverse.parquetry.format.ColumnMetaData} returned in {@link RowGroupFlushResult#rowGroup()}; it is
     * surfaced here so dataset-level accumulators can union across row groups without re-parsing the descriptor.
     *
     * @param columnPath the leaf column's dotted path
     * @param columnIndex assembled per-page column index; {@code null} when the writer is in V1 mode
     * @param offsetIndex assembled per-page offset index with absolute file offsets; {@code null} when the writer is in
     *     V1 mode
     * @param bloomFilterBytes raw SBBF bitset bytes (header excluded); {@link MemorySegment#NULL} when no bloom filter
     *     was emitted for this column
     * @param geospatialStatistics geometry / geography statistics for this column chunk; {@code null} for non-geo
     *     columns
     */
    public record ColumnArtifacts(
            ColumnPath columnPath,
            ColumnIndex columnIndex,
            OffsetIndex offsetIndex,
            MemorySegment bloomFilterBytes,
            GeospatialStatistics geospatialStatistics) {

        public ColumnArtifacts {
            bloomFilterBytes = readOnlyOrAbsent(bloomFilterBytes);
        }

        private static MemorySegment readOnlyOrAbsent(MemorySegment segment) {
            if (segment == null) {
                return MemorySegment.NULL;
            }
            if (segment == MemorySegment.NULL) {
                return segment;
            }
            return segment.isReadOnly() ? segment : segment.asReadOnly();
        }
    }
}
