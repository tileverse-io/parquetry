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

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.List;

import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.GeospatialStatistics;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.Statistics;

/**
 * Sealed receipt returned by {@link ColumnChunkWriter#finishChunk()}.
 *
 * <p>The temp file at {@link #tempFile()} carries the column's pages concatenated in write order: an optional
 * dictionary page first, then every data page back-to-back. The row-group writer consolidates this file into the final
 * output and is responsible for deleting it. {@link #firstDataPageOffset()} and {@link #dictionaryPageOffset()} are
 * offsets within the temp file; the consolidator rewrites them to absolute file offsets before they land in
 * {@code ColumnMetaData}.
 *
 * <p>The {@link #offsetIndex()} entries carry per-page offsets that are also relative to the temp file. The
 * consolidator shifts every entry by the column chunk's absolute base offset.
 *
 * @param tempFile column-chunk temp file holding the concatenated pages
 * @param uncompressedBytes total uncompressed byte size of every page in the chunk, including page headers
 * @param compressedBytes total compressed byte size of every page in the chunk, including page headers; matches the
 *     temp file's byte length
 * @param firstDataPageOffset offset of the first data page within the temp file
 * @param dictionaryPageOffset offset of the dictionary page within the temp file; {@code -1} when none was emitted
 * @param numValues total number of cells in the chunk, including nulls
 * @param firstRowIndex zero-based row index, relative to the start of the row group, of the chunk's first row
 * @param encodings distinct encodings used across the chunk's pages, in declaration order
 * @param chunkStatistics chunk-level typed min/max/null-count statistics
 * @param columnIndex assembled per-page column index; {@code null} when the writer is in V1 mode
 * @param offsetIndex assembled per-page offset index; {@code null} when the writer is in V1 mode
 * @param bloomFilterBytes raw SBBF bitset bytes (header excluded); {@link MemorySegment#NULL} when no bloom filter was
 *     emitted for this column
 * @param geospatialStatistics geometry / geography statistics; {@code null} for non-geo columns
 */
public record ColumnChunkResult(
        Path tempFile,
        long uncompressedBytes,
        long compressedBytes,
        long firstDataPageOffset,
        long dictionaryPageOffset,
        long numValues,
        long firstRowIndex,
        List<Encoding> encodings,
        Statistics chunkStatistics,
        ColumnIndex columnIndex,
        OffsetIndex offsetIndex,
        MemorySegment bloomFilterBytes,
        GeospatialStatistics geospatialStatistics) {

    public ColumnChunkResult {
        encodings = encodings == null ? List.of() : List.copyOf(encodings);
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
