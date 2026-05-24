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
package io.tileverse.parquetry.data.read;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;
import lombok.NonNull;

/**
 * Plans and fetches one row group's projected column chunks as a small set of coalesced range reads. Replaces the
 * former per-column fetch: instead of one {@code readRange} per column chunk, it issues one read per coalesced range
 * and hands out zero-copy {@link FetchedColumnChunk} views into the range buffers.
 */
public final class RowGroupFetcher {

    private final RangeReader rangeReader;
    private final ParquetSchema fileSchema;
    private final ParquetSchema projectedSchema;
    private final ByteBufferPool pool;
    private final int maxCoalesceGap;
    private final int maxCoalescedSpan;

    public RowGroupFetcher(
            @NonNull RangeReader rangeReader,
            @NonNull ParquetSchema fileSchema,
            @NonNull ParquetSchema projectedSchema,
            @NonNull ByteBufferPool pool,
            int maxCoalesceGap,
            int maxCoalescedSpan) {
        this.rangeReader = rangeReader;
        this.fileSchema = fileSchema;
        this.projectedSchema = projectedSchema;
        this.pool = pool;
        this.maxCoalesceGap = maxCoalesceGap;
        this.maxCoalescedSpan = maxCoalescedSpan;
    }

    /**
     * Builds the coalescing plan for {@code survivor} without performing any I/O (used to size budget reservations).
     */
    public FetchPlan planFor(RowGroupSurvivor survivor) {
        Map<List<String>, ColumnChunk> chunksByPath = indexChunksByPath(survivor.rowGroup());
        List<ColumnRange> columns = new ArrayList<>();
        for (ColumnPath path : projectedSchema.leafColumns()) {
            ColumnMetaData meta = requireMeta(chunksByPath, path);
            columns.add(new ColumnRange(path, chunkStart(meta), chunkLength(meta, path)));
        }
        return CoalescingFetchPlanner.plan(columns, maxCoalesceGap, maxCoalescedSpan);
    }

    /**
     * Reads {@code plan}'s coalesced ranges into pooled buffers and slices each projected column out of them. On any
     * failure, closes every buffer borrowed so far and releases {@code reservation} before propagating.
     */
    public RowGroupFetch fetch(RowGroupSurvivor survivor, FetchPlan plan, BudgetReservation reservation)
            throws IOException {
        Map<List<String>, ColumnChunk> chunksByPath = indexChunksByPath(survivor.rowGroup());
        List<PooledByteBuffer> buffers = new ArrayList<>(plan.ranges().size());
        List<MemorySegment> rangeSegments = new ArrayList<>(plan.ranges().size());
        try {
            for (CoalescedRange range : plan.ranges()) {
                PooledByteBuffer pooled = pool.borrowDirect(range.length());
                buffers.add(pooled);
                ByteBuffer buffer = pooled.buffer();
                buffer.clear();
                buffer.limit(range.length());
                int read = rangeReader.readRange(range.fileOffset(), range.length(), buffer);
                if (read != range.length()) {
                    throw new IOException("Short read for coalesced range at offset " + range.fileOffset()
                            + ": expected " + range.length() + " bytes, got " + read);
                }
                buffer.flip();
                rangeSegments.add(MemorySegment.ofBuffer(buffer));
            }
            List<FetchedColumnChunk> columns = sliceColumns(plan, chunksByPath, rangeSegments);
            return new RowGroupFetch(buffers, columns, reservation);
        } catch (IOException | RuntimeException e) {
            for (PooledByteBuffer buffer : buffers) {
                try {
                    buffer.close();
                } catch (RuntimeException ignored) {
                    // best-effort cleanup; rethrow the original failure
                }
            }
            reservation.release();
            throw e;
        }
    }

    private List<FetchedColumnChunk> sliceColumns(
            FetchPlan plan, Map<List<String>, ColumnChunk> chunksByPath, List<MemorySegment> rangeSegments)
            throws IOException {
        List<FetchedColumnChunk> columns = new ArrayList<>(plan.slices().size());
        for (ColumnPath path : projectedSchema.leafColumns()) {
            ColumnSlice slice = plan.slices().get(path);
            ColumnMetaData meta = requireMeta(chunksByPath, path);
            MemorySegment chunkSegment =
                    rangeSegments.get(slice.rangeIndex()).asSlice(slice.offsetWithinRange(), slice.length());
            columns.add(ColumnChunkSlicer.slice(chunkSegment, meta, path, fileSchema));
        }
        return columns;
    }

    private ColumnMetaData requireMeta(Map<List<String>, ColumnChunk> chunksByPath, ColumnPath path) {
        ColumnChunk chunk = chunksByPath.get(path.parts());
        if (chunk == null) {
            throw new IllegalStateException("Row group does not contain column " + path.dot());
        }
        return chunk.metaData()
                .orElseThrow(() ->
                        new ParquetFormatException("ColumnChunk for " + path.dot() + " is missing inline metaData"));
    }

    private static long chunkStart(ColumnMetaData meta) {
        return meta.dictionaryPageOffset().orElse(meta.dataPageOffset());
    }

    private static int chunkLength(ColumnMetaData meta, ColumnPath path) {
        long size = meta.totalCompressedSize();
        if (size <= 0 || size > Integer.MAX_VALUE) {
            throw new MalformedFileException(
                    "Column chunk for " + path.dot() + " has an unsupported totalCompressedSize " + size);
        }
        return (int) size;
    }

    private static Map<List<String>, ColumnChunk> indexChunksByPath(RowGroup rowGroup) {
        Map<List<String>, ColumnChunk> index = new LinkedHashMap<>();
        for (ColumnChunk chunk : rowGroup.columns()) {
            ColumnMetaData meta = chunk.metaData()
                    .orElseThrow(
                            () -> new MalformedFileException("ColumnChunk without inline metadata is not supported"));
            index.put(meta.pathInSchema(), chunk);
        }
        return index;
    }
}
