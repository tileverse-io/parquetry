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

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.internal.filter.FilterPipeline.ColumnBloom;
import io.tileverse.parquetry.internal.filter.FilterPipeline.ColumnPageStats;
import io.tileverse.parquetry.internal.filter.FilterPipeline.ColumnStats;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.internal.footer.ChunkMeta;
import io.tileverse.parquetry.internal.footer.CompactFooter;
import io.tileverse.parquetry.internal.footer.LeafIndex;
import io.tileverse.parquetry.observe.FetchPurpose;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

import lombok.NonNull;

/**
 * An in-memory view over one row group of a {@link CompactFooter} plus the file schema, built once per read call. It
 * resolves a column path to that row group's chunk and serves the per-column metadata needed by the read path
 * (primitive kind, flatness, the inline statistics), and it memoizes the lazily-loaded index sections - a column's
 * {@link OffsetIndex}, {@link ColumnIndex}, and bloom filter are each read at most once per call no matter how many
 * pipeline stages ask.
 *
 * <p>Construction is cheap and side-effect free. Index sections load on first access through the injected
 * {@link IndexSectionLoader}. When a phase knows its whole set of sections up front, it calls
 * {@link #warmOffsetIndexes} or {@link #warmPageStats} first to read them together; that leaves the memo below
 * unchanged, and every lookup still goes through it. A missing section, a non-primitive column, or a read failure all
 * degrade to {@link Optional#empty()}; the read still succeeds with that tier off for the column.
 *
 * <p>This view is confined to the calling thread, together with the {@link IndexSectionLoader} behind it: one loader
 * serves every row-group view of a read and holds the bytes of its last batch in a mutable map. Index sections are
 * loaded during filter evaluation and mask building, both of which run on the consumer thread before decode workers
 * start; neither the view nor the loader holds state touched by decode workers. A future change that loads sections
 * from a worker thread would need to guard the memo below and that map.
 */
public final class RowGroupChunks {

    /** Passed to the loader when the footer records no bloom-filter length; the filter's header states its size. */
    private static final int LENGTH_FROM_FILTER_HEADER = -1;

    private final CompactFooter footer;
    private final int rowGroupIndex;
    private final LeafIndex leaves;
    private final ParquetSchema fileSchema;
    private final IndexSectionLoader loader;

    private final Map<ColumnPath, Optional<OffsetIndex>> offsetIndexMemo = new HashMap<>();
    private final Map<ColumnPath, Optional<ColumnIndex>> columnIndexMemo = new HashMap<>();
    private final Map<ColumnPath, Optional<ColumnPageStats>> pageStatsMemo = new HashMap<>();
    private final Map<ColumnPath, Optional<ColumnBloom>> bloomMemo = new HashMap<>();

    private RowGroupChunks(
            CompactFooter footer,
            int rowGroupIndex,
            LeafIndex leaves,
            ParquetSchema fileSchema,
            IndexSectionLoader loader) {
        this.footer = footer;
        this.rowGroupIndex = rowGroupIndex;
        this.leaves = leaves;
        this.fileSchema = fileSchema;
        this.loader = loader;
    }

    /** Builds the view over row group {@code rowGroupIndex}; nothing is read from the blob until a lookup asks. */
    public static RowGroupChunks of(
            @NonNull CompactFooter footer,
            int rowGroupIndex,
            @NonNull LeafIndex leaves,
            @NonNull ParquetSchema fileSchema,
            @NonNull IndexSectionLoader loader) {
        return new RowGroupChunks(footer, rowGroupIndex, leaves, fileSchema, loader);
    }

    /** The row group's position in file order. */
    public int rowGroupIndex() {
        return rowGroupIndex;
    }

    public long numRows() {
        return footer.numRows(rowGroupIndex);
    }

    /** The column chunk for {@code path}, or empty when the row group has no such column. */
    public Optional<ChunkMeta> chunk(ColumnPath path) {
        int leaf = leaves.ordinalOf(path);
        if (leaf == LeafIndex.UNKNOWN) {
            return Optional.empty();
        }
        return footer.chunkIfPresent(rowGroupIndex, leaf);
    }

    /** The primitive kind of {@code path}, or empty for non-primitive or unknown paths. */
    public Optional<PrimitiveKind> primitiveKind(ColumnPath path) {
        return fileSchema
                .find(path)
                .flatMap(node -> node instanceof SchemaNode.Primitive p ? Optional.of(p.kind()) : Optional.empty());
    }

    /** The logical type annotation of {@code path}, or empty when absent or when the path is not a primitive. */
    private Optional<LogicalType> logicalType(ColumnPath path) {
        return fileSchema
                .find(path)
                .flatMap(node -> node instanceof SchemaNode.Primitive p ? p.logicalType() : Optional.empty());
    }

    /** True when {@code path} is a non-repeated leaf (max repetition level 0). */
    public boolean isFlat(ColumnPath path) {
        return fileSchema.maxLevels(path).maxRepetitionLevel() == 0;
    }

    /** Inline statistics for {@code path}, or empty when the column has none or maps to no primitive leaf. */
    public Optional<ColumnStats> stats(ColumnPath path) {
        Optional<ChunkMeta> maybeChunk = chunk(path);
        if (maybeChunk.isEmpty()) {
            return Optional.empty();
        }
        ChunkMeta chunk = maybeChunk.orElseThrow();
        Optional<MemorySegment> minValue = orderedBound(chunk, chunk.minValue());
        Optional<MemorySegment> maxValue = orderedBound(chunk, chunk.maxValue());
        OptionalLong nullCount = chunk.nullCount();
        if (!hasStatistics(minValue, maxValue, nullCount)) {
            return Optional.empty();
        }
        return primitiveKind(path).map(kind -> new ColumnStats(kind, minValue, maxValue, nullCount, logicalType(path)));
    }

    /** A bound in an order not applied by this reader is left out, as if the writer had recorded none. */
    private static Optional<MemorySegment> orderedBound(ChunkMeta chunk, Optional<MemorySegment> bound) {
        if (!chunk.boundsOrdered()) {
            return Optional.empty();
        }
        return bound;
    }

    /**
     * Whether the writer recorded anything readable by a pruning tier. A chunk with no null count and neither bound had
     * no usable statistics on the wire either.
     */
    private static boolean hasStatistics(
            Optional<MemorySegment> minValue, Optional<MemorySegment> maxValue, OptionalLong nullCount) {
        return nullCount.isPresent() || minValue.isPresent() || maxValue.isPresent();
    }

    /**
     * Reads the offset index of every path in {@code paths} that has one and is not memoized yet, in one call. A later
     * {@link #offsetIndex} of any of them decodes bytes already in hand. A path outside {@code paths} still loads on
     * its own.
     */
    public void warmOffsetIndexes(@NonNull List<ColumnPath> paths) {
        List<IndexSectionRange> ranges = new ArrayList<>(paths.size());
        for (ColumnPath path : distinct(paths)) {
            addOffsetIndexRange(path, ranges);
        }
        prefetchOrLeaveToTheLookups(ranges);
    }

    /**
     * Reads the column index and the offset index of every path in {@code paths} that has them and is not memoized yet,
     * in one call. A later {@link #pageStats}, {@link #columnIndex} or {@link #offsetIndex} of any of them decodes
     * bytes already in hand.
     */
    public void warmPageStats(@NonNull List<ColumnPath> paths) {
        List<IndexSectionRange> ranges = new ArrayList<>(paths.size() * 2);
        for (ColumnPath path : distinct(paths)) {
            addColumnIndexRange(path, ranges);
            addOffsetIndexRange(path, ranges);
        }
        prefetchOrLeaveToTheLookups(ranges);
    }

    /**
     * Hands the batch to the loader, dropping a failed read. A section degrades to {@link Optional#empty()} at its own
     * lookup when its bytes cannot be read, and a warm must not turn that into a failed read.
     */
    private void prefetchOrLeaveToTheLookups(List<IndexSectionRange> ranges) {
        try {
            loader.prefetch(ranges);
        } catch (RuntimeException _) {
            // every section still reads itself on its lookup, where a failure degrades to Optional.empty()
        }
    }

    /**
     * The paths to warm, each once. A path named twice would put the same range in the batch twice, and a batch read
     * satisfies each of its requests independently.
     */
    private static Set<ColumnPath> distinct(List<ColumnPath> paths) {
        return new LinkedHashSet<>(paths);
    }

    private void addOffsetIndexRange(ColumnPath path, List<IndexSectionRange> ranges) {
        if (offsetIndexMemo.containsKey(path)) {
            return;
        }
        chunk(path)
                .ifPresent(chunk -> addRange(
                        ranges, FetchPurpose.OFFSET_INDEX, chunk.offsetIndexOffset(), chunk.offsetIndexLength()));
    }

    private void addColumnIndexRange(ColumnPath path, List<IndexSectionRange> ranges) {
        if (columnIndexMemo.containsKey(path)) {
            return;
        }
        chunk(path)
                .ifPresent(chunk -> addRange(
                        ranges, FetchPurpose.COLUMN_INDEX, chunk.columnIndexOffset(), chunk.columnIndexLength()));
    }

    /**
     * Adds the section to the batch when the footer locates it where a read can reach it. A chunk with no such section
     * contributes nothing, and neither does a section placed by the footer at an impossible offset or length: such a
     * section is left to fail at its own lookup, where the failure degrades to {@link Optional#empty()}.
     */
    private static void addRange(
            List<IndexSectionRange> ranges, FetchPurpose purpose, OptionalLong offset, int length) {
        if (offset.isEmpty() || offset.getAsLong() < 0 || length <= 0) {
            return;
        }
        ranges.add(new IndexSectionRange(purpose, offset.getAsLong(), length));
    }

    /** The memoized {@link OffsetIndex}, or empty when absent or unreadable. */
    public Optional<OffsetIndex> offsetIndex(ColumnPath path) {
        return offsetIndexMemo.computeIfAbsent(path, this::loadOffsetIndex);
    }

    /** The memoized {@link ColumnIndex}, or empty when absent or unreadable. */
    public Optional<ColumnIndex> columnIndex(ColumnPath path) {
        return columnIndexMemo.computeIfAbsent(path, this::loadColumnIndex);
    }

    /**
     * The memoized column-index tier input (kind + column index + offset index). Loading it populates the
     * {@link #offsetIndex} and {@link #columnIndex} memos, and a later {@code offsetIndex(path)} for the same column
     * does not re-read.
     */
    public Optional<ColumnPageStats> pageStats(ColumnPath path) {
        return pageStatsMemo.computeIfAbsent(path, this::loadPageStats);
    }

    /** The memoized bloom filter input (kind + filter), or empty when absent or unreadable. */
    public Optional<ColumnBloom> bloom(ColumnPath path) {
        return bloomMemo.computeIfAbsent(path, this::loadBloom);
    }

    private Optional<OffsetIndex> loadOffsetIndex(ColumnPath path) {
        return chunk(path).flatMap(this::readOffsetIndex);
    }

    private Optional<OffsetIndex> readOffsetIndex(ChunkMeta chunk) {
        OptionalLong offset = chunk.offsetIndexOffset();
        if (offset.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(loader.readOffsetIndex(offset.getAsLong(), chunk.offsetIndexLength()));
        } catch (RuntimeException _) {
            return Optional.empty();
        }
    }

    private Optional<ColumnIndex> loadColumnIndex(ColumnPath path) {
        return chunk(path).flatMap(this::readColumnIndex);
    }

    private Optional<ColumnIndex> readColumnIndex(ChunkMeta chunk) {
        OptionalLong offset = chunk.columnIndexOffset();
        if (offset.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(loader.readColumnIndex(offset.getAsLong(), chunk.columnIndexLength()));
        } catch (RuntimeException _) {
            return Optional.empty();
        }
    }

    private Optional<ColumnPageStats> loadPageStats(ColumnPath path) {
        Optional<PrimitiveKind> kind = primitiveKind(path);
        if (kind.isEmpty()) {
            return Optional.empty();
        }
        Optional<ColumnIndex> ci = columnIndex(path);
        Optional<OffsetIndex> oi = offsetIndex(path);
        if (ci.isEmpty() || oi.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new ColumnPageStats(
                kind.orElseThrow(), ci.orElseThrow(), oi.orElseThrow(), logicalType(path), boundsOrdered(path)));
    }

    private boolean boundsOrdered(ColumnPath path) {
        return chunk(path).map(ChunkMeta::boundsOrdered).orElse(true);
    }

    private Optional<ColumnBloom> loadBloom(ColumnPath path) {
        Optional<PrimitiveKind> kind = primitiveKind(path);
        if (kind.isEmpty()) {
            return Optional.empty();
        }
        return chunk(path).flatMap(chunk -> readBloom(chunk, kind.orElseThrow(), logicalType(path)));
    }

    private Optional<ColumnBloom> readBloom(ChunkMeta chunk, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        OptionalLong offset = chunk.bloomFilterOffset();
        if (offset.isEmpty()) {
            return Optional.empty();
        }
        try {
            int length = chunk.bloomFilterLength().orElse(LENGTH_FROM_FILTER_HEADER);
            SplitBlockBloomFilter filter = loader.readBloom(offset.getAsLong(), length);
            return Optional.of(new ColumnBloom(kind, filter, logicalType));
        } catch (RuntimeException _) {
            return Optional.empty();
        }
    }
}
