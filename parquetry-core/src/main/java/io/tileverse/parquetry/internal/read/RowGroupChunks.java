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
package io.tileverse.parquetry.internal.read;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.filter.FilterPipeline.ColumnBloom;
import io.tileverse.parquetry.internal.filter.FilterPipeline.ColumnPageStats;
import io.tileverse.parquetry.internal.filter.FilterPipeline.ColumnStats;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

import lombok.NonNull;

/**
 * An in-memory view over one {@link RowGroup} plus the file schema, built once per read call. It owns the
 * column-path-to-chunk index and the per-column metadata lookups the read path needs (primitive kind, flatness, the
 * inline statistics), and it memoizes the lazily-loaded index sections - a column's {@link OffsetIndex},
 * {@link ColumnIndex}, and bloom filter are each read at most once per call no matter how many pipeline stages ask.
 *
 * <p>Construction is cheap and side-effect free (it builds the chunk index only). Index sections load on first access
 * through the injected {@link IndexSectionLoader}. A missing section, a non-primitive column, or a read failure all
 * degrade to {@link Optional#empty()}; the read still succeeds with that tier off for the column.
 *
 * <p>This view is confined to the calling thread. Index sections are loaded during filter evaluation and mask building,
 * both of which run on the consumer thread before decode workers start; the view carries no state touched by decode
 * workers. A future change that loads sections from a worker thread would need to guard the memo.
 */
public final class RowGroupChunks {

    private final RowGroup rowGroup;
    private final ParquetSchema fileSchema;
    private final IndexSectionLoader loader;
    private final Map<ColumnPath, ColumnChunk> chunkByPath;

    private final Map<ColumnPath, Optional<OffsetIndex>> offsetIndexMemo = new HashMap<>();
    private final Map<ColumnPath, Optional<ColumnIndex>> columnIndexMemo = new HashMap<>();
    private final Map<ColumnPath, Optional<ColumnPageStats>> pageStatsMemo = new HashMap<>();
    private final Map<ColumnPath, Optional<ColumnBloom>> bloomMemo = new HashMap<>();

    private RowGroupChunks(RowGroup rowGroup, ParquetSchema fileSchema, IndexSectionLoader loader) {
        this.rowGroup = rowGroup;
        this.fileSchema = fileSchema;
        this.loader = loader;
        this.chunkByPath = indexChunksByPath(rowGroup);
    }

    /** Builds the view; only the chunk index is computed eagerly. */
    public static RowGroupChunks of(
            @NonNull RowGroup rowGroup, @NonNull ParquetSchema fileSchema, @NonNull IndexSectionLoader loader) {
        return new RowGroupChunks(rowGroup, fileSchema, loader);
    }

    public RowGroup rowGroup() {
        return rowGroup;
    }

    public long numRows() {
        return rowGroup.numRows();
    }

    /** The column chunk for {@code path}, or empty when the row group has no such column. */
    public Optional<ColumnChunk> chunk(ColumnPath path) {
        return Optional.ofNullable(chunkByPath.get(path));
    }

    public Optional<ColumnMetaData> meta(ColumnPath path) {
        return chunk(path).flatMap(ColumnChunk::metaData);
    }

    /** The primitive kind of {@code path}, or empty for non-primitive or unknown paths. */
    public Optional<PrimitiveKind> primitiveKind(ColumnPath path) {
        return fileSchema
                .find(path)
                .flatMap(node -> node instanceof SchemaNode.Primitive p ? Optional.of(p.kind()) : Optional.empty());
    }

    /** True when {@code path} is a non-repeated leaf (max repetition level 0). */
    public boolean isFlat(ColumnPath path) {
        return fileSchema.maxLevels(path).maxRepetitionLevel() == 0;
    }

    /** Inline statistics for {@code path}, or empty when the column has none or maps to no primitive leaf. */
    public Optional<ColumnStats> stats(ColumnPath path) {
        Optional<ColumnMetaData> maybeMeta = meta(path);
        if (maybeMeta.isEmpty()) {
            return Optional.empty();
        }
        ColumnMetaData columnMeta = maybeMeta.orElseThrow();
        if (columnMeta.statistics().isEmpty()) {
            return Optional.empty();
        }
        return primitiveKind(path)
                .map(kind -> new ColumnStats(kind, columnMeta.statistics().orElseThrow()));
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
        ColumnChunk chunk = chunkByPath.get(path);
        if (chunk == null
                || chunk.offsetIndexOffset().isEmpty()
                || chunk.offsetIndexLength().isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(loader.readOffsetIndex(
                    chunk.offsetIndexOffset().getAsLong(),
                    chunk.offsetIndexLength().getAsInt()));
        } catch (RuntimeException _) {
            return Optional.empty();
        }
    }

    private Optional<ColumnIndex> loadColumnIndex(ColumnPath path) {
        ColumnChunk chunk = chunkByPath.get(path);
        if (chunk == null
                || chunk.columnIndexOffset().isEmpty()
                || chunk.columnIndexLength().isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(loader.readColumnIndex(
                    chunk.columnIndexOffset().getAsLong(),
                    chunk.columnIndexLength().getAsInt()));
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
        return Optional.of(new ColumnPageStats(kind.orElseThrow(), ci.orElseThrow(), oi.orElseThrow()));
    }

    private Optional<ColumnBloom> loadBloom(ColumnPath path) {
        Optional<ColumnMetaData> maybeMeta = meta(path);
        if (maybeMeta.isEmpty() || maybeMeta.orElseThrow().bloomFilterOffset().isEmpty()) {
            return Optional.empty();
        }
        Optional<PrimitiveKind> kind = primitiveKind(path);
        if (kind.isEmpty()) {
            return Optional.empty();
        }
        ColumnMetaData columnMeta = maybeMeta.orElseThrow();
        try {
            int length = columnMeta.bloomFilterLength().isPresent()
                    ? Math.toIntExact(columnMeta.bloomFilterLength().getAsLong())
                    : -1;
            SplitBlockBloomFilter filter =
                    loader.readBloom(columnMeta.bloomFilterOffset().getAsLong(), length);
            return Optional.of(new ColumnBloom(kind.orElseThrow(), filter));
        } catch (RuntimeException _) {
            return Optional.empty();
        }
    }

    private static Map<ColumnPath, ColumnChunk> indexChunksByPath(RowGroup rowGroup) {
        Map<ColumnPath, ColumnChunk> index = new LinkedHashMap<>();
        for (ColumnChunk chunk : rowGroup.columns()) {
            chunk.metaData().ifPresent(meta -> index.put(ColumnPath.of(meta.pathInSchema()), chunk));
        }
        return index;
    }
}
