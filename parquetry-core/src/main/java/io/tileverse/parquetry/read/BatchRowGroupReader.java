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
package io.tileverse.parquetry.read;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Per-row-group driver that emits {@link ParquetRecordBatch} instances.
 *
 * <p>Owns one {@link BatchColumnReader} per projected leaf. Each {@link #nextBatch()} call:
 *
 * <ol>
 *   <li>Computes the batch row count as {@code min(rowsRemainingInCurrentPage across all columns, batchSizeCap)}.
 *   <li>Asks each column reader for a vector at that row count.
 *   <li>Allocates a {@link Arena#ofConfined() confined Arena} as the batch's token resource and wraps everything in a
 *       {@link DefaultParquetRecordBatch} that owns that Arena.
 * </ol>
 *
 * <p>Page memory lifecycle: each column reader creates its own confined Arena per page (in
 * {@link BatchColumnReader#loadNextPage}). That Arena is closed when the reader advances past the page. Callers must
 * not hold references to vectors from a batch after the next page is loaded in any column. When {@code batchSizeCap}
 * splits a single page across multiple batches, the page Arena stays open until all rows in that page are consumed.
 *
 * <p>Currently handles the FLAT-schema case. Nested-shape vector assembly (LIST / MAP / STRUCT) is a follow-up.
 */
public final class BatchRowGroupReader implements AutoCloseable {

    private final ParquetSchema projectedSchema;
    private final OptionalInt batchSizeCap;
    private final List<ProjectedLeaf> projectedLeaves;

    // Lazily built on first nextBatch() call; keyed by column path in declaration order.
    private Map<ColumnPath, BatchColumnReader> columnReaders;

    public BatchRowGroupReader(
            @NonNull List<FetchedColumnChunk> chunks,
            @NonNull ParquetSchema projectedSchema,
            @NonNull ParquetSchema fileSchema,
            @NonNull OptionalInt batchSizeCap) {
        this.projectedSchema = projectedSchema;
        this.batchSizeCap = batchSizeCap;
        this.projectedLeaves = resolveProjectedLeaves(chunks, fileSchema);
    }

    /**
     * Returns true while any column reader still has rows. Before the first batch is read, returns true if there are
     * projected columns to read from.
     */
    public boolean hasMore() {
        if (columnReaders == null) {
            return !projectedLeaves.isEmpty();
        }
        for (BatchColumnReader reader : columnReaders.values()) {
            if (reader.hasMore()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Builds and returns the next batch. The batch is backed by a fresh {@link Arena#ofConfined() confined Arena} that
     * is closed when the batch is closed. Page memory is managed per-column-reader (see class javadoc) and is
     * invalidated when the reader advances to the next page, not when the batch closes.
     *
     * <p>After collecting per-leaf flat vectors, this method calls
     * {@link NestedVectorAssembler#wrapStructGroups(ParquetSchema, Map, int)} to wrap child leaf vectors under any
     * non-repeated, non-LIST/MAP group nodes in the projected schema into
     * {@link io.tileverse.parquetry.batch.StructVector} carriers. LIST and MAP wrapping requires exposing the per-leaf
     * rep-level stream from {@link BatchColumnReader} and is deferred to a follow-up task.
     *
     * @throws IllegalStateException if called when {@link #hasMore()} returns false
     */
    @MustBeClosed
    public ParquetRecordBatch nextBatch() {
        if (!hasMore()) {
            throw new IllegalStateException("No more rows to read");
        }
        Arena batchArena = Arena.ofConfined();
        try {
            if (columnReaders == null) {
                // Pass batchArena for API compatibility; readers allocate page bytes into their own per-page Arenas.
                columnReaders = buildColumnReaders(batchArena);
            }
            int batchRows = computeBatchRows();
            Map<ColumnPath, ColumnVector> leafVectors = readVectors(batchRows);
            Map<ColumnPath, ColumnVector> vectors =
                    NestedVectorAssembler.wrapStructGroups(projectedSchema, leafVectors, batchRows);
            return new DefaultParquetRecordBatch(projectedSchema, vectors, batchRows, batchArena);
        } catch (RuntimeException e) {
            batchArena.close();
            throw e;
        }
    }

    /**
     * Closes all column readers, releasing any page Arenas they still hold.
     *
     * <p>Must be called when done with the row group, whether rows were fully consumed or the consumer broke early.
     * Without this call, the last page's Arena in each column reader would leak (it is only closed on the next
     * {@code loadNextPage} call, which never comes if the reader is abandoned).
     *
     * <p>Each column reader's {@link BatchColumnReader#close()} is invoked inside a try/catch so that one failing close
     * does not leave the remaining readers unclosed.
     */
    @Override
    public void close() {
        if (columnReaders == null) {
            return;
        }
        RuntimeException firstFailure = null;
        for (BatchColumnReader reader : columnReaders.values()) {
            try {
                reader.close();
            } catch (RuntimeException e) {
                if (firstFailure == null) {
                    firstFailure = e;
                }
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    // --- column reader construction ---

    private Map<ColumnPath, BatchColumnReader> buildColumnReaders(Arena arena) {
        Map<ColumnPath, BatchColumnReader> readers = new HashMap<>();
        for (ProjectedLeaf leaf : projectedLeaves) {
            readers.put(leaf.path(), new BatchColumnReader(leaf.chunk(), leaf.leaf(), arena));
        }
        return readers;
    }

    // --- batch row count computation ---

    /**
     * Computes the row count for the next batch as the minimum of {@code rowsRemainingInCurrentPage()} across all
     * columns, further capped by {@code batchSizeCap} if present.
     *
     * <p>Taking the minimum across columns keeps all readers on the same page boundary: after this batch is consumed,
     * every column will have exhausted its current page simultaneously and be ready to advance.
     */
    private int computeBatchRows() {
        int min = Integer.MAX_VALUE;
        for (BatchColumnReader reader : columnReaders.values()) {
            min = Math.min(min, reader.rowsRemainingInCurrentPage());
        }
        if (batchSizeCap.isPresent()) {
            min = Math.min(min, batchSizeCap.getAsInt());
        }
        return min;
    }

    // --- vector reads ---

    private Map<ColumnPath, ColumnVector> readVectors(int batchRows) {
        Map<ColumnPath, ColumnVector> vectors = new HashMap<>();
        for (Map.Entry<ColumnPath, BatchColumnReader> entry : columnReaders.entrySet()) {
            vectors.put(entry.getKey(), entry.getValue().readBatch(batchRows));
        }
        return vectors;
    }

    // --- projected-leaf resolution ---

    /**
     * Resolves each fetched chunk to its file-schema leaf. Uses {@link ParquetSchema#find(ColumnPath)} to walk the
     * schema tree and cast to {@link Field.Primitive} - the same pattern {@link RowGroupReader#resolvePrimitiveLeaf}
     * uses.
     */
    private static List<ProjectedLeaf> resolveProjectedLeaves(
            List<FetchedColumnChunk> chunks, ParquetSchema fileSchema) {
        List<ProjectedLeaf> result = new ArrayList<>(chunks.size());
        for (FetchedColumnChunk chunk : chunks) {
            ColumnPath path = chunk.path();
            Field field = fileSchema
                    .find(path)
                    .orElseThrow(() -> new IllegalStateException(
                            "Projected chunk path " + path.dot() + " not found in file schema"));
            if (!(field instanceof Field.Primitive primitive)) {
                throw new IllegalStateException(
                        "Projected column " + path.dot() + " is not a primitive leaf in the file schema");
            }
            result.add(new ProjectedLeaf(path, chunk, primitive));
        }
        return result;
    }

    // --- internal value type ---

    private record ProjectedLeaf(ColumnPath path, FetchedColumnChunk chunk, Field.Primitive leaf) {}
}
