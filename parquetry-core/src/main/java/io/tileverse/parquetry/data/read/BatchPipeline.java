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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.OptionalInt;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.BatchRowAccessor;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Synchronous batch pipeline. Iterates surviving row groups on the caller's thread; for each row group, fetches the
 * projected column chunks once, drains {@link BatchRowGroupReader#nextBatch()} into the returned stream, and closes the
 * row group's resources when its batches are exhausted (or the stream is closed early).
 *
 * <p>No background producer, no queue, no thread handoff. Prefetching of column-chunk bytes is the {@code RangeReader}
 * caching layer's responsibility.
 */
public final class BatchPipeline {

    private BatchPipeline() {}

    /**
     * Returns a closeable stream that yields one {@link ParquetRecordBatch} at a time across {@code survivors}. Closing
     * the stream releases any in-flight row-group resources.
     */
    @MustBeClosed
    public static Stream<ParquetRecordBatch> batches(
            @NonNull ParquetSchema fileSchema,
            @NonNull ParquetSchema projectedSchema,
            @NonNull List<RowGroupSurvivor> survivors,
            @NonNull ColumnFetcher columnFetcher,
            @NonNull OptionalInt batchSizeCap) {
        BatchIterator iterator = new BatchIterator(fileSchema, projectedSchema, survivors, columnFetcher, batchSizeCap);
        Spliterator<ParquetRecordBatch> spliterator =
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, /*parallel*/ false).onClose(iterator::close);
    }

    /**
     * Returns a closeable stream of rows materialized through {@code materializer}. Each batch is iterated row-by-row
     * and closed when its row stream ends; closing the outer stream cascades to the underlying batch iterator.
     */
    @MustBeClosed
    public static <T> Stream<T> rows(
            @NonNull ParquetSchema fileSchema,
            @NonNull ParquetSchema projectedSchema,
            @NonNull List<RowGroupSurvivor> survivors,
            @NonNull ColumnFetcher columnFetcher,
            @NonNull OptionalInt batchSizeCap,
            @NonNull Materializer<T> materializer) {
        Stream<ParquetRecordBatch> batchStream =
                batches(fileSchema, projectedSchema, survivors, columnFetcher, batchSizeCap);
        return batchStream.flatMap(batch -> rowsFromBatch(batch, materializer));
    }

    private static <T> Stream<T> rowsFromBatch(ParquetRecordBatch batch, Materializer<T> materializer) {
        ParquetSchema schema = batch.projectedSchema();
        Stream<T> rows = IntStream.range(0, batch.rowCount())
                .mapToObj(rowIndex -> materializer.materialize(schema, new BatchRowAccessor(batch, rowIndex)));
        return rows.onClose(batch::close);
    }

    // ---- iterator ----

    /**
     * Pulls one {@link ParquetRecordBatch} at a time. Advances to the next row group when the current one is drained,
     * closing the previous row group's reader and pooled chunks before fetching the next.
     */
    private static final class BatchIterator implements Iterator<ParquetRecordBatch>, AutoCloseable {

        private final ParquetSchema fileSchema;
        private final ParquetSchema projectedSchema;
        private final ColumnFetcher columnFetcher;
        private final OptionalInt batchSizeCap;
        private final Iterator<RowGroupSurvivor> survivors;

        private BatchRowGroupReader currentReader;
        private List<FetchedColumnChunk> currentChunks;

        BatchIterator(
                ParquetSchema fileSchema,
                ParquetSchema projectedSchema,
                List<RowGroupSurvivor> survivors,
                ColumnFetcher columnFetcher,
                OptionalInt batchSizeCap) {
            this.fileSchema = fileSchema;
            this.projectedSchema = projectedSchema;
            this.columnFetcher = columnFetcher;
            this.batchSizeCap = batchSizeCap;
            this.survivors = List.copyOf(survivors).iterator();
        }

        @Override
        public boolean hasNext() {
            while (currentReader == null || !currentReader.hasMore()) {
                releaseCurrentRowGroup();
                if (!survivors.hasNext()) {
                    return false;
                }
                advanceTo(survivors.next());
            }
            return true;
        }

        @Override
        public ParquetRecordBatch next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return currentReader.nextBatch();
        }

        @Override
        public void close() {
            releaseCurrentRowGroup();
        }

        private void advanceTo(RowGroupSurvivor survivor) {
            List<FetchedColumnChunk> chunks = fetchProjectedColumns(survivor);
            try {
                currentReader = new BatchRowGroupReader(chunks, projectedSchema, fileSchema, batchSizeCap);
                currentChunks = chunks;
            } catch (RuntimeException e) {
                closeChunks(chunks);
                throw e;
            }
        }

        private void releaseCurrentRowGroup() {
            BatchRowGroupReader reader = currentReader;
            List<FetchedColumnChunk> chunks = currentChunks;
            currentReader = null;
            currentChunks = null;
            if (reader != null) {
                try {
                    reader.close();
                } catch (RuntimeException _) {
                    // best-effort; the chunk close below still runs
                }
            }
            if (chunks != null) {
                closeChunks(chunks);
            }
        }

        private List<FetchedColumnChunk> fetchProjectedColumns(RowGroupSurvivor survivor) {
            Map<List<String>, ColumnChunk> chunksByPath = indexChunksByPath(survivor.rowGroup());
            List<ColumnPath> projectedLeaves = projectedSchema.leafColumns();
            List<FetchedColumnChunk> fetched = new ArrayList<>(projectedLeaves.size());
            try {
                for (ColumnPath path : projectedLeaves) {
                    ColumnChunk chunk = chunksByPath.get(path.parts());
                    if (chunk == null) {
                        throw new IllegalStateException("Row group does not contain column " + path.dot());
                    }
                    fetched.add(columnFetcher.fetch(chunk, path));
                }
                return fetched;
            } catch (IOException e) {
                closeChunks(fetched);
                throw new UncheckedIOException("Column fetch failed in row group", e);
            } catch (RuntimeException e) {
                closeChunks(fetched);
                throw e;
            }
        }

        private static Map<List<String>, ColumnChunk> indexChunksByPath(
                io.tileverse.parquetry.format.RowGroup rowGroup) {
            Map<List<String>, ColumnChunk> index = new LinkedHashMap<>();
            for (ColumnChunk chunk : rowGroup.columns()) {
                ColumnMetaData meta = chunk.metaData()
                        .orElseThrow(() ->
                                new IllegalStateException("ColumnChunk without inline metadata is not supported"));
                index.put(meta.pathInSchema(), chunk);
            }
            return index;
        }

        private static void closeChunks(List<FetchedColumnChunk> chunks) {
            for (FetchedColumnChunk chunk : chunks) {
                try {
                    chunk.close();
                } catch (RuntimeException _) {
                    // best-effort; pool stats surface any chronic leak
                }
            }
        }
    }
}
