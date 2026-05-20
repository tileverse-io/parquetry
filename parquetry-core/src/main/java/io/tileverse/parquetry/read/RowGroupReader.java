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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.tileverse.storage.RangeReader;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.materializer.RowAccessor;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Reads one Parquet row group end-to-end: fan out one compressed-chunk fetch per projected column, wire each result
 * into a {@link ColumnReader} that decodes pages lazily, then walk the {@link RecordAssembler} row by row, handing each
 * row to the caller's {@link Materializer}.
 *
 * <h2>Streaming memory contract</h2>
 *
 * <p>Per the package documentation: per active row group, resident bytes equals the sum of compressed column-chunk
 * bytes (held in pooled buffers, one borrow per column) plus, while assembling, one decompressed page worth of bytes
 * per column. Decompressed column-chunk bodies are never materialized whole. The lazy page walk lives in the
 * {@link ColumnReader} implementation that wraps a {@link FetchedColumnChunk}; {@code RowGroupReader} owns the
 * buffer-pool lifecycle.
 *
 * <h2>Concurrency modes</h2>
 *
 * <ul>
 *   <li>{@link ConcurrencyMode#SYNC} and {@link ConcurrencyMode#PREFETCH_ONLY} - columns fetched sequentially on the
 *       calling thread. {@code PREFETCH_ONLY}'s parallelism lives in the outer {@code RowGroupPipeline} (row groups N+k
 *       prefetched while the consumer iterates N); column fan-out inside a row group stays disabled.
 *   <li>{@link ConcurrencyMode#FAN_OUT_ONLY} and {@link ConcurrencyMode#FULL} - columns fetched in parallel via a
 *       {@link StructuredTaskScope}, one virtual thread per projected column. First failure cancels the scope; any
 *       chunks already fetched are closed before the {@link UncheckedIOException} propagates. {@code FULL} additionally
 *       enables row-group prefetch in the outer pipeline; here in the row-group reader the two modes are
 *       indistinguishable.
 *   <li>{@link ConcurrencyMode#AUTO} - {@code ParquetDataset.open} is expected to resolve this to a concrete mode from
 *       {@code RangeReader.localityHint()}; if it reaches us we conservatively treat it as fan-out (the cloud-read
 *       default).
 * </ul>
 *
 * <h2>Lifecycle</h2>
 *
 * <p>Instances are single-use: {@link #read(Materializer)} can be invoked at most once and returns a {@link Stream}
 * whose {@link Stream#close()} hook returns every borrowed compressed buffer to the pool. {@link #close()} is also
 * available for callers that want to release buffers without consuming the stream.
 *
 * <p>{@code close()} releases column-reader-owned resources (current page's pooled decompressed-values buffer) before
 * releasing the column-chunk-owned compressed buffers, so the streaming memory contract holds along every exit path.
 */
final class RowGroupReader implements AutoCloseable {

    private final ParquetSchema fileSchema;
    private final ParquetSchema projectedSchema;
    private final RowGroup rowGroup;
    private final ReadOptions options;
    private final ColumnFetcher columnFetcher;
    private final BiFunction<FetchedColumnChunk, Field.Primitive, ColumnReader> columnReaderFactory;

    private final List<FetchedColumnChunk> ownedChunks = new ArrayList<>();
    private final List<ColumnReader> ownedReaders = new ArrayList<>();
    private boolean closed;
    private boolean read;

    /**
     * Builds a row-group reader backed by real I/O.
     *
     * <p>The {@code reader} parameter is held for parity with future wiring where the {@code ColumnFetcher.real}
     * factory consumes it; this constructor does not perform direct {@code RangeReader} reads outside the fetcher.
     */
    public RowGroupReader(
            RangeReader reader,
            ParquetSchema fileSchema,
            ParquetSchema projectedSchema,
            RowGroup rowGroup,
            ReadOptions options,
            ColumnFetcher columnFetcher) {
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(options, "options");
        this(
                Objects.requireNonNull(fileSchema, "fileSchema"),
                Objects.requireNonNull(projectedSchema, "projectedSchema"),
                Objects.requireNonNull(rowGroup, "rowGroup"),
                options,
                Objects.requireNonNull(columnFetcher, "columnFetcher"),
                defaultColumnReaderFactory());
    }

    /**
     * Test-friendly constructor that lets callers supply a column-reader factory directly, bypassing the page-cursor
     * the production page-cursor path. The factory receives the fetched compressed chunk plus the file-schema leaf and
     * must return a fully positioned {@link ColumnReader} for the chunk's rows.
     */
    RowGroupReader(
            ParquetSchema fileSchema,
            ParquetSchema projectedSchema,
            RowGroup rowGroup,
            ReadOptions options,
            ColumnFetcher columnFetcher,
            BiFunction<FetchedColumnChunk, Field.Primitive, ColumnReader> columnReaderFactory) {
        this.fileSchema = Objects.requireNonNull(fileSchema, "fileSchema");
        this.projectedSchema = Objects.requireNonNull(projectedSchema, "projectedSchema");
        this.rowGroup = Objects.requireNonNull(rowGroup, "rowGroup");
        this.options = Objects.requireNonNull(options, "options");
        this.columnFetcher = Objects.requireNonNull(columnFetcher, "columnFetcher");
        this.columnReaderFactory = Objects.requireNonNull(columnReaderFactory, "columnReaderFactory");
    }

    /**
     * Returns a one-shot stream of materialized records over this row group.
     *
     * <p>The fetch + dictionary decode happens synchronously inside this call so that {@link IOException}s surface as
     * an {@link UncheckedIOException} at the call site (real I/O errors propagate from {@code RangeReader}). Data pages
     * remain compressed in their pooled buffers until the column readers walk them inside the stream's iteration.
     *
     * <p>The returned stream's {@link Stream#close()} hook closes this {@code RowGroupReader}, which returns every
     * borrowed compressed buffer to its pool. Callers should always use try-with-resources on the stream.
     */
    public <T> Stream<T> read(Materializer<T> materializer) {
        Objects.requireNonNull(materializer, "materializer");
        ensureOpenAndUnread();
        read = true;
        try {
            List<ProjectedColumn> projected = resolveProjectedColumns();
            fetchAllColumnsInto(projected, ownedChunks);
            List<ColumnReader> readers = buildColumnReaders(projected, ownedChunks);
            ownedReaders.addAll(readers);
            RecordAssembler assembler = new RecordAssembler(projectedSchema, readers);
            return streamFromAssembler(assembler, materializer).onClose(this::close);
        } catch (RuntimeException e) {
            close();
            throw e;
        }
    }

    /**
     * Closes column readers first (returning each reader's current decompressed-page pooled buffer), then the
     * compressed column-chunk buffers. The order matters: a column reader's page buffer is borrowed from the same pool
     * the chunk's compressed buffer came from, but it is owned by the reader, so it must be released before the chunk's
     * compressed slice is closed (which currently has no aliasing relation to the page buffer but the explicit ordering
     * documents the contract for future caches that might share storage).
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        RuntimeException firstFailure = null;
        try {
            closeAllReaders(ownedReaders);
        } catch (RuntimeException e) {
            firstFailure = e;
        }
        try {
            closeAll(ownedChunks);
        } catch (RuntimeException e) {
            if (firstFailure == null) {
                firstFailure = e;
            } else {
                firstFailure.addSuppressed(e);
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    // --- projected column resolution ---

    private List<ProjectedColumn> resolveProjectedColumns() {
        Map<List<String>, ColumnChunk> chunksByPath = indexChunksByPath();
        List<ColumnPath> projectedLeaves = projectedSchema.leafColumns();
        List<ProjectedColumn> projected = new ArrayList<>(projectedLeaves.size());
        for (ColumnPath path : projectedLeaves) {
            Field.Primitive leaf = resolvePrimitiveLeaf(path);
            ColumnChunk chunk = chunksByPath.get(path.parts());
            if (chunk == null) {
                throw new IllegalStateException("Row group does not contain column " + path.dot());
            }
            LevelMaximaResolver.LevelMaxima maxima = LevelMaximaResolver.resolve(fileSchema, path);
            projected.add(
                    new ProjectedColumn(path, leaf, chunk, maxima.maxRepetitionLevel(), maxima.maxDefinitionLevel()));
        }
        return projected;
    }

    private Map<List<String>, ColumnChunk> indexChunksByPath() {
        Map<List<String>, ColumnChunk> index = new LinkedHashMap<>();
        for (ColumnChunk chunk : rowGroup.columns()) {
            ColumnMetaData meta = chunk.metaData()
                    .orElseThrow(
                            () -> new IllegalStateException("ColumnChunk without inline metadata is not supported"));
            index.put(meta.pathInSchema(), chunk);
        }
        return index;
    }

    private Field.Primitive resolvePrimitiveLeaf(ColumnPath path) {
        Field field = fileSchema
                .find(path)
                .orElseThrow(() -> new IllegalArgumentException("Projected column " + path.dot() + " not in schema"));
        if (!(field instanceof Field.Primitive primitive)) {
            throw new IllegalArgumentException("Projected column " + path.dot() + " is not a primitive leaf");
        }
        return primitive;
    }

    // --- fetch fan-out ---

    private void fetchAllColumnsInto(List<ProjectedColumn> projected, List<FetchedColumnChunk> out) {
        if (fansOutColumns(options.concurrencyMode())) {
            fetchInParallel(projected, out);
        } else {
            fetchSequentially(projected, out);
        }
    }

    /**
     * Whether the given mode wants per-column virtual-thread fan-out inside a single row group. {@code PREFETCH_ONLY}
     * groups with {@code SYNC} here because its parallelism is row-group prefetch in the outer pipeline, not column
     * fan-out inside one row group. {@code AUTO} should have been resolved by {@code ParquetDataset.open} from the
     * {@code RangeReader} locality hint; if it leaks through to us we conservatively assume cloud (fan-out).
     */
    private static boolean fansOutColumns(ConcurrencyMode mode) {
        return switch (mode) {
            case SYNC, PREFETCH_ONLY -> false;
            case FAN_OUT_ONLY, FULL, AUTO -> true;
        };
    }

    private void fetchSequentially(List<ProjectedColumn> projected, List<FetchedColumnChunk> out) {
        try {
            for (ProjectedColumn pc : projected) {
                out.add(columnFetcher.fetch(pc.chunk(), pc.path()));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Column fetch failed in row group", e);
        }
    }

    /**
     * Forks one subtask per projected column; collects results in declaration order so the downstream zip with
     * {@link ProjectedColumn} stays deterministic. Any subtask failure cancels the scope and propagates as an
     * {@link UncheckedIOException} after closing every successfully-fetched sibling chunk; no borrowed buffer leaks.
     */
    private void fetchInParallel(List<ProjectedColumn> projected, List<FetchedColumnChunk> out) {
        List<Subtask<FetchedColumnChunk>> tasks = null;
        // Default joiner (allSuccessfulOrThrow) returns Void/null; we collect per-subtask results below.
        try (StructuredTaskScope<FetchedColumnChunk, Void> scope = StructuredTaskScope.<FetchedColumnChunk>open()) {
            tasks = forkPerColumn(scope, projected);
            scope.join();
            collectSubtaskResults(tasks, out);
        } catch (StructuredTaskScope.FailedException e) {
            closeSuccessfulTasks(tasks);
            throw new UncheckedIOException("Column fetch failed in row group", toIOException(e.getCause()));
        } catch (InterruptedException e) {
            closeSuccessfulTasks(tasks);
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while fetching columns in parallel", e);
        }
    }

    /**
     * Closes every chunk produced by a subtask in {@link Subtask.State#SUCCESS} state so their pooled compressed
     * buffers go back to the pool even though we are about to abandon the row group. Safe to call with a null tasks
     * list (no forks were created yet).
     */
    private void closeSuccessfulTasks(List<Subtask<FetchedColumnChunk>> tasks) {
        if (tasks == null) {
            return;
        }
        List<FetchedColumnChunk> toClose = new ArrayList<>(tasks.size());
        for (Subtask<FetchedColumnChunk> task : tasks) {
            if (task.state() == Subtask.State.SUCCESS) {
                toClose.add(task.get());
            }
        }
        closeAll(toClose);
    }

    private List<Subtask<FetchedColumnChunk>> forkPerColumn(
            StructuredTaskScope<FetchedColumnChunk, Void> scope, List<ProjectedColumn> projected) {
        List<Subtask<FetchedColumnChunk>> tasks = new ArrayList<>(projected.size());
        for (ProjectedColumn pc : projected) {
            tasks.add(scope.fork(() -> columnFetcher.fetch(pc.chunk(), pc.path())));
        }
        return tasks;
    }

    /**
     * Drains every subtask result into {@code out} in declaration order. With the default {@code allSuccessfulOrThrow}
     * joiner, {@code scope.join()} returns normally only when every subtask is in {@link Subtask.State#SUCCESS}, so
     * {@link Subtask#get()} is always safe here; failure paths run in {@link #fetchInParallel} instead.
     */
    private void collectSubtaskResults(List<Subtask<FetchedColumnChunk>> tasks, List<FetchedColumnChunk> out) {
        for (Subtask<FetchedColumnChunk> task : tasks) {
            out.add(task.get());
        }
    }

    private IOException toIOException(Throwable cause) {
        if (cause instanceof IOException ioe) {
            return ioe;
        }
        return new IOException("Column fetch failed", cause);
    }

    // --- column reader wiring ---

    private List<ColumnReader> buildColumnReaders(List<ProjectedColumn> projected, List<FetchedColumnChunk> chunks) {
        List<ColumnReader> readers = new ArrayList<>(projected.size());
        for (int i = 0; i < projected.size(); i++) {
            ProjectedColumn pc = projected.get(i);
            FetchedColumnChunk chunk = chunks.get(i);
            ColumnReader reader = columnReaderFactory.apply(chunk, pc.leaf());
            readers.add(Objects.requireNonNull(
                    reader, "columnReaderFactory returned null for " + pc.path().dot()));
        }
        return readers;
    }

    /**
     * Production column-reader factory: wraps each {@link FetchedColumnChunk} in a {@link StreamingColumnReader} that
     * walks the chunk's compressed bytes page by page using the {@code DataPageReader} sealed abstraction. Each page
     * allocates one {@link java.lang.foreign.Arena} for its decompressed bytes; the Arena is released when the reader
     * advances to the next page.
     */
    private static BiFunction<FetchedColumnChunk, Field.Primitive, ColumnReader> defaultColumnReaderFactory() {
        return (chunk, leaf) -> new StreamingColumnReader(
                chunk.path(),
                chunk.maxRepetitionLevel(),
                chunk.maxDefinitionLevel(),
                leaf,
                chunk.compressedBuffer().buffer(),
                chunk.dictionary(),
                chunk.metadata().codec(),
                chunk.metadata().numValues());
    }

    // --- stream plumbing ---

    private <T> Stream<T> streamFromAssembler(RecordAssembler assembler, Materializer<T> materializer) {
        Iterator<T> iterator = new AssemblerIterator<>(assembler, materializer, projectedSchema);
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, /*parallel*/ false);
    }

    private void ensureOpenAndUnread() {
        if (closed) {
            throw new IllegalStateException("RowGroupReader is closed");
        }
        if (read) {
            throw new IllegalStateException("RowGroupReader is single-use; read() already called");
        }
    }

    private static void closeAllReaders(List<ColumnReader> readers) {
        RuntimeException firstFailure = null;
        for (ColumnReader reader : readers) {
            try {
                reader.close();
            } catch (RuntimeException e) {
                if (firstFailure == null) {
                    firstFailure = e;
                } else {
                    firstFailure.addSuppressed(e);
                }
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    private static void closeAll(List<FetchedColumnChunk> chunks) {
        RuntimeException firstFailure = null;
        for (FetchedColumnChunk chunk : chunks) {
            try {
                chunk.close();
            } catch (RuntimeException e) {
                if (firstFailure == null) {
                    firstFailure = e;
                } else {
                    firstFailure.addSuppressed(e);
                }
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    // --- internal value types ---

    private record ProjectedColumn(
            ColumnPath path, Field.Primitive leaf, ColumnChunk chunk, int maxRepetitionLevel, int maxDefinitionLevel) {}

    /**
     * Pulls one row per call from the underlying {@link RecordAssembler} and hands it to {@code materializer}.
     * Materialization happens on the consumer thread; page decoding inside the column readers happens here too, which
     * is why the streaming memory contract bounds resident bytes at one decompressed page per column.
     */
    private static final class AssemblerIterator<T> implements Iterator<T> {

        private final RecordAssembler assembler;
        private final Materializer<T> materializer;
        private final ParquetSchema projectedSchema;

        AssemblerIterator(RecordAssembler assembler, Materializer<T> materializer, ParquetSchema projectedSchema) {
            this.assembler = assembler;
            this.materializer = materializer;
            this.projectedSchema = projectedSchema;
        }

        @Override
        public boolean hasNext() {
            return assembler.hasNext();
        }

        @Override
        public T next() {
            if (!assembler.hasNext()) {
                throw new NoSuchElementException("No more rows in row group");
            }
            RowAccessor row = assembler.next();
            return materializer.materialize(projectedSchema, row);
        }
    }
}
