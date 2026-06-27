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
package io.tileverse.parquetry.data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.stream.Stream;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.format.BloomFilterHeader;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.ColumnOrder;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.GeospatialStatistics;
import io.tileverse.parquetry.format.KeyValue;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.SchemaElement;
import io.tileverse.parquetry.internal.write.GeoColumnSummary;
import io.tileverse.parquetry.internal.write.GeoMetadataWriter;
import io.tileverse.parquetry.internal.write.RowGroupFlushResult;
import io.tileverse.parquetry.internal.write.RowGroupWriter;
import io.tileverse.parquetry.io.ByteSink;
import io.tileverse.parquetry.observe.IndexesWritten;
import io.tileverse.parquetry.observe.RowGroupFlushed;
import io.tileverse.parquetry.observe.WriteObserver;
import io.tileverse.parquetry.observe.WriteStarted;
import io.tileverse.parquetry.observe.WriteStats;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Write entry point for emitting a single Parquet file. Owns the full file lifecycle: the leading {@code PAR1} magic,
 * one or more row groups (each consolidated in turn into the caller's {@link ByteSink sink}), the column-index /
 * offset-index / bloom-filter blobs that follow every row group, the GeoParquet metadata when configured, and the
 * trailing {@code FileMetaData} footer, footer length, and tail {@code PAR1}.
 *
 * <p>The writer never closes the sink it is given; the caller owns the sink's lifecycle. On the success path the caller
 * commits by closing the sink after a successful {@link #close()}; on the failure path the caller does not close the
 * sink (and closes its backing storage instead). A close that finalizes the footer does not commit by itself.
 *
 * <p>Writers are not thread-safe; one logical record stream per instance. {@link #close()} is required: it
 * force-flushes the in-flight row group, writes the footer, and deletes the temp directory backing the per-column
 * accumulators. Close is idempotent. On any internal failure the writer enters a terminal failed state; subsequent
 * record-emitting calls throw {@link ParquetWriteException}, and {@link #close()} cleans up without writing a footer.
 * An interrupt mid-stream emerges as an {@link java.io.UncheckedIOException} wrapping an
 * {@link java.io.InterruptedIOException} on the next record-emitting call, with the interrupt status restored and the
 * writer transitioned to failed.
 *
 * <p>The class is non-final and the file-layout helpers ({@link #flushCurrentRowGroup flushCurrentRowGroup},
 * {@link #placeIndexBlobsAndPatch placeIndexBlobsAndPatch}, {@link #writeBloomFilters writeBloomFilters},
 * {@link #writeColumnIndexes writeColumnIndexes}, {@link #writeOffsetIndexes writeOffsetIndexes}, {@link #writeFooter
 * writeFooter}, {@link #buildKeyValueMetadata buildKeyValueMetadata}, {@link #openRowGroupWriter openRowGroupWriter})
 * are {@code protected}. A subclass can specialize how index blobs are laid out, where row-group writers come from, or
 * what extra key/value metadata lands in the footer without re-implementing the rest of the write pipeline. Lifecycle
 * primitives ({@link #checkOpen}, {@link #checkInterrupt}, {@link #markFailed}, {@link #cleanupAfterFailure},
 * {@link #maybeFlushRowGroup}) and the write-observer hooks ({@link #maybeFireProgress}, {@link #fireRowGroupFlushed},
 * {@link #fireOnClose}) are likewise {@code protected} for subclasses to participate in.
 */
public class ParquetFileWriter implements AutoCloseable {

    private static final byte[] MAGIC = {'P', 'A', 'R', '1'};
    private static final String GEO_KEY = "geo";
    private static final String CREATED_BY = "parquetry";

    private final ByteSink out;
    private final WriteOptions options;
    private final WriteObserver observer;
    private final ParquetSchema schema;
    private final Path tempDir;
    private final GeoMetadataWriter geoWriter;
    private final long maxRowGroupBytesLimit;
    private final List<RowGroup> completedRowGroups = new ArrayList<>();
    private final Map<ColumnPath, GeoColumnSummary> geoSummaries = new LinkedHashMap<>();
    private final Instant writerStart = Instant.now();

    private RowGroupWriter currentRowGroup;
    private ParquetRecordBatchBuilder activeAppender;
    private Instant currentRowGroupStart = Instant.now();
    private long totalRows;
    private long lastProgressMilestone;
    private long observedCompressedBytes;
    private long observedUncompressedBytes;
    private boolean writeStartedFired;
    private boolean closed;
    private boolean failed;

    /**
     * Opens a writer with {@link WriteOptions#defaults()}. The writer never closes {@code sink}; the caller owns it.
     */
    public static ParquetFileWriter create(ByteSink sink, ParquetSchema schema) {
        return create(sink, schema, WriteOptions.defaults());
    }

    /** Opens a writer with the supplied options. The writer never closes {@code sink}; the caller owns it. */
    public static ParquetFileWriter create(
            @NonNull ByteSink sink, @NonNull ParquetSchema rawSchema, @NonNull WriteOptions options) {
        return assembleWriter(sink, rawSchema, options, WriteOptions.MAX_ROW_GROUP_BYTES_LIMIT);
    }

    /**
     * Package-private factory used by {@link #create} and by tests that need a reduced {@code maxRowGroupBytesLimit}. A
     * tiny limit lets a test trip the row-group byte safety valve without accumulating the production limit's worth of
     * bytes.
     */
    static ParquetFileWriter assembleWriter(
            @NonNull ByteSink sink,
            @NonNull ParquetSchema rawSchema,
            @NonNull WriteOptions options,
            long maxRowGroupBytesLimit) {
        GeoMetadataWriter geoWriter = new GeoMetadataWriter(options);
        ParquetSchema schema = geoWriter.applyV2LogicalTypes(rawSchema);
        Path tempDir = createTempDir(options);
        writeLeadingMagic(sink, tempDir);
        RowGroupWriter first = openRowGroupWriter(options, schema, tempDir, sink.position());
        return new ParquetFileWriter(sink, options, schema, tempDir, geoWriter, first, maxRowGroupBytesLimit);
    }

    /**
     * Convenience overload wrapping {@code out} in a {@link ByteSink} via {@link ByteSink#ofOutputStream}. The writer
     * never closes the sink, and therefore never closes {@code out}; the caller keeps ownership and closes {@code out}
     * to commit after a successful {@link #close()}.
     */
    public static ParquetFileWriter create(OutputStream out, ParquetSchema schema) {
        return create(out, schema, WriteOptions.defaults());
    }

    /**
     * Convenience overload wrapping {@code out} in a {@link ByteSink} via {@link ByteSink#ofOutputStream}. The writer
     * never closes the sink, and therefore never closes {@code out}; the caller keeps ownership and closes {@code out}
     * to commit after a successful {@link #close()}.
     */
    public static ParquetFileWriter create(OutputStream out, ParquetSchema schema, WriteOptions options) {
        return create(ByteSink.ofOutputStream(out), schema, options);
    }

    protected ParquetFileWriter(
            ByteSink out,
            WriteOptions options,
            ParquetSchema schema,
            Path tempDir,
            GeoMetadataWriter geoWriter,
            RowGroupWriter first,
            long maxRowGroupBytesLimit) {
        this.out = out;
        this.options = options;
        this.observer = options.writeObserver();
        this.schema = schema;
        this.tempDir = tempDir;
        this.geoWriter = geoWriter;
        this.currentRowGroup = first;
        this.maxRowGroupBytesLimit = maxRowGroupBytesLimit;
    }

    /** Appends a whole batch of rows via the vectorised column-chunk path. */
    public void writeBatch(@NonNull ParquetRecordBatch batch) {
        checkInterrupt();
        checkOpen();
        appendBatchToCurrentRowGroup(batch);
    }

    /**
     * Appends a batch during {@link #close()} when the writer has already transitioned to closed. Skips the open check
     * that {@link #writeBatch} applies, because draining the active appender's trailing rows is part of closing, not a
     * post-close write.
     */
    void writeClosingBatch(@NonNull ParquetRecordBatch batch) {
        checkInterrupt();
        appendBatchToCurrentRowGroup(batch);
    }

    private void appendBatchToCurrentRowGroup(ParquetRecordBatch batch) {
        try {
            fireWriteStartedOnce();
            currentRowGroup.appendBatch(batch);
            totalRows += batch.rowCount();
            maybeFireProgress();
            maybeFlushRowGroup();
        } catch (RuntimeException e) {
            markFailed();
            throw e;
        }
    }

    /**
     * A row-appender bound to this writer that auto-flushes a batch every
     * {@value ParquetRecordBatchBuilder#DEFAULT_BATCH_ROWS} rows.
     *
     * <p>The writer flushes the appender's pending rows during {@link #close()}, hence an explicit
     * {@link ParquetRecordBatchBuilder#flush()} is optional; the appender must be used before the writer is closed. One
     * appender per writer is the expected use; obtaining a second appender flushes the first one's pending rows.
     */
    public ParquetRecordBatchBuilder appender() {
        return appender(ParquetRecordBatchBuilder.DEFAULT_BATCH_ROWS);
    }

    /**
     * A row-appender bound to this writer that auto-flushes a batch every {@code batchRows} rows. The writer flushes
     * the appender's pending rows during {@link #close()}; one appender per writer is the expected use.
     */
    public ParquetRecordBatchBuilder appender(int batchRows) {
        return appender(batchRows, ParquetRecordBatchBuilder.DEFAULT_BATCH_BYTES);
    }

    /**
     * A row-appender that auto-flushes when it reaches {@code batchRows} rows or its accumulated cells reach
     * {@code batchBytes}. Package-private so tests can drive the byte threshold with a small value instead of writing
     * the production-sized default; the public {@link #appender(int)} uses
     * {@value ParquetRecordBatchBuilder#DEFAULT_BATCH_BYTES}.
     */
    ParquetRecordBatchBuilder appender(int batchRows, long batchBytes) {
        checkOpen();
        if (batchRows <= 0) {
            throw new IllegalArgumentException("batchRows must be positive: " + batchRows);
        }
        if (batchBytes <= 0) {
            throw new IllegalArgumentException("batchBytes must be positive: " + batchBytes);
        }
        if (activeAppender != null) {
            activeAppender.flush();
        }
        ParquetRecordBatchBuilder builder = ParquetRecordBatchBuilder.boundTo(this, schema, batchRows, batchBytes);
        this.activeAppender = builder;
        return builder;
    }

    /**
     * Explicitly closes the current row group and starts a new one. No-ops when the current row group has received no
     * rows; callers can use this as an opportunistic flush hint without tracking row counts.
     */
    public void flushRowGroup() {
        checkInterrupt();
        checkOpen();
        if (currentRowGroup.rowCount() == 0L) {
            return;
        }
        try {
            flushCurrentRowGroup();
        } catch (RuntimeException e) {
            markFailed();
            throw e;
        }
    }

    /** Total rows appended so far across every row group, including the still-open one. */
    public long totalRows() {
        return totalRows;
    }

    /** Total bytes written to the caller's sink so far, including magic and footer when present. */
    public long totalBytes() {
        return out.position();
    }

    /** Row groups that have already been consolidated to the output stream. */
    public long rowGroupsWritten() {
        return completedRowGroups.size();
    }

    /**
     * Finishes the file: force-flushes the current row group when non-empty, places the GeoParquet metadata, writes the
     * footer, footer length, and trailing magic, then deletes the temp directory. The sink is left open; the caller
     * commits by closing the sink after this returns successfully.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (failed) {
            cleanupAfterFailure();
            return;
        }
        try {
            flushActiveAppender();
            finishLastRowGroup();
            writeFooter();
        } catch (RuntimeException e) {
            cleanupAfterFailure();
            throw e;
        }
        deleteTempDirQuietly(tempDir);
        fireOnClose();
    }

    /**
     * Flushes a writer-bound appender's trailing partial batch into the current row group before it is finalized.
     * Without this, the rows accumulated since the last auto-flush are lost when the caller forgets an explicit flush.
     * No-op when the appender is empty or absent.
     */
    private void flushActiveAppender() {
        if (activeAppender != null) {
            activeAppender.flushOnClose();
        }
    }

    /** Flushes the in-flight row group when non-empty; otherwise just closes its temp resources. */
    private void finishLastRowGroup() {
        if (currentRowGroup.rowCount() > 0L) {
            flushCurrentRowGroup();
        } else {
            currentRowGroup.close();
        }
    }

    /**
     * Flushes the in-flight row group to the sink, places its index blobs, accumulates GeoParquet summaries, fires the
     * row-group-flushed progress event, and opens a fresh row-group writer. Subclasses may override to inject custom
     * post-flush bookkeeping, but must preserve the invariant that {@link #currentRowGroup} is non-null on return.
     */
    protected void flushCurrentRowGroup() {
        RowGroupFlushResult flushed = currentRowGroup.flushTo(out);
        currentRowGroup.close();
        RowGroup patched = placeIndexBlobsAndPatch(flushed);
        int rowGroupIndex = completedRowGroups.size();
        completedRowGroups.add(patched);
        accumulateGeoSummaries(flushed);
        accumulateColumnDataBytes(patched, flushed);
        fireRowGroupFlushed(rowGroupIndex, patched, flushed);
        fireIndexesWritten(patched);
        currentRowGroup = openRowGroupWriter(options, schema, tempDir, out.position());
        currentRowGroupStart = Instant.now();
    }

    private void accumulateColumnDataBytes(RowGroup patched, RowGroupFlushResult flushed) {
        observedCompressedBytes += flushed.bytesWritten();
        observedUncompressedBytes += patched.totalByteSize();
    }

    /**
     * Trips the row-group flush when the sizing policy says the current row group is full. Subclasses may override to
     * implement a richer sizing policy (e.g. based on column cardinality or external signals).
     */
    protected void maybeFlushRowGroup() {
        OptionalLong targetRows = RowGroupSizingPolicy.targetRows(options);
        if (targetRows.isPresent() && currentRowGroup.rowCount() >= targetRows.getAsLong()) {
            flushCurrentRowGroup();
            return;
        }
        long targetBytes = RowGroupSizingPolicy.targetBytes(options);
        if (targetBytes != Long.MAX_VALUE && currentRowGroup.estimatedUncompressedBytes() >= targetBytes) {
            flushCurrentRowGroup();
            return;
        }
        // Hard ceiling independent of any byte policy: keep per-row-group byte counts within an int.
        if (currentRowGroup.estimatedUncompressedBytes() >= maxRowGroupBytesLimit) {
            flushCurrentRowGroup();
        }
    }

    /**
     * Lays out the bloom-filter, column-index, and offset-index blobs after the row group, then returns a patched
     * {@link RowGroup} whose column metadata points at the placed blobs. Subclasses may override to change the layout
     * order or to skip individual index types.
     */
    protected RowGroup placeIndexBlobsAndPatch(RowGroupFlushResult flushed) {
        List<ColumnChunk> sourceChunks = flushed.rowGroup().columns();
        List<RowGroupFlushResult.ColumnArtifacts> artifacts = flushed.columnArtifacts();
        List<long[]> bloomPlacements = writeBloomFilters(artifacts);
        List<long[]> columnIndexPlacements = writeColumnIndexes(artifacts);
        List<long[]> offsetIndexPlacements = writeOffsetIndexes(artifacts);

        List<ColumnChunk> patched =
                patchColumnChunks(sourceChunks, bloomPlacements, columnIndexPlacements, offsetIndexPlacements);
        return buildPatchedRowGroup(flushed.rowGroup(), patched);
    }

    /**
     * Writes the bloom-filter blob for every column artifact and returns one {@code {offset, length}} pair per column.
     * Columns without a bloom filter get {@code {-1, -1}}.
     */
    protected List<long[]> writeBloomFilters(List<RowGroupFlushResult.ColumnArtifacts> artifacts) {
        List<long[]> placements = new ArrayList<>(artifacts.size());
        for (RowGroupFlushResult.ColumnArtifacts artifact : artifacts) {
            placements.add(writeOneBloomFilter(artifact.bloomFilterBytes()));
        }
        return placements;
    }

    /**
     * Writes the column-index blob for every column artifact and returns one {@code {offset, length}} pair per column.
     * Columns without a column index get {@code {-1, -1}}.
     */
    protected List<long[]> writeColumnIndexes(List<RowGroupFlushResult.ColumnArtifacts> artifacts) {
        List<long[]> placements = new ArrayList<>(artifacts.size());
        for (RowGroupFlushResult.ColumnArtifacts artifact : artifacts) {
            if (artifact.columnIndex() == null) {
                placements.add(absentPlacement());
                continue;
            }
            placements.add(writeThriftBlob(buffer -> ParquetFormat.writeColumnIndex(buffer, artifact.columnIndex())));
        }
        return placements;
    }

    /**
     * Writes the offset-index blob for every column artifact and returns one {@code {offset, length}} pair per column.
     * Columns without an offset index get {@code {-1, -1}}.
     */
    protected List<long[]> writeOffsetIndexes(List<RowGroupFlushResult.ColumnArtifacts> artifacts) {
        List<long[]> placements = new ArrayList<>(artifacts.size());
        for (RowGroupFlushResult.ColumnArtifacts artifact : artifacts) {
            if (artifact.offsetIndex() == null) {
                placements.add(absentPlacement());
                continue;
            }
            placements.add(writeThriftBlob(buffer -> ParquetFormat.writeOffsetIndex(buffer, artifact.offsetIndex())));
        }
        return placements;
    }

    /**
     * Builds and writes the footer (schema, row groups, key-value metadata, created-by tag), the 4-byte little-endian
     * footer length, and the trailing magic. Subclasses may override to inject extra fields, but must keep the
     * footer-length and trailing-magic suffix intact.
     */
    protected void writeFooter() {
        List<SchemaElement> elements = SchemaElementWriter.flatten(schema);
        List<KeyValue> keyValueMetadata = buildKeyValueMetadata();
        FileMetaData footer = FileMetaData.builder()
                .version(1)
                .schema(elements)
                .numRows(totalRows)
                .rowGroups(completedRowGroups)
                .keyValueMetadata(keyValueMetadata)
                .createdBy(Optional.of(CREATED_BY + " version " + ParquetryVersion.version()))
                .columnOrders(Optional.of(typeDefinedColumnOrders()))
                .build();

        long[] placement = writeThriftBlob(buffer -> ParquetFormat.writeFooter(buffer, footer));
        int footerLength = Math.toIntExact(placement[1]);
        writeFooterTrailer(footerLength);
    }

    /**
     * Builds the file's key/value metadata: the caller's {@link WriteOptions#keyValueMetadata()} entries followed by
     * the writer-managed GeoParquet 1.x {@code "geo"} entry, emitted when at least one geospatial column was written.
     * Subclasses may override to add bespoke entries (e.g. authoring tool version, custom application metadata).
     */
    protected List<KeyValue> buildKeyValueMetadata() {
        List<KeyValue> entries = new ArrayList<>();
        options.keyValueMetadata().forEach((key, value) -> entries.add(new KeyValue(key, Optional.of(value))));
        Optional<String> geoJson = geoWriter.v1JsonPayload(schema, geoSummaries);
        if (geoJson.isPresent()) {
            entries.add(new KeyValue(GEO_KEY, geoJson));
        }
        return entries;
    }

    /**
     * Declares the type-defined sort order for every leaf column. This is the signal that the footer's
     * {@code min_value}/{@code max_value} statistics obey each column type's logical ordering (signed for numeric
     * types, unsigned-lexicographic for binary) rather than the legacy signed-byte comparison. Readers must disregard
     * the modern statistics fields when it is absent.
     */
    private List<ColumnOrder> typeDefinedColumnOrders() {
        int leafCount = schema.leafColumns().size();
        List<ColumnOrder> orders = new ArrayList<>(leafCount);
        for (int i = 0; i < leafCount; i++) {
            orders.add(new ColumnOrder.TypeDefined());
        }
        return orders;
    }

    /**
     * Opens a fresh per-row-group writer rooted at {@code baseFileOffset}. Subclasses may override to inject a custom
     * writer.
     */
    protected static RowGroupWriter openRowGroupWriter(
            WriteOptions options, ParquetSchema schema, Path tempDir, long baseFileOffset) {
        return new RowGroupWriter(options, schema, tempDir, baseFileOffset);
    }

    /** Throws when the writer has been closed or marked failed. Subclasses may override to add custom state checks. */
    protected void checkOpen() {
        if (closed) {
            throw new ParquetWriteException("ParquetWriter is closed");
        }
        if (failed) {
            throw new ParquetWriteException("ParquetWriter is in failed state; close it");
        }
    }

    /**
     * Throws an {@link UncheckedIOException} wrapping an {@link InterruptedIOException} when the current thread has a
     * pending interrupt, after marking the writer failed and restoring the interrupt status. Subclasses may override to
     * add cooperative cancellation hooks.
     */
    protected void checkInterrupt() {
        if (Thread.interrupted()) {
            markFailed();
            Thread.currentThread().interrupt();
            throw new UncheckedIOException(new InterruptedIOException("ParquetWriter interrupted"));
        }
    }

    /** Marks the writer as terminally failed. Subclasses may override to add their own bookkeeping. */
    protected void markFailed() {
        failed = true;
    }

    /**
     * Closes the current row group quietly and deletes the temp directory. Subclasses may override to extend cleanup.
     */
    protected void cleanupAfterFailure() {
        try {
            currentRowGroup.close();
        } catch (RuntimeException _) {
            /* best effort: failure path */
        }
        deleteTempDirQuietly(tempDir);
    }

    /** Fires the write-started event the first time a row reaches the writer. */
    private void fireWriteStartedOnce() {
        if (writeStartedFired || !observing()) {
            return;
        }
        writeStartedFired = true;
        observer.onWriteStarted(new WriteStarted(schema));
    }

    /** Fires the row-progress event at the configured cadence. Subclasses may override to add extra telemetry. */
    protected void maybeFireProgress() {
        if (!observing()) {
            return;
        }
        long cadence = options.writeObserverCadenceRows();
        while (lastProgressMilestone + cadence <= totalRows) {
            lastProgressMilestone += cadence;
            observer.onRowsWritten(lastProgressMilestone);
        }
    }

    /** Fires the row-group-flushed event. Subclasses may override to add extra telemetry. */
    protected void fireRowGroupFlushed(int rowGroupIndex, RowGroup patched, RowGroupFlushResult flushed) {
        if (!observing()) {
            return;
        }
        Duration scope = Duration.between(currentRowGroupStart, Instant.now());
        RowGroupFlushed event = new RowGroupFlushed(
                rowGroupIndex,
                patched.numRows(),
                patched.totalByteSize(),
                flushed.bytesWritten(),
                flushed.dictionaryBytes(),
                scope,
                patched.columns().size());
        observer.onRowGroupFlushed(event);
    }

    /** Fires the indexes-written event with the column-index, offset-index, and bloom-filter byte totals. */
    private void fireIndexesWritten(RowGroup patched) {
        if (!observing()) {
            return;
        }
        long columnIndexBytes = 0L;
        long offsetIndexBytes = 0L;
        long bloomFilterBytes = 0L;
        for (ColumnChunk chunk : patched.columns()) {
            columnIndexBytes += chunk.columnIndexLength().orElse(0);
            offsetIndexBytes += chunk.offsetIndexLength().orElse(0);
            bloomFilterBytes += bloomFilterLengthOf(chunk);
        }
        observer.onIndexesWritten(new IndexesWritten(columnIndexBytes, offsetIndexBytes, bloomFilterBytes));
    }

    private static long bloomFilterLengthOf(ColumnChunk chunk) {
        Optional<ColumnMetaData> metaData = chunk.metaData();
        if (metaData.isEmpty()) {
            return 0L;
        }
        return metaData.get().bloomFilterLength().orElse(0L);
    }

    /** Fires the write-finished event with the file-level totals and total elapsed writer time. */
    protected void fireOnClose() {
        if (!observing()) {
            return;
        }
        Duration wallClock = Duration.between(writerStart, Instant.now());
        WriteStats stats = new WriteStats(
                totalRows, completedRowGroups.size(), observedCompressedBytes, observedUncompressedBytes, wallClock);
        observer.onWriteFinished(stats);
    }

    private boolean observing() {
        return observer != WriteObserver.NONE;
    }

    /** Returns the schema after V2 logical-type application. Test helper; not part of the public API. */
    ParquetSchema schemaForTesting() {
        return schema;
    }

    // --- private helpers below this line ---

    /**
     * Serializes one footer / column-index / offset-index blob into a bounded byte array, then appends it to the sink
     * in a single write. These blobs are file-metadata sized; the per-blob array is acceptable where a per-row array
     * would not be. Returns the {@code {offset, length}} placement of the blob in the file.
     */
    private long[] writeThriftBlob(Consumer<ByteArrayOutputStream> serializer) {
        long offset = out.position();
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        serializer.accept(buffer);
        byte[] bytes = buffer.toByteArray();
        out.write(MemorySegment.ofArray(bytes));
        return new long[] {offset, bytes.length};
    }

    private long[] writeOneBloomFilter(MemorySegment bloomBytes) {
        if (bloomBytes == null || bloomBytes == MemorySegment.NULL || bloomBytes.byteSize() == 0L) {
            return absentPlacement();
        }
        long offset = out.position();
        BloomFilterHeader header = new BloomFilterHeader(
                (int) bloomBytes.byteSize(),
                BloomFilterHeader.Algorithm.SPLIT_BLOCK,
                BloomFilterHeader.HashStrategy.XXHASH,
                BloomFilterHeader.Compression.UNCOMPRESSED);
        byte[] bitset = bloomBytes.toArray(ValueLayout.JAVA_BYTE);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        ParquetFormat.writeBloomFilterHeader(buffer, header);
        buffer.writeBytes(bitset);
        byte[] bytes = buffer.toByteArray();
        out.write(MemorySegment.ofArray(bytes));
        return new long[] {offset, bytes.length};
    }

    private List<ColumnChunk> patchColumnChunks(
            List<ColumnChunk> sourceChunks,
            List<long[]> bloomPlacements,
            List<long[]> columnIndexPlacements,
            List<long[]> offsetIndexPlacements) {
        List<ColumnChunk> patched = new ArrayList<>(sourceChunks.size());
        for (int i = 0; i < sourceChunks.size(); i++) {
            ColumnChunk source = sourceChunks.get(i);
            ColumnMetaData patchedMeta = patchColumnMetaData(source.metaData().orElseThrow(), bloomPlacements.get(i));
            patched.add(ColumnChunk.builder()
                    .fileOffset(source.fileOffset())
                    .metaData(Optional.of(patchedMeta))
                    .columnIndexOffset(longToOptional(columnIndexPlacements.get(i)[0]))
                    .columnIndexLength(intToOptional(columnIndexPlacements.get(i)[1]))
                    .offsetIndexOffset(longToOptional(offsetIndexPlacements.get(i)[0]))
                    .offsetIndexLength(intToOptional(offsetIndexPlacements.get(i)[1]))
                    .build());
        }
        return patched;
    }

    private RowGroup buildPatchedRowGroup(RowGroup source, List<ColumnChunk> patchedColumns) {
        return RowGroup.builder()
                .columns(patchedColumns)
                .totalByteSize(source.totalByteSize())
                .numRows(source.numRows())
                .fileOffset(source.fileOffset())
                .totalCompressedSize(source.totalCompressedSize())
                .ordinal(OptionalInt.of(completedRowGroups.size()))
                .build();
    }

    private ColumnMetaData patchColumnMetaData(ColumnMetaData source, long[] bloomPlacement) {
        return ColumnMetaData.builder()
                .type(source.type())
                .encodings(source.encodings())
                .pathInSchema(source.pathInSchema())
                .codec(source.codec())
                .numValues(source.numValues())
                .totalUncompressedSize(source.totalUncompressedSize())
                .totalCompressedSize(source.totalCompressedSize())
                .keyValueMetadata(source.keyValueMetadata())
                .dataPageOffset(source.dataPageOffset())
                .indexPageOffset(source.indexPageOffset())
                .dictionaryPageOffset(source.dictionaryPageOffset())
                .statistics(source.statistics())
                .encodingStats(source.encodingStats())
                .bloomFilterOffset(longToOptional(bloomPlacement[0]))
                .bloomFilterLength(longToOptional(bloomPlacement[1]))
                .sizeStatistics(source.sizeStatistics())
                .geospatialStatistics(source.geospatialStatistics())
                .build();
    }

    private void accumulateGeoSummaries(RowGroupFlushResult flushed) {
        for (RowGroupFlushResult.ColumnArtifacts artifact : flushed.columnArtifacts()) {
            GeospatialStatistics stats = artifact.geospatialStatistics();
            if (stats == null) {
                continue;
            }
            GeoColumnSummary previous = geoSummaries.get(artifact.columnPath());
            geoSummaries.put(artifact.columnPath(), mergeGeoSummary(previous, stats));
        }
    }

    private void writeFooterTrailer(int footerLength) {
        // The footer length is a 4-byte little-endian int per the parquet-format trailer layout.
        byte[] trailer = new byte[4 + MAGIC.length];
        trailer[0] = (byte) (footerLength & 0xff);
        trailer[1] = (byte) ((footerLength >>> 8) & 0xff);
        trailer[2] = (byte) ((footerLength >>> 16) & 0xff);
        trailer[3] = (byte) ((footerLength >>> 24) & 0xff);
        System.arraycopy(MAGIC, 0, trailer, 4, MAGIC.length);
        out.write(MemorySegment.ofArray(trailer));
    }

    // --- static helpers below this line ---

    /**
     * Creates a private working directory inside the caller-supplied {@link WriteOptions#tempDir()}.
     *
     * <p>The caller's {@code tempDir} (typically {@code $java.io.tmpdir}) must already exist, be a directory, and be
     * writable; the writer never modifies its permissions or contents beyond the per-run subdirectory it creates here.
     * The subdirectory is named with a random suffix to avoid collisions between concurrent writers, and on POSIX
     * filesystems is created atomically with owner-only permissions (rwx------) so other users on the same machine
     * cannot read intermediate column-chunk bytes. On non-POSIX filesystems the underlying platform's per-user temp
     * conventions apply.
     *
     * <p>The directory is deleted when the writer closes (success or failure path).
     */
    private static Path createTempDir(WriteOptions options) {
        Path parent = options.tempDir();
        if (!Files.exists(parent)) {
            throw new ParquetWriteException("WriteOptions.tempDir does not exist: " + parent);
        }
        if (!Files.isDirectory(parent)) {
            throw new ParquetWriteException("WriteOptions.tempDir is not a directory: " + parent);
        }
        if (!Files.isWritable(parent)) {
            throw new ParquetWriteException("WriteOptions.tempDir is not writable: " + parent);
        }
        try {
            return Files.createTempDirectory(parent, "parquetry-write-", ownerOnlyAttributes(parent.getFileSystem()));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create writer temp directory under " + parent, e);
        }
    }

    private static FileAttribute<?>[] ownerOnlyAttributes(FileSystem fs) {
        if (!fs.supportedFileAttributeViews().contains("posix")) {
            return new FileAttribute<?>[0];
        }
        EnumSet<PosixFilePermission> ownerRwx = EnumSet.of(
                PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE);
        return new FileAttribute<?>[] {PosixFilePermissions.asFileAttribute(ownerRwx)};
    }

    private static void writeLeadingMagic(ByteSink sink, Path tempDir) {
        try {
            sink.write(MemorySegment.ofArray(MAGIC));
        } catch (RuntimeException e) {
            deleteTempDirQuietly(tempDir);
            throw e;
        }
    }

    private static long[] absentPlacement() {
        return new long[] {-1L, -1L};
    }

    private static GeoColumnSummary mergeGeoSummary(GeoColumnSummary previous, GeospatialStatistics chunk) {

        Optional<BoundingBox> mergedBbox = chunk.bbox();
        List<Integer> left = List.of();
        if (previous != null) {
            mergedBbox = unionBbox(previous.bbox(), chunk.bbox());
            left = previous.geometryTypeCodes();
        }
        List<Integer> right = chunk.geospatialTypes().orElse(List.of());
        List<Integer> mergedTypes = unionTypeCodes(left, right);
        return GeoColumnSummary.wkb(mergedBbox, mergedTypes);
    }

    private static Optional<BoundingBox> unionBbox(Optional<BoundingBox> left, Optional<BoundingBox> right) {
        if (left.isEmpty()) {
            return right;
        }
        if (right.isEmpty()) {
            return left;
        }
        BoundingBox a = left.get();
        BoundingBox b = right.get();
        return Optional.of(BoundingBox.builder()
                .xmin(Math.min(a.xmin(), b.xmin()))
                .xmax(Math.max(a.xmax(), b.xmax()))
                .ymin(Math.min(a.ymin(), b.ymin()))
                .ymax(Math.max(a.ymax(), b.ymax()))
                .zmin(unionMin(a.zmin(), b.zmin()))
                .zmax(unionMax(a.zmax(), b.zmax()))
                .mmin(unionMin(a.mmin(), b.mmin()))
                .mmax(unionMax(a.mmax(), b.mmax()))
                .build());
    }

    private static OptionalDouble unionMin(OptionalDouble left, OptionalDouble right) {
        if (left.isEmpty()) {
            return right;
        }
        if (right.isEmpty()) {
            return left;
        }
        return OptionalDouble.of(Math.min(left.getAsDouble(), right.getAsDouble()));
    }

    private static OptionalDouble unionMax(OptionalDouble left, OptionalDouble right) {
        if (left.isEmpty()) {
            return right;
        }
        if (right.isEmpty()) {
            return left;
        }
        return OptionalDouble.of(Math.max(left.getAsDouble(), right.getAsDouble()));
    }

    private static List<Integer> unionTypeCodes(List<Integer> left, List<Integer> right) {
        return Stream.concat(left.stream(), right.stream())
                .distinct()
                .sorted(Comparator.naturalOrder())
                .toList();
    }

    private static OptionalLong longToOptional(long value) {
        if (value < 0L) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(value);
    }

    private static OptionalInt intToOptional(long value) {
        if (value < 0L) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(Math.toIntExact(value));
    }

    private static void deleteTempDirQuietly(Path dir) {
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException _) {
                    /* best effort */
                }
            });
        } catch (IOException _) {
            /* best effort */
        }
    }
}
