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
package io.tileverse.parquetry.data.write;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.BloomFilterConfig;
import io.tileverse.parquetry.data.WriteOptions.EncodingPolicy;
import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.data.write.page.DictionaryAttempt;
import io.tileverse.parquetry.data.write.page.DictionaryAttemptEncoder;
import io.tileverse.parquetry.data.write.page.EncodedPage;
import io.tileverse.parquetry.data.write.page.Encoder;
import io.tileverse.parquetry.data.write.page.PageEncodeJob;
import io.tileverse.parquetry.data.write.page.PageStatistics;
import io.tileverse.parquetry.data.write.page.PageWriter;
import io.tileverse.parquetry.data.write.page.PlainBinaryEncoder;
import io.tileverse.parquetry.data.write.page.PlainBooleanEncoder;
import io.tileverse.parquetry.data.write.page.PlainDoubleEncoder;
import io.tileverse.parquetry.data.write.page.PlainFixedLenBinaryEncoder;
import io.tileverse.parquetry.data.write.page.PlainFloatEncoder;
import io.tileverse.parquetry.data.write.page.PlainInt32Encoder;
import io.tileverse.parquetry.data.write.page.PlainInt64Encoder;
import io.tileverse.parquetry.data.write.page.PlainInt96Encoder;
import io.tileverse.parquetry.data.write.page.PreEncodedPageJob;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.GeospatialStatistics;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

import lombok.NonNull;

/**
 * Per-column accumulator that drives {@link PageWriter} against a temp file.
 *
 * <p>Callers issue typed {@code appendXxx} calls until the page byte- or value-threshold trips; the writer then encodes
 * a data page through {@link PageWriter}. When dictionary encoding is enabled for the column, the writer drives a
 * {@link DictionaryAttemptEncoder}: indices accumulate in a column-chunk dictionary until either the chunk closes or
 * the dictionary outgrows {@link WriteOptions#dictionaryByteLimit()}. On overflow, subsequent pages fall back to
 * {@code PLAIN}. Pages are buffered in memory while dictionary encoding is active so the dictionary page can be written
 * first at chunk finish; non-dictionary columns stream their pages directly into the temp file. {@link #finishChunk()}
 * flushes any partial page, writes the dictionary page (when present) followed by the buffered data pages, materializes
 * the column-chunk statistics, and returns a {@link ColumnChunkResult} carrying everything the row-group writer needs
 * to consolidate the temp file into the final output.
 *
 * <p>Instances are single-threaded: the row-group writer dedicates one worker thread per column chunk. Concurrent calls
 * from multiple threads on the same instance are not supported.
 */
public final class ColumnChunkWriter implements AutoCloseable {

    private static final String FIXED_LEN_TYPE_LENGTH_REQUIRED_MESSAGE =
            "FIXED_LEN_BYTE_ARRAY column missing type_length: ";

    private final WriteOptions options;
    private final SchemaNode.Primitive leaf;
    private final Path tempFile;
    private final FileChannel tempFileChannel;

    private final PrimitiveKind kind;
    private final ColumnContext column;
    private final PageWriter pageWriter;
    private final StatisticsAccumulator chunkStats;
    private final StatisticsAccumulator pageStats;
    private final GeospatialStatisticsAccumulator geoStats;

    private final ColumnIndexBuilder columnIndexBuilder;
    private final OffsetIndexBuilder offsetIndexBuilder;
    private final BloomFilterBuilder bloomFilter;

    private final ValueBuffer valueBuffer;
    private final LevelBuffer repLevelBuffer;
    private final LevelBuffer defLevelBuffer;

    private final LinkedHashSet<Encoding> usedEncodings = new LinkedHashSet<>();

    /**
     * Dictionary attempt encoder, when the column's encoding policy permits dictionary encoding and the primitive kind
     * supports it. {@code null} for forced-plain columns or kinds that bypass dictionary attempt.
     */
    private final DictionaryAttempt<?, ?> dictionary;

    /**
     * Buffers each data page's bytes in memory while {@link #dictionary} is active. The dictionary page is unknown
     * until {@link #finishChunk()} (its byte size depends on how many unique values landed in the column), so data
     * pages cannot stream directly to the temp file: they would either lock in a placeholder header whose
     * compact-protocol byte length cannot be patched in place, or force a final shuffle. Buffering bounds memory by the
     * chunk's compressed size and avoids both pitfalls.
     */
    private final List<byte[]> bufferedPages;

    private long uncompressedBytes;
    private long compressedBytes;
    private long firstDataPageOffset = -1L;
    private long dictionaryPageOffset = -1L;
    private long totalCells;
    private long rowsInChunk;
    private long rowsInCurrentPage;
    private long rowsBeforeCurrentPage;
    private int pageCellCount;
    private int pageNullCount;
    private long pageByteEstimate;
    private boolean finished;
    private boolean closed;

    public ColumnChunkWriter(@NonNull WriteOptions options, @NonNull SchemaNode.Primitive leaf, @NonNull Path tempFile)
            throws IOException {
        this.options = options;
        this.leaf = leaf;
        this.tempFile = tempFile;
        this.kind = leaf.kind();

        this.tempFileChannel = FileChannel.open(
                tempFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        Compression compression = resolveCompression(options, leaf.name());
        this.column = new ColumnContext(
                maxRepetitionLevelFor(leaf), maxDefinitionLevelFor(leaf), kind, options.parquetVersion(), compression);
        this.pageWriter = new PageWriter(column);

        LogicalType logicalType = leaf.logicalType().orElse(null);
        this.chunkStats = StatisticsAccumulator.forKind(kind, logicalType);
        this.pageStats = StatisticsAccumulator.forKind(kind, logicalType);
        this.geoStats = isGeometryLike(logicalType) ? new GeospatialStatisticsAccumulator() : null;

        boolean useIndexes = options.parquetVersion() == ParquetVersion.V2_0;
        this.columnIndexBuilder = useIndexes ? new ColumnIndexBuilder(kind, logicalType) : null;
        this.offsetIndexBuilder = useIndexes ? new OffsetIndexBuilder() : null;

        this.bloomFilter = createBloomFilterIfRequested(options, leaf.name());

        this.valueBuffer = ValueBuffer.forKind(kind);
        this.repLevelBuffer = column.maxRepetitionLevel() > 0 ? new LevelBuffer() : null;
        this.defLevelBuffer = column.maxDefinitionLevel() > 0 ? new LevelBuffer() : null;

        this.dictionary = createDictionaryAttempt(options, leaf, kind);
        this.bufferedPages = dictionary != null ? new ArrayList<>() : null;
    }

    /** Append one INT32 cell. */
    public void appendInt(int value, int repLevel, int defLevel) {
        ensureKind(PrimitiveKind.INT32);
        recordLevels(repLevel, defLevel);
        valueBuffer.addInt(value);
        chunkStats.update(value, false);
        pageStats.update(value, false);
        addToBloom(value);
        appendToDictionaryIfActive(value);
        pageByteEstimate += Integer.BYTES;
        pageCellCount++;
        totalCells++;
        maybeFlushPage();
    }

    /** Append one INT64 cell. */
    public void appendLong(long value, int repLevel, int defLevel) {
        ensureKind(PrimitiveKind.INT64);
        recordLevels(repLevel, defLevel);
        valueBuffer.addLong(value);
        chunkStats.update(value, false);
        pageStats.update(value, false);
        addToBloom(value);
        appendToDictionaryIfActive(value);
        pageByteEstimate += Long.BYTES;
        pageCellCount++;
        totalCells++;
        maybeFlushPage();
    }

    /** Append one FLOAT cell. */
    public void appendFloat(float value, int repLevel, int defLevel) {
        ensureKind(PrimitiveKind.FLOAT);
        recordLevels(repLevel, defLevel);
        valueBuffer.addFloat(value);
        chunkStats.update(value, false);
        pageStats.update(value, false);
        addToBloom(value);
        appendToDictionaryIfActive(value);
        pageByteEstimate += Float.BYTES;
        pageCellCount++;
        totalCells++;
        maybeFlushPage();
    }

    /** Append one DOUBLE cell. */
    public void appendDouble(double value, int repLevel, int defLevel) {
        ensureKind(PrimitiveKind.DOUBLE);
        recordLevels(repLevel, defLevel);
        valueBuffer.addDouble(value);
        chunkStats.update(value, false);
        pageStats.update(value, false);
        addToBloom(value);
        appendToDictionaryIfActive(value);
        pageByteEstimate += Double.BYTES;
        pageCellCount++;
        totalCells++;
        maybeFlushPage();
    }

    /** Append one BOOLEAN cell. */
    public void appendBoolean(boolean value, int repLevel, int defLevel) {
        ensureKind(PrimitiveKind.BOOLEAN);
        recordLevels(repLevel, defLevel);
        valueBuffer.addBoolean(value);
        chunkStats.update(value, false);
        pageStats.update(value, false);
        addToBloom(value);
        pageByteEstimate += 1L;
        pageCellCount++;
        totalCells++;
        maybeFlushPage();
    }

    /** Append one BYTE_ARRAY cell. */
    public void appendBinary(@NonNull MemorySegment value, int repLevel, int defLevel) {
        ensureKind(PrimitiveKind.BYTE_ARRAY);
        recordLevels(repLevel, defLevel);
        byte[] bytes = value.toArray(ValueLayout.JAVA_BYTE);
        valueBuffer.addBinary(bytes);
        MemorySegment readOnly = MemorySegment.ofArray(bytes).asReadOnly();
        chunkStats.update(readOnly, false);
        pageStats.update(readOnly, false);
        addToBloom(readOnly);
        if (geoStats != null) {
            geoStats.update(readOnly);
        }
        appendToDictionaryIfActive(bytes);
        pageByteEstimate += Integer.BYTES + (long) bytes.length;
        pageCellCount++;
        totalCells++;
        maybeFlushPage();
    }

    /** Append one FIXED_LEN_BYTE_ARRAY cell. */
    public void appendFixedLenBinary(@NonNull MemorySegment value, int repLevel, int defLevel) {
        ensureKind(PrimitiveKind.FIXED_LEN_BYTE_ARRAY);
        int expected = leaf.typeLength()
                .orElseThrow(() -> new ParquetWriteException(FIXED_LEN_TYPE_LENGTH_REQUIRED_MESSAGE + leaf.name()));
        if (value.byteSize() != expected) {
            throw new ParquetWriteException("FIXED_LEN_BYTE_ARRAY value byte size " + value.byteSize()
                    + " does not match column type_length " + expected);
        }
        recordLevels(repLevel, defLevel);
        byte[] bytes = value.toArray(ValueLayout.JAVA_BYTE);
        valueBuffer.addBinary(bytes);
        MemorySegment readOnly = MemorySegment.ofArray(bytes).asReadOnly();
        chunkStats.update(readOnly, false);
        pageStats.update(readOnly, false);
        addToBloom(readOnly);
        appendToDictionaryIfActive(bytes);
        pageByteEstimate += bytes.length;
        pageCellCount++;
        totalCells++;
        maybeFlushPage();
    }

    /** Append one INT96 cell, encoded as a (julianDay, timestamp-nanos) pair per the legacy INT96 layout. */
    public void appendInt96(long timestampNanos, int julianDay, int repLevel, int defLevel) {
        ensureKind(PrimitiveKind.INT96);
        recordLevels(repLevel, defLevel);
        byte[] packed = packInt96(timestampNanos, julianDay);
        valueBuffer.addInt96Bytes(packed);
        chunkStats.update(null, false);
        pageStats.update(null, false);
        appendToDictionaryIfActive(packed);
        pageByteEstimate += 12L;
        pageCellCount++;
        totalCells++;
        maybeFlushPage();
    }

    /**
     * Appends every cell of {@code vector} to this column chunk, walking the vector's validity bitset and dispatching
     * to the matching typed {@code appendXxx} for non-null cells. Nulls go through {@link #appendNull}; the caller is
     * responsible for matching the vector's kind to this column's primitive kind, which {@code ensureKind} re-checks on
     * every non-null cell.
     */
    public void appendVector(@NonNull SchemaNode.Primitive leaf, @NonNull ColumnVector vector) {
        int defLevelForValue = leaf.repetition() == Repetition.REQUIRED ? 0 : 1;
        java.util.BitSet validity = vector.validity();
        int size = vector.size();
        switch (vector) {
            case io.tileverse.parquetry.batch.IntVector iv ->
                appendIntVectorCells(iv, validity, size, defLevelForValue);
            case io.tileverse.parquetry.batch.LongVector lv ->
                appendLongVectorCells(lv, validity, size, defLevelForValue);
            case io.tileverse.parquetry.batch.FloatVector fv ->
                appendFloatVectorCells(fv, validity, size, defLevelForValue);
            case io.tileverse.parquetry.batch.DoubleVector dv ->
                appendDoubleVectorCells(dv, validity, size, defLevelForValue);
            case io.tileverse.parquetry.batch.BooleanVector bv ->
                appendBooleanVectorCells(bv, validity, size, defLevelForValue);
            case io.tileverse.parquetry.batch.BinaryVector binv ->
                appendBinaryVectorCells(binv, validity, size, defLevelForValue);
            case io.tileverse.parquetry.batch.FixedLenBinaryVector flbv ->
                appendFixedLenBinaryVectorCells(flbv, validity, size, defLevelForValue);
            case io.tileverse.parquetry.batch.Int96Vector _,
                    io.tileverse.parquetry.batch.ListVector _,
                    io.tileverse.parquetry.batch.MapVector _,
                    io.tileverse.parquetry.batch.StructVector _ ->
                throw new ParquetWriteException(
                        "appendVector: column " + leaf.name() + " uses unsupported vector kind " + vector.getClass());
        }
    }

    private void appendIntVectorCells(
            io.tileverse.parquetry.batch.IntVector vector, java.util.BitSet validity, int size, int defLevelForValue) {
        int[] values = vector.asArray();
        for (int i = 0; i < size; i++) {
            if (validity.get(i)) {
                appendInt(values[i], 0, defLevelForValue);
            } else {
                appendNull(0, 0);
            }
        }
    }

    private void appendLongVectorCells(
            io.tileverse.parquetry.batch.LongVector vector, java.util.BitSet validity, int size, int defLevelForValue) {
        long[] values = vector.asArray();
        for (int i = 0; i < size; i++) {
            if (validity.get(i)) {
                appendLong(values[i], 0, defLevelForValue);
            } else {
                appendNull(0, 0);
            }
        }
    }

    private void appendFloatVectorCells(
            io.tileverse.parquetry.batch.FloatVector vector,
            java.util.BitSet validity,
            int size,
            int defLevelForValue) {
        float[] values = vector.asArray();
        for (int i = 0; i < size; i++) {
            if (validity.get(i)) {
                appendFloat(values[i], 0, defLevelForValue);
            } else {
                appendNull(0, 0);
            }
        }
    }

    private void appendDoubleVectorCells(
            io.tileverse.parquetry.batch.DoubleVector vector,
            java.util.BitSet validity,
            int size,
            int defLevelForValue) {
        double[] values = vector.asArray();
        for (int i = 0; i < size; i++) {
            if (validity.get(i)) {
                appendDouble(values[i], 0, defLevelForValue);
            } else {
                appendNull(0, 0);
            }
        }
    }

    private void appendBooleanVectorCells(
            io.tileverse.parquetry.batch.BooleanVector vector,
            java.util.BitSet validity,
            int size,
            int defLevelForValue) {
        boolean[] values = vector.asArray();
        for (int i = 0; i < size; i++) {
            if (validity.get(i)) {
                appendBoolean(values[i], 0, defLevelForValue);
            } else {
                appendNull(0, 0);
            }
        }
    }

    private void appendBinaryVectorCells(
            io.tileverse.parquetry.batch.BinaryVector vector,
            java.util.BitSet validity,
            int size,
            int defLevelForValue) {
        MemorySegment[] values = vector.asArray();
        for (int i = 0; i < size; i++) {
            if (validity.get(i)) {
                MemorySegment cell = values[i];
                appendBinary(cell.isReadOnly() ? cell : cell.asReadOnly(), 0, defLevelForValue);
            } else {
                appendNull(0, 0);
            }
        }
    }

    private void appendFixedLenBinaryVectorCells(
            io.tileverse.parquetry.batch.FixedLenBinaryVector vector,
            java.util.BitSet validity,
            int size,
            int defLevelForValue) {
        MemorySegment[] values = vector.asArray();
        for (int i = 0; i < size; i++) {
            if (validity.get(i)) {
                MemorySegment cell = values[i];
                appendFixedLenBinary(cell.isReadOnly() ? cell : cell.asReadOnly(), 0, defLevelForValue);
            } else {
                appendNull(0, 0);
            }
        }
    }

    /** Append a single null cell at the given repetition / definition levels. */
    public void appendNull(int repLevel, int defLevel) {
        if (column.maxDefinitionLevel() == 0) {
            throw new ParquetWriteException("Cannot append null to a REQUIRED column: " + leaf.name());
        }
        if (defLevel >= column.maxDefinitionLevel()) {
            throw new ParquetWriteException(
                    "appendNull defLevel " + defLevel + " must be < maxDefLevel " + column.maxDefinitionLevel());
        }
        recordLevels(repLevel, defLevel);
        chunkStats.update(null, true);
        pageStats.update(null, true);
        pageByteEstimate += 1L;
        pageCellCount++;
        pageNullCount++;
        totalCells++;
        maybeFlushPage();
    }

    /**
     * Flush any partial page, write the dictionary page if applicable, and return the assembled result. After this call
     * the writer is sealed and {@link #close()} only releases the temp file channel.
     */
    public ColumnChunkResult finishChunk() throws IOException {
        if (finished) {
            throw new ParquetWriteException("finishChunk already called on " + leaf.name());
        }
        finished = true;

        if (pageCellCount > 0) {
            flushPage();
        }

        if (dictionary != null) {
            emitDictionaryAndBufferedPages();
        }

        ColumnIndex columnIndex = columnIndexBuilder != null ? columnIndexBuilder.finishChunk() : null;
        OffsetIndex offsetIndex = offsetIndexBuilder != null ? offsetIndexBuilder.finishChunk() : null;
        Statistics statistics = chunkStats.finishChunk();
        GeospatialStatistics geospatialStatistics = geoStats != null ? geoStats.finish() : null;

        tempFileChannel.force(true);

        MemorySegment bloomBytes = bloomFilter != null ? captureBloomBytes(bloomFilter) : MemorySegment.NULL;

        return new ColumnChunkResult(
                tempFile,
                uncompressedBytes,
                compressedBytes,
                firstDataPageOffset,
                dictionaryPageOffset,
                totalCells,
                0L,
                List.copyOf(usedEncodings),
                statistics,
                columnIndex,
                offsetIndex,
                bloomBytes,
                geospatialStatistics);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (!finished && pageCellCount > 0) {
                throw new ParquetWriteException(
                        "ColumnChunkWriter closed without finishChunk; pending values would be lost");
            }
        } finally {
            if (tempFileChannel.isOpen()) {
                tempFileChannel.close();
            }
        }
    }

    /**
     * Running uncompressed-byte tally: the bytes already flushed to data pages plus the rough size of the values
     * currently buffered for the next page. Used by the dataset-level sizing policy.
     */
    public long estimatedUncompressedBytes() {
        return uncompressedBytes + pageByteEstimate;
    }

    private void maybeFlushPage() {
        if (pageCellCount >= options.pageValueLimit() || pageByteEstimate >= options.pageByteLimit()) {
            try {
                flushPage();
            } catch (IOException e) {
                throw new ParquetWriteException("Failed to flush page for column " + leaf.name(), e);
            }
        }
    }

    private void flushPage() throws IOException {
        PageStatistics snapshot = pageStats.finishPage();
        int[] repLevels = repLevelBuffer != null ? repLevelBuffer.snapshot(pageCellCount) : null;
        int[] defLevels = defLevelBuffer != null ? defLevelBuffer.snapshot(pageCellCount) : null;
        int rowCount = (int) pageRowCount();

        EncodedPage encoded;
        long pageStartOffset;
        if (dictionary != null) {
            ByteArrayOutputStream pageBuffer = new ByteArrayOutputStream();
            WritableByteChannel pageBufferChannel = Channels.newChannel(pageBuffer);
            encoded = encodeDictionaryDataPage(snapshot, repLevels, defLevels, rowCount, pageBufferChannel);
            byte[] pageBytes = pageBuffer.toByteArray();
            pageStartOffset = bufferedDataBytes();
            bufferedPages.add(pageBytes);
        } else {
            pageStartOffset = tempFileChannel.position();
            encoded = encodePlainDataPage(snapshot, repLevels, defLevels, rowCount, tempFileChannel);
        }

        if (firstDataPageOffset < 0L) {
            firstDataPageOffset = pageStartOffset;
        }

        int totalPageBytes = encoded.totalBytes();
        uncompressedBytes += (long) encoded.headerBytes() + encoded.pageHeader().uncompressedPageSize();
        compressedBytes += totalPageBytes;
        usedEncodings.add(encoded.pageHeader()
                .dataPageHeaderV2()
                .map(h -> h.encoding())
                .orElseGet(() ->
                        encoded.pageHeader().dataPageHeader().orElseThrow().encoding()));

        if (offsetIndexBuilder != null) {
            offsetIndexBuilder.appendPage(pageStartOffset, totalPageBytes, rowsBeforeCurrentPage);
        }
        if (columnIndexBuilder != null) {
            columnIndexBuilder.appendPage(snapshot);
        }

        rowsInChunk += rowCount;
        rowsBeforeCurrentPage = rowsInChunk;
        pageStats.reset();
        valueBuffer.clear();
        if (repLevelBuffer != null) {
            repLevelBuffer.clear();
        }
        if (defLevelBuffer != null) {
            defLevelBuffer.clear();
        }
        pageCellCount = 0;
        pageNullCount = 0;
        pageByteEstimate = 0L;
        rowsInCurrentPage = 0L;
    }

    private EncodedPage encodePlainDataPage(
            PageStatistics snapshot, int[] repLevels, int[] defLevels, int rowCount, WritableByteChannel dst)
            throws IOException {
        Encoder<?> encoder = plainEncoderForKind();
        Object payloadValues = valueBuffer.payloadValues(pageCellCount - pageNullCount);
        PageEncodeJob job = new PageEncodeJob(
                payloadValues, pageCellCount, pageNullCount, rowCount, repLevels, defLevels, encoder, snapshot);
        if (options.parquetVersion() == ParquetVersion.V2_0) {
            return pageWriter.writeDataPageV2(job, dst);
        }
        return pageWriter.writeDataPageV1(job, dst);
    }

    private EncodedPage encodeDictionaryDataPage(
            PageStatistics snapshot, int[] repLevels, int[] defLevels, int rowCount, WritableByteChannel dst)
            throws IOException {
        ByteArrayOutputStream valueBytesBuffer = new ByteArrayOutputStream();
        WritableByteChannel valueBytesChannel = Channels.newChannel(valueBytesBuffer);
        DictionaryAttemptEncoder.PageResult pageResult = dictionary.encoder().flushPage(valueBytesChannel);
        MemorySegment encodedValues =
                MemorySegment.ofArray(valueBytesBuffer.toByteArray()).asReadOnly();
        Encoding marker =
                options.parquetVersion() == ParquetVersion.V2_0 ? pageResult.v2Encoding() : pageResult.v1Encoding();
        PreEncodedPageJob job = new PreEncodedPageJob(
                encodedValues, marker, pageCellCount, pageNullCount, rowCount, repLevels, defLevels, snapshot);
        if (options.parquetVersion() == ParquetVersion.V2_0) {
            return pageWriter.writeDataPageV2PreEncoded(job, dst);
        }
        return pageWriter.writeDataPageV1PreEncoded(job, dst);
    }

    private long bufferedDataBytes() {
        long total = 0L;
        for (byte[] page : bufferedPages) {
            total += page.length;
        }
        return total;
    }

    private void emitDictionaryAndBufferedPages() throws IOException {
        long dictBytes = 0L;
        // Write the dictionary page whenever any buffered page was emitted as RLE_DICTIONARY, even if a later page
        // overflowed to PLAIN: those early pages index into the (now frozen) dictionary and are unreadable without it.
        if (dictionary.encoder().emittedDictionaryPage()) {
            dictBytes = writeDictionaryPage();
        }
        for (byte[] page : bufferedPages) {
            writeAllToTempFile(ByteBuffer.wrap(page));
        }
        bufferedPages.clear();
        if (dictBytes > 0L) {
            firstDataPageOffset += dictBytes;
            shiftOffsetIndexEntries(dictBytes);
        }
    }

    private long writeDictionaryPage() throws IOException {
        long pos = tempFileChannel.position();
        dictionaryPageOffset = pos;
        EncodedPage encoded = dictionary.writeDictionaryPage(pageWriter, tempFileChannel);
        uncompressedBytes += (long) encoded.headerBytes() + encoded.pageHeader().uncompressedPageSize();
        compressedBytes += encoded.totalBytes();
        usedEncodings.add(
                encoded.pageHeader().dictionaryPageHeader().orElseThrow().encoding());
        return encoded.totalBytes();
    }

    private void writeAllToTempFile(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            tempFileChannel.write(buffer);
        }
    }

    private void shiftOffsetIndexEntries(long delta) {
        if (offsetIndexBuilder == null) {
            return;
        }
        List<PageLocation> shifted =
                new ArrayList<>(offsetIndexBuilder.pageLocations().size());
        for (PageLocation loc : offsetIndexBuilder.pageLocations()) {
            shifted.add(new PageLocation(loc.offset() + delta, loc.compressedPageSize(), loc.firstRowIndex()));
        }
        offsetIndexBuilder.replaceAll(shifted);
    }

    private long pageRowCount() {
        if (column.maxRepetitionLevel() == 0) {
            return pageCellCount;
        }
        return rowsInCurrentPage;
    }

    private Encoder<?> plainEncoderForKind() {
        return switch (kind) {
            case BOOLEAN -> new PlainBooleanEncoder();
            case INT32 -> new PlainInt32Encoder();
            case INT64 -> new PlainInt64Encoder();
            case INT96 -> new PlainInt96Encoder();
            case FLOAT -> new PlainFloatEncoder();
            case DOUBLE -> new PlainDoubleEncoder();
            case BYTE_ARRAY -> new PlainBinaryEncoder();
            case FIXED_LEN_BYTE_ARRAY ->
                new PlainFixedLenBinaryEncoder(leaf.typeLength()
                        .orElseThrow(
                                () -> new ParquetWriteException(FIXED_LEN_TYPE_LENGTH_REQUIRED_MESSAGE + leaf.name())));
        };
    }

    private void ensureKind(PrimitiveKind expected) {
        if (kind != expected) {
            throw new ParquetWriteException(
                    "Type mismatch on column " + leaf.name() + ": expected " + expected + " but got " + kind);
        }
    }

    private void recordLevels(int repLevel, int defLevel) {
        validateLevel("repetition", repLevel, column.maxRepetitionLevel());
        validateLevel("definition", defLevel, column.maxDefinitionLevel());
        if (repLevelBuffer != null) {
            repLevelBuffer.add(repLevel);
        }
        if (defLevelBuffer != null) {
            defLevelBuffer.add(defLevel);
        }
        if (column.maxRepetitionLevel() > 0 && repLevel == 0) {
            rowsInCurrentPage++;
        }
    }

    private void validateLevel(String kind, int level, int maxLevel) {
        if (level < 0 || level > maxLevel) {
            throw new ParquetWriteException(
                    kind + " level " + level + " out of range [0, " + maxLevel + "] for column " + leaf.name());
        }
    }

    private void addToBloom(int value) {
        if (bloomFilter != null) {
            bloomFilter.add(value);
        }
    }

    private void addToBloom(long value) {
        if (bloomFilter != null) {
            bloomFilter.add(value);
        }
    }

    private void addToBloom(float value) {
        if (bloomFilter != null) {
            bloomFilter.add(value);
        }
    }

    private void addToBloom(double value) {
        if (bloomFilter != null) {
            bloomFilter.add(value);
        }
    }

    private void addToBloom(boolean value) {
        if (bloomFilter != null) {
            bloomFilter.add(value);
        }
    }

    private void addToBloom(MemorySegment value) {
        if (bloomFilter != null) {
            bloomFilter.add(value);
        }
    }

    private void appendToDictionaryIfActive(int value) {
        if (dictionary instanceof DictionaryAttempt.IntAttempt(DictionaryAttemptEncoder<Integer, int[]> encoder)) {
            encoder.appendValue(value);
        }
    }

    private void appendToDictionaryIfActive(long value) {
        if (dictionary instanceof DictionaryAttempt.LongAttempt(DictionaryAttemptEncoder<Long, long[]> encoder)) {
            encoder.appendValue(value);
        }
    }

    private void appendToDictionaryIfActive(float value) {
        if (dictionary instanceof DictionaryAttempt.FloatAttempt(DictionaryAttemptEncoder<Float, float[]> encoder)) {
            encoder.appendValue(value);
        }
    }

    private void appendToDictionaryIfActive(double value) {
        if (dictionary instanceof DictionaryAttempt.DoubleAttempt(DictionaryAttemptEncoder<Double, double[]> encoder)) {
            encoder.appendValue(value);
        }
    }

    // S7475: Palantir formatter does not accept the bare `_` unnamed pattern; keep the typed unnamed binding.
    @SuppressWarnings("java:S7475")
    private void appendToDictionaryIfActive(byte[] bytes) {
        if (dictionary
                instanceof
                DictionaryAttempt.BinaryAttempt(
                        DictionaryAttemptEncoder<ByteBuffer, byte[][]> encoder,
                        Encoder<byte[][]> _)) {
            encoder.appendValue(ByteBuffer.wrap(bytes));
        }
    }

    private static int maxRepetitionLevelFor(SchemaNode.Primitive leaf) {
        return leaf.repetition() == Repetition.REPEATED ? 1 : 0;
    }

    private static int maxDefinitionLevelFor(SchemaNode.Primitive leaf) {
        return leaf.repetition() == Repetition.REQUIRED ? 0 : 1;
    }

    private static Compression resolveCompression(WriteOptions options, String columnName) {
        Compression override = options.compression().get(columnName);
        if (override != null) {
            return override;
        }
        return options.defaultCompression();
    }

    private static BloomFilterBuilder createBloomFilterIfRequested(WriteOptions options, String columnName) {
        BloomFilterConfig explicit = options.bloomFilters().get(columnName);
        if (explicit != null) {
            return new BloomFilterBuilder(explicit);
        }
        if (options.indexedColumns().contains(columnName)) {
            return new BloomFilterBuilder(BloomFilterConfig.defaults());
        }
        return null;
    }

    private static boolean isGeometryLike(LogicalType logicalType) {
        return logicalType instanceof LogicalType.Geometry || logicalType instanceof LogicalType.Geography;
    }

    private static DictionaryAttempt<?, ?> createDictionaryAttempt(
            WriteOptions options, SchemaNode.Primitive leaf, PrimitiveKind kind) {
        EncodingPolicy policy = options.encodingPolicies().getOrDefault(leaf.name(), EncodingPolicy.AUTO);
        if (!dictionaryAttemptAllowed(policy, kind)) {
            return null;
        }
        long byteLimit = options.dictionaryByteLimit();
        return switch (kind) {
            case INT32 -> DictionaryAttempt.IntAttempt.create(byteLimit);
            case INT64 -> DictionaryAttempt.LongAttempt.create(byteLimit);
            case FLOAT -> DictionaryAttempt.FloatAttempt.create(byteLimit);
            case DOUBLE -> DictionaryAttempt.DoubleAttempt.create(byteLimit);
            case BYTE_ARRAY -> DictionaryAttempt.BinaryAttempt.create(byteLimit, new PlainBinaryEncoder());
            case FIXED_LEN_BYTE_ARRAY -> {
                int len = leaf.typeLength()
                        .orElseThrow(
                                () -> new ParquetWriteException(FIXED_LEN_TYPE_LENGTH_REQUIRED_MESSAGE + leaf.name()));
                yield DictionaryAttempt.BinaryAttempt.createFixedLen(byteLimit, len);
            }
            case INT96 -> DictionaryAttempt.BinaryAttempt.createFixedLen(byteLimit, 12);
            case BOOLEAN -> null;
        };
    }

    private static boolean dictionaryAttemptAllowed(EncodingPolicy policy, PrimitiveKind kind) {
        if (kind == PrimitiveKind.BOOLEAN) {
            return false;
        }
        return !(policy instanceof EncodingPolicy.ForcePlain);
    }

    private static byte[] packInt96(long timestampNanos, int julianDay) {
        byte[] packed = new byte[12];
        for (int i = 0; i < 8; i++) {
            packed[i] = (byte) (timestampNanos >>> (i * 8));
        }
        for (int i = 0; i < 4; i++) {
            packed[8 + i] = (byte) (julianDay >>> (i * 8));
        }
        return packed;
    }

    /**
     * Returns the raw SBBF bitset bytes (without the Thrift header) so the row-group writer can frame and place the
     * bloom filter against the absolute file offset. The header is regenerated by {@link BloomFilterBuilder#writeTo} at
     * consolidation time.
     */
    private static MemorySegment captureBloomBytes(BloomFilterBuilder builder) {
        java.io.ByteArrayOutputStream framed = new java.io.ByteArrayOutputStream();
        try {
            builder.writeTo(framed);
        } catch (IOException e) {
            throw new ParquetWriteException("Failed to materialize bloom filter bytes", e);
        }
        byte[] all = framed.toByteArray();
        int bitsetLen = builder.bitsetByteSize();
        int headerLen = all.length - bitsetLen;
        if (headerLen < 0) {
            throw new ParquetWriteException("Bloom filter framing too short: " + all.length);
        }
        byte[] bitset = new byte[bitsetLen];
        System.arraycopy(all, headerLen, bitset, 0, bitsetLen);
        return MemorySegment.ofArray(bitset).asReadOnly();
    }

    /**
     * Per-page level buffer. Holds an {@code int[]} that grows as cells are appended; {@link #snapshot(int)} returns a
     * defensive copy sized to the cell count, suitable for handing to {@link PageEncodeJob}.
     */
    private static final class LevelBuffer {

        private int[] levels = new int[1024];
        private int size;

        void add(int level) {
            if (size == levels.length) {
                levels = Arrays.copyOf(levels, levels.length * 2);
            }
            levels[size++] = level;
        }

        int[] snapshot(int count) {
            return Arrays.copyOf(levels, count);
        }

        void clear() {
            size = 0;
        }
    }

    /**
     * Per-kind growable scratch buffer for non-null values. The carrier type matches the column's {@link PrimitiveKind}
     * so the per-page {@link Encoder} can consume it directly.
     */
    private abstract static class ValueBuffer {

        static ValueBuffer forKind(PrimitiveKind kind) {
            return switch (kind) {
                case BOOLEAN -> new BooleanBuffer();
                case INT32 -> new IntBuffer();
                case INT64 -> new LongBuffer();
                case INT96 -> new Int96Buffer();
                case FLOAT -> new FloatBuffer();
                case DOUBLE -> new DoubleBuffer();
                case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> new BinaryBuffer();
            };
        }

        abstract Object payloadValues(int nonNullCount);

        abstract void clear();

        void addInt(int value) {
            throw new UnsupportedOperationException();
        }

        void addLong(long value) {
            throw new UnsupportedOperationException();
        }

        void addFloat(float value) {
            throw new UnsupportedOperationException();
        }

        void addDouble(double value) {
            throw new UnsupportedOperationException();
        }

        void addBoolean(boolean value) {
            throw new UnsupportedOperationException();
        }

        void addBinary(byte[] value) {
            throw new UnsupportedOperationException();
        }

        void addInt96Bytes(byte[] packed) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class IntBuffer extends ValueBuffer {

        private int[] values = new int[1024];
        private int size;

        @Override
        void addInt(int value) {
            ensureCapacity();
            values[size++] = value;
        }

        private void ensureCapacity() {
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
        }

        @Override
        Object payloadValues(int nonNullCount) {
            return Arrays.copyOf(values, nonNullCount);
        }

        @Override
        void clear() {
            size = 0;
        }
    }

    private static final class LongBuffer extends ValueBuffer {

        private long[] values = new long[1024];
        private int size;

        @Override
        void addLong(long value) {
            ensureCapacity();
            values[size++] = value;
        }

        private void ensureCapacity() {
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
        }

        @Override
        Object payloadValues(int nonNullCount) {
            return Arrays.copyOf(values, nonNullCount);
        }

        @Override
        void clear() {
            size = 0;
        }
    }

    private static final class FloatBuffer extends ValueBuffer {

        private float[] values = new float[1024];
        private int size;

        @Override
        void addFloat(float value) {
            ensureCapacity();
            values[size++] = value;
        }

        private void ensureCapacity() {
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
        }

        @Override
        Object payloadValues(int nonNullCount) {
            return Arrays.copyOf(values, nonNullCount);
        }

        @Override
        void clear() {
            size = 0;
        }
    }

    private static final class DoubleBuffer extends ValueBuffer {

        private double[] values = new double[1024];
        private int size;

        @Override
        void addDouble(double value) {
            ensureCapacity();
            values[size++] = value;
        }

        private void ensureCapacity() {
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
        }

        @Override
        Object payloadValues(int nonNullCount) {
            return Arrays.copyOf(values, nonNullCount);
        }

        @Override
        void clear() {
            size = 0;
        }
    }

    private static final class BooleanBuffer extends ValueBuffer {

        private boolean[] values = new boolean[1024];
        private int size;

        @Override
        void addBoolean(boolean value) {
            ensureCapacity();
            values[size++] = value;
        }

        private void ensureCapacity() {
            if (size == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
        }

        @Override
        Object payloadValues(int nonNullCount) {
            return Arrays.copyOf(values, nonNullCount);
        }

        @Override
        void clear() {
            size = 0;
        }
    }

    private static final class BinaryBuffer extends ValueBuffer {

        private final List<byte[]> values = new ArrayList<>(1024);

        @Override
        void addBinary(byte[] value) {
            values.add(value);
        }

        @Override
        Object payloadValues(int nonNullCount) {
            byte[][] carrier = new byte[nonNullCount][];
            for (int i = 0; i < nonNullCount; i++) {
                carrier[i] = values.get(i);
            }
            return carrier;
        }

        @Override
        void clear() {
            values.clear();
        }
    }

    private static final class Int96Buffer extends ValueBuffer {

        private final List<byte[]> values = new ArrayList<>(1024);

        @Override
        void addInt96Bytes(byte[] packed) {
            values.add(packed);
        }

        @Override
        Object payloadValues(int nonNullCount) {
            byte[][] carrier = new byte[nonNullCount][];
            for (int i = 0; i < nonNullCount; i++) {
                carrier[i] = values.get(i);
            }
            return carrier;
        }

        @Override
        void clear() {
            values.clear();
        }
    }
}
