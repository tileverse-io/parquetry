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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.BloomFilterConfig;
import io.tileverse.parquetry.data.WriteOptions.EncodingPolicy;
import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.GeospatialStatistics;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.write.page.BinaryDictionaryEncoder;
import io.tileverse.parquetry.internal.write.page.BinaryPayload;
import io.tileverse.parquetry.internal.write.page.ChannelWrites;
import io.tileverse.parquetry.internal.write.page.DictionaryAttempt;
import io.tileverse.parquetry.internal.write.page.EncodedPage;
import io.tileverse.parquetry.internal.write.page.Encoder;
import io.tileverse.parquetry.internal.write.page.GrowableByteSink;
import io.tileverse.parquetry.internal.write.page.LittleEndianSink;
import io.tileverse.parquetry.internal.write.page.PageDictionaryEncoder;
import io.tileverse.parquetry.internal.write.page.PageEncodeJob;
import io.tileverse.parquetry.internal.write.page.PageStatistics;
import io.tileverse.parquetry.internal.write.page.PageWriter;
import io.tileverse.parquetry.internal.write.page.PlainBinaryEncoder;
import io.tileverse.parquetry.internal.write.page.PlainBooleanEncoder;
import io.tileverse.parquetry.internal.write.page.PlainDoubleEncoder;
import io.tileverse.parquetry.internal.write.page.PlainFixedLenBinaryEncoder;
import io.tileverse.parquetry.internal.write.page.PlainFloatEncoder;
import io.tileverse.parquetry.internal.write.page.PlainInt32Encoder;
import io.tileverse.parquetry.internal.write.page.PlainInt64Encoder;
import io.tileverse.parquetry.internal.write.page.PlainInt96Encoder;
import io.tileverse.parquetry.internal.write.page.PreEncodedPageJob;
import io.tileverse.parquetry.internal.write.page.PrimitiveDictionaryEncoder;
import io.tileverse.parquetry.schema.LevelMaxima;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

import lombok.NonNull;

/**
 * Per-column accumulator that drives {@link PageWriter} against a temp file.
 *
 * <p>Callers issue typed {@code appendXxx} calls until the page byte- or value-threshold trips; the writer then encodes
 * a data page through {@link PageWriter}. When dictionary encoding is enabled for the column, the writer drives a
 * {@link PageDictionaryEncoder}: indices accumulate in a column-chunk dictionary until either the chunk closes or the
 * dictionary outgrows {@link WriteOptions#dictionaryByteLimit()}. On overflow, subsequent pages fall back to
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
    private final WriteLeafLevels levels;

    private final LinkedHashSet<Encoding> usedEncodings = new LinkedHashSet<>();

    /**
     * Dictionary attempt encoder, when the column's encoding policy permits dictionary encoding and the primitive kind
     * supports it. {@code null} for forced-plain columns or kinds that bypass dictionary attempt.
     */
    private final DictionaryAttempt dictionary;

    /**
     * Buffers each data page's bytes in memory while {@link #dictionary} is active. The dictionary page is unknown
     * until {@link #finishChunk()} (its byte size depends on how many unique values landed in the column), so data
     * pages cannot stream directly to the temp file: they would either lock in a placeholder header whose
     * compact-protocol byte length cannot be patched in place, or force a final shuffle. Buffering bounds memory by the
     * chunk's compressed size and avoids both pitfalls.
     */
    private final List<byte[]> bufferedPages;

    /**
     * Assembles one page before it is flushed once: to the temp file for non-dictionary columns, or into
     * {@link #bufferedPages} for dictionary columns (and, at chunk finish, the dictionary page itself). Reused across
     * pages via {@link GrowableByteSink#reset()}.
     */
    private final GrowableByteSink pageSink = new GrowableByteSink(256);

    /**
     * Holds the dictionary encoder's pre-encoded value bytes for one data page; {@code null} for non-dictionary
     * columns.
     */
    private final GrowableByteSink dictValueSink;

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
        this(options, leaf, tempFile, new LevelMaxima(maxRepetitionLevelFor(leaf), maxDefinitionLevelFor(leaf)));
    }

    /**
     * Constructs a writer for a nested leaf whose repetition and definition level maxima are schema-derived rather than
     * local to the leaf's own repetition field. A nested optional leaf's maxDef accumulates across all enclosing
     * optional ancestors; this constructor receives those computed maxima directly.
     */
    ColumnChunkWriter(
            @NonNull WriteOptions options,
            @NonNull SchemaNode.Primitive leaf,
            @NonNull Path tempFile,
            @NonNull LevelMaxima levelMaxima)
            throws IOException {
        this.options = options;
        this.leaf = leaf;
        this.tempFile = tempFile;
        this.kind = leaf.kind();

        this.tempFileChannel = FileChannel.open(
                tempFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        Compression compression = resolveCompression(options, leaf.name());
        this.column = new ColumnContext(
                levelMaxima.maxRepetitionLevel(),
                levelMaxima.maxDefinitionLevel(),
                kind,
                options.parquetVersion(),
                compression);
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
        this.levels = new WriteLeafLevels(column.maxRepetitionLevel(), column.maxDefinitionLevel());

        this.dictionary = createDictionaryAttempt(options, leaf, kind);
        this.bufferedPages = dictionary != null ? new ArrayList<>() : null;
        this.dictValueSink = dictionary != null ? new GrowableByteSink(256) : null;
    }

    /** Append one INT32 cell. */
    public void appendInt(int value, int repLevel, int defLevel) {
        ensureKind(PrimitiveKind.INT32);
        recordLevels(repLevel, defLevel);
        addIntCell(value);
    }

    private void addIntCell(int value) {
        if (dictionary == null) {
            valueBuffer.addInt(value);
        }
        pageStats.updateInt(value);
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
        addLongCell(value);
    }

    private void addLongCell(long value) {
        if (dictionary == null) {
            valueBuffer.addLong(value);
        }
        pageStats.updateLong(value);
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
        addFloatCell(value);
    }

    private void addFloatCell(float value) {
        if (dictionary == null) {
            valueBuffer.addFloat(value);
        }
        pageStats.updateFloat(value);
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
        addDoubleCell(value);
    }

    private void addDoubleCell(double value) {
        if (dictionary == null) {
            valueBuffer.addDouble(value);
        }
        pageStats.updateDouble(value);
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
        addBooleanCell(value);
    }

    private void addBooleanCell(boolean value) {
        valueBuffer.addBoolean(value);
        pageStats.updateBoolean(value);
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
        addBinaryCell(value);
    }

    private void addBinaryCell(MemorySegment value) {
        if (dictionary == null) {
            valueBuffer.addBinary(value, 0, value.byteSize());
        } else {
            appendToDictionaryIfActive(value);
        }
        pageStats.updateBinary(value);
        addToBloom(value);
        if (geoStats != null) {
            geoStats.update(value);
        }
        pageByteEstimate += Integer.BYTES + value.byteSize();
        pageCellCount++;
        totalCells++;
        maybeFlushPage();
    }

    /** Append one FIXED_LEN_BYTE_ARRAY cell. */
    public void appendFixedLenBinary(@NonNull MemorySegment value, int repLevel, int defLevel) {
        ensureKind(PrimitiveKind.FIXED_LEN_BYTE_ARRAY);
        int expected = requireTypeLength(leaf);
        if (value.byteSize() != expected) {
            throw new ParquetWriteException("FIXED_LEN_BYTE_ARRAY value byte size " + value.byteSize()
                    + " does not match column type_length " + expected);
        }
        recordLevels(repLevel, defLevel);
        addFixedLenBinaryCell(value);
    }

    private void addFixedLenBinaryCell(MemorySegment value) {
        if (dictionary == null) {
            valueBuffer.addBinary(value, 0, value.byteSize());
        } else {
            appendToDictionaryIfActive(value);
        }
        pageStats.updateBinary(value);
        addToBloom(value);
        pageByteEstimate += value.byteSize();
        pageCellCount++;
        totalCells++;
        maybeFlushPage();
    }

    /** Append one INT96 cell, encoded as a (julianDay, timestamp-nanos) pair per the legacy INT96 layout. */
    public void appendInt96(long timestampNanos, int julianDay, int repLevel, int defLevel) {
        ensureKind(PrimitiveKind.INT96);
        recordLevels(repLevel, defLevel);
        addInt96Cell(timestampNanos, julianDay);
    }

    private void addInt96Cell(long timestampNanos, int julianDay) {
        byte[] packed = packInt96(timestampNanos, julianDay);
        if (dictionary == null) {
            valueBuffer.addBinary(packed);
        }
        pageStats.updateNonNull();
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
     *
     * <p>A flat vector expresses presence only at the leaf's own repetition: a present cell maps to a single definition
     * level. This is well-defined only when the chunk's actual maximum definition level equals that flat expectation.
     * An optional or repeated ancestor raises the maximum definition level above it, and a flat vector cannot express
     * the ancestor's per-row presence; rejecting that input here prevents writing present cells at too low a definition
     * level, which the reader would reconstruct as nulls.
     */
    public void appendVector(@NonNull SchemaNode.Primitive leaf, @NonNull ColumnVector vector) {
        int flatMaxDef = maxDefinitionLevelFor(leaf);
        if (column.maxDefinitionLevel() > flatMaxDef) {
            throw new ParquetWriteException("column " + leaf.name()
                    + " has optional or repeated ancestors and cannot be supplied as a flat column; "
                    + "author it through its enclosing struct or list");
        }
        int defLevelForValue = leaf.repetition() == Repetition.REQUIRED ? 0 : 1;
        Validity validity = vector.validity();
        int size = vector.size();
        switch (vector) {
            case io.tileverse.parquetry.columnar.IntVector iv ->
                appendIntVectorCells(iv, validity, size, defLevelForValue);
            case io.tileverse.parquetry.columnar.LongVector lv ->
                appendLongVectorCells(lv, validity, size, defLevelForValue);
            case io.tileverse.parquetry.columnar.FloatVector fv ->
                appendFloatVectorCells(fv, validity, size, defLevelForValue);
            case io.tileverse.parquetry.columnar.DoubleVector dv ->
                appendDoubleVectorCells(dv, validity, size, defLevelForValue);
            case io.tileverse.parquetry.columnar.BooleanVector bv ->
                appendBooleanVectorCells(bv, validity, size, defLevelForValue);
            case io.tileverse.parquetry.columnar.BinaryVector binv ->
                appendBinaryVectorCells(binv, validity, size, defLevelForValue);
            case io.tileverse.parquetry.columnar.FixedLenBinaryVector flbv ->
                appendFixedLenBinaryVectorCells(flbv, validity, size, defLevelForValue);
            case io.tileverse.parquetry.columnar.Int96Vector _,
                    io.tileverse.parquetry.columnar.ListVector _,
                    io.tileverse.parquetry.columnar.MapVector _,
                    io.tileverse.parquetry.columnar.LevelListVector _,
                    io.tileverse.parquetry.columnar.LevelMapVector _,
                    io.tileverse.parquetry.columnar.StructVector _,
                    io.tileverse.parquetry.columnar.VariantVector _,
                    io.tileverse.parquetry.columnar.ShreddedVariantVector _ ->
                throw new ParquetWriteException(
                        "appendVector: column " + leaf.name() + " uses unsupported vector kind " + vector.getClass());
        }
    }

    private void appendIntVectorCells(
            io.tileverse.parquetry.columnar.IntVector vector, Validity validity, int size, int defLevelForValue) {
        for (int i = 0; i < size; i++) {
            if (validity.isValid(i)) {
                appendInt(vector.valueAt(i), 0, defLevelForValue);
            } else {
                appendNull(0, 0);
            }
        }
    }

    private void appendLongVectorCells(
            io.tileverse.parquetry.columnar.LongVector vector, Validity validity, int size, int defLevelForValue) {
        for (int i = 0; i < size; i++) {
            if (validity.isValid(i)) {
                appendLong(vector.valueAt(i), 0, defLevelForValue);
            } else {
                appendNull(0, 0);
            }
        }
    }

    private void appendFloatVectorCells(
            io.tileverse.parquetry.columnar.FloatVector vector, Validity validity, int size, int defLevelForValue) {
        for (int i = 0; i < size; i++) {
            if (validity.isValid(i)) {
                appendFloat(vector.valueAt(i), 0, defLevelForValue);
            } else {
                appendNull(0, 0);
            }
        }
    }

    private void appendDoubleVectorCells(
            io.tileverse.parquetry.columnar.DoubleVector vector, Validity validity, int size, int defLevelForValue) {
        for (int i = 0; i < size; i++) {
            if (validity.isValid(i)) {
                appendDouble(vector.valueAt(i), 0, defLevelForValue);
            } else {
                appendNull(0, 0);
            }
        }
    }

    private void appendBooleanVectorCells(
            io.tileverse.parquetry.columnar.BooleanVector vector, Validity validity, int size, int defLevelForValue) {
        for (int i = 0; i < size; i++) {
            if (validity.isValid(i)) {
                appendBoolean(vector.valueAt(i), 0, defLevelForValue);
            } else {
                appendNull(0, 0);
            }
        }
    }

    private void appendBinaryVectorCells(
            io.tileverse.parquetry.columnar.BinaryVector vector, Validity validity, int size, int defLevelForValue) {
        for (int i = 0; i < size; i++) {
            if (validity.isValid(i)) {
                appendBinary(vector.get(i), 0, defLevelForValue);
            } else {
                appendNull(0, 0);
            }
        }
    }

    private void appendFixedLenBinaryVectorCells(
            io.tileverse.parquetry.columnar.FixedLenBinaryVector vector,
            Validity validity,
            int size,
            int defLevelForValue) {
        for (int i = 0; i < size; i++) {
            if (validity.isValid(i)) {
                appendFixedLenBinary(vector.get(i), 0, defLevelForValue);
            } else {
                appendNull(0, 0);
            }
        }
    }

    /**
     * Appends one shredded leaf's whole level and value streams in bulk. Validates the level arrays once, selects the
     * typed value loop once by vector kind, and preserves the exact per-cell page-boundary accounting of the per-cell
     * append API. {@code values} is the leaf's raw row-length vector; {@code keptOrdinals} lists the source ordinal of
     * each present value in stream order. Each occurrence whose definition level reaches {@code maxDef} reads the next
     * present value from {@code values} at its source ordinal in {@code keptOrdinals}; every other occurrence is a
     * null. A {@code null} {@code repLevels} means a non-repeated leaf whose repetition stream is uniformly zero; every
     * occurrence records repetition level zero. Only {@code [0, entryCount)} of the level arrays is read; both may be
     * oversized past that prefix.
     */
    public void appendStriped(
            @NonNull ColumnVector values,
            int[] keptOrdinals,
            int[] repLevels,
            int[] defLevels,
            int entryCount,
            int maxDef) {
        if (entryCount < 0 || entryCount > defLevels.length) {
            throw new ParquetWriteException("striped entryCount " + entryCount + " out of range for " + defLevels.length
                    + " definition levels on column " + leaf.name());
        }
        if (repLevels != null && repLevels.length < entryCount) {
            throw new ParquetWriteException(
                    "striped repetition levels shorter than entryCount " + entryCount + " for column " + leaf.name());
        }
        validateStripedLevels(repLevels, defLevels, entryCount);
        switch (values) {
            case io.tileverse.parquetry.columnar.IntVector iv ->
                appendStripedInts(iv, keptOrdinals, repLevels, defLevels, entryCount, maxDef);
            case io.tileverse.parquetry.columnar.LongVector lv ->
                appendStripedLongs(lv, keptOrdinals, repLevels, defLevels, entryCount, maxDef);
            case io.tileverse.parquetry.columnar.FloatVector fv ->
                appendStripedFloats(fv, keptOrdinals, repLevels, defLevels, entryCount, maxDef);
            case io.tileverse.parquetry.columnar.DoubleVector dv ->
                appendStripedDoubles(dv, keptOrdinals, repLevels, defLevels, entryCount, maxDef);
            case io.tileverse.parquetry.columnar.BooleanVector bv ->
                appendStripedBooleans(bv, keptOrdinals, repLevels, defLevels, entryCount, maxDef);
            case io.tileverse.parquetry.columnar.BinaryVector binv ->
                appendStripedBinaries(binv, keptOrdinals, repLevels, defLevels, entryCount, maxDef);
            case io.tileverse.parquetry.columnar.FixedLenBinaryVector flbv ->
                appendStripedFixedLenBinaries(flbv, keptOrdinals, repLevels, defLevels, entryCount, maxDef);
            default ->
                throw new ParquetWriteException("Unsupported value vector type for nested leaf: "
                        + values.getClass().getSimpleName());
        }
    }

    private void validateStripedLevels(int[] repLevels, int[] defLevels, int entryCount) {
        if (repLevels == null) {
            for (int i = 0; i < entryCount; i++) {
                validateLevel("definition", defLevels[i], column.maxDefinitionLevel());
            }
            return;
        }
        for (int i = 0; i < entryCount; i++) {
            validateLevels(repLevels[i], defLevels[i]);
        }
    }

    private void appendStripedInts(
            io.tileverse.parquetry.columnar.IntVector values,
            int[] keptOrdinals,
            int[] repLevels,
            int[] defLevels,
            int entryCount,
            int maxDef) {
        ensureKind(PrimitiveKind.INT32);
        int valueIndex = 0;
        for (int i = 0; i < entryCount; i++) {
            int rep = repLevels == null ? 0 : repLevels[i];
            int def = defLevels[i];
            recordLevelsUnchecked(rep, def);
            if (def == maxDef) {
                addIntCell(values.valueAt(keptOrdinals[valueIndex]));
                valueIndex++;
            } else {
                addNullCell();
            }
        }
    }

    private void appendStripedLongs(
            io.tileverse.parquetry.columnar.LongVector values,
            int[] keptOrdinals,
            int[] repLevels,
            int[] defLevels,
            int entryCount,
            int maxDef) {
        ensureKind(PrimitiveKind.INT64);
        int valueIndex = 0;
        for (int i = 0; i < entryCount; i++) {
            int rep = repLevels == null ? 0 : repLevels[i];
            int def = defLevels[i];
            recordLevelsUnchecked(rep, def);
            if (def == maxDef) {
                addLongCell(values.valueAt(keptOrdinals[valueIndex]));
                valueIndex++;
            } else {
                addNullCell();
            }
        }
    }

    private void appendStripedFloats(
            io.tileverse.parquetry.columnar.FloatVector values,
            int[] keptOrdinals,
            int[] repLevels,
            int[] defLevels,
            int entryCount,
            int maxDef) {
        ensureKind(PrimitiveKind.FLOAT);
        int valueIndex = 0;
        for (int i = 0; i < entryCount; i++) {
            int rep = repLevels == null ? 0 : repLevels[i];
            int def = defLevels[i];
            recordLevelsUnchecked(rep, def);
            if (def == maxDef) {
                addFloatCell(values.valueAt(keptOrdinals[valueIndex]));
                valueIndex++;
            } else {
                addNullCell();
            }
        }
    }

    private void appendStripedDoubles(
            io.tileverse.parquetry.columnar.DoubleVector values,
            int[] keptOrdinals,
            int[] repLevels,
            int[] defLevels,
            int entryCount,
            int maxDef) {
        ensureKind(PrimitiveKind.DOUBLE);
        int valueIndex = 0;
        for (int i = 0; i < entryCount; i++) {
            int rep = repLevels == null ? 0 : repLevels[i];
            int def = defLevels[i];
            recordLevelsUnchecked(rep, def);
            if (def == maxDef) {
                addDoubleCell(values.valueAt(keptOrdinals[valueIndex]));
                valueIndex++;
            } else {
                addNullCell();
            }
        }
    }

    private void appendStripedBooleans(
            io.tileverse.parquetry.columnar.BooleanVector values,
            int[] keptOrdinals,
            int[] repLevels,
            int[] defLevels,
            int entryCount,
            int maxDef) {
        ensureKind(PrimitiveKind.BOOLEAN);
        int valueIndex = 0;
        for (int i = 0; i < entryCount; i++) {
            int rep = repLevels == null ? 0 : repLevels[i];
            int def = defLevels[i];
            recordLevelsUnchecked(rep, def);
            if (def == maxDef) {
                addBooleanCell(values.valueAt(keptOrdinals[valueIndex]));
                valueIndex++;
            } else {
                addNullCell();
            }
        }
    }

    private void appendStripedBinaries(
            io.tileverse.parquetry.columnar.BinaryVector values,
            int[] keptOrdinals,
            int[] repLevels,
            int[] defLevels,
            int entryCount,
            int maxDef) {
        ensureKind(PrimitiveKind.BYTE_ARRAY);
        int valueIndex = 0;
        for (int i = 0; i < entryCount; i++) {
            int rep = repLevels == null ? 0 : repLevels[i];
            int def = defLevels[i];
            recordLevelsUnchecked(rep, def);
            if (def == maxDef) {
                addBinaryCell(values.get(keptOrdinals[valueIndex]));
                valueIndex++;
            } else {
                addNullCell();
            }
        }
    }

    private void appendStripedFixedLenBinaries(
            io.tileverse.parquetry.columnar.FixedLenBinaryVector values,
            int[] keptOrdinals,
            int[] repLevels,
            int[] defLevels,
            int entryCount,
            int maxDef) {
        ensureKind(PrimitiveKind.FIXED_LEN_BYTE_ARRAY);
        int expected = requireTypeLength(leaf);
        if (values.byteWidth() != expected) {
            throw new ParquetWriteException("FIXED_LEN_BYTE_ARRAY value byte size " + values.byteWidth()
                    + " does not match column type_length " + expected);
        }
        int valueIndex = 0;
        for (int i = 0; i < entryCount; i++) {
            int rep = repLevels == null ? 0 : repLevels[i];
            int def = defLevels[i];
            recordLevelsUnchecked(rep, def);
            if (def == maxDef) {
                addFixedLenBinaryCell(values.get(keptOrdinals[valueIndex]));
                valueIndex++;
            } else {
                addNullCell();
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
        addNullCell();
    }

    private void addNullCell() {
        pageStats.updateNull();
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
        int[] repLevels = levels.repetitionBacking();
        int[] defLevels = levels.definitionBacking();
        int rowCount = (int) pageRowCount();

        EncodedPage encoded;
        long pageStartOffset;
        pageSink.reset();
        if (dictionary != null) {
            encoded = encodeDictionaryDataPage(snapshot, repLevels, defLevels, rowCount, pageSink);
            pageStartOffset = bufferedDataBytes();
            bufferedPages.add(pageSink.toByteArray());
        } else {
            pageStartOffset = tempFileChannel.position();
            encoded = encodePlainDataPage(snapshot, repLevels, defLevels, rowCount, pageSink);
            ChannelWrites.writeFully(tempFileChannel, ByteBuffer.wrap(pageSink.array(), 0, pageSink.size()));
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
        chunkStats.merge(pageStats);
        pageStats.reset();
        valueBuffer.clear();
        levels.clear();
        pageCellCount = 0;
        pageNullCount = 0;
        pageByteEstimate = 0L;
        rowsInCurrentPage = 0L;
    }

    private EncodedPage encodePlainDataPage(
            PageStatistics snapshot, int[] repLevels, int[] defLevels, int rowCount, LittleEndianSink dst)
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
            PageStatistics snapshot, int[] repLevels, int[] defLevels, int rowCount, LittleEndianSink dst)
            throws IOException {
        dictValueSink.reset();
        PageDictionaryEncoder.PageResult pageResult = dictionary.encoder().flushPage(dictValueSink);
        MemorySegment encodedValues = dictValueSink.heapSegment();
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
            ChannelWrites.writeFully(tempFileChannel, ByteBuffer.wrap(page));
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
        pageSink.reset();
        EncodedPage encoded = dictionary.writeDictionaryPage(pageWriter, pageSink);
        ChannelWrites.writeFully(tempFileChannel, ByteBuffer.wrap(pageSink.array(), 0, pageSink.size()));
        uncompressedBytes += (long) encoded.headerBytes() + encoded.pageHeader().uncompressedPageSize();
        compressedBytes += encoded.totalBytes();
        usedEncodings.add(
                encoded.pageHeader().dictionaryPageHeader().orElseThrow().encoding());
        return encoded.totalBytes();
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
            case FIXED_LEN_BYTE_ARRAY -> new PlainFixedLenBinaryEncoder(requireTypeLength(leaf));
        };
    }

    private void ensureKind(PrimitiveKind expected) {
        if (kind != expected) {
            throw new ParquetWriteException(
                    "Type mismatch on column " + leaf.name() + ": expected " + expected + " but got " + kind);
        }
    }

    private void recordLevels(int repLevel, int defLevel) {
        validateLevels(repLevel, defLevel);
        recordLevelsUnchecked(repLevel, defLevel);
    }

    private void validateLevels(int repLevel, int defLevel) {
        validateLevel("repetition", repLevel, column.maxRepetitionLevel());
        validateLevel("definition", defLevel, column.maxDefinitionLevel());
    }

    /**
     * appendStriped stays byte-equivalent to the per-cell path only because recordLevels is exactly validateLevels then
     * this method; level logic added to recordLevels alone would never run on the striped path.
     */
    private void recordLevelsUnchecked(int repLevel, int defLevel) {
        levels.append(repLevel, defLevel);
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

    // S7475: Palantir formatter does not accept the bare `_` unnamed pattern; keep the typed unnamed binding.
    @SuppressWarnings("java:S7475")
    private void appendToDictionaryIfActive(int value) {
        if (dictionary
                instanceof DictionaryAttempt.NumericAttempt(PrimitiveKind _, PrimitiveDictionaryEncoder encoder)) {
            encoder.appendInt(value);
        }
    }

    // S7475: Palantir formatter does not accept the bare `_` unnamed pattern; keep the typed unnamed binding.
    @SuppressWarnings("java:S7475")
    private void appendToDictionaryIfActive(long value) {
        if (dictionary
                instanceof DictionaryAttempt.NumericAttempt(PrimitiveKind _, PrimitiveDictionaryEncoder encoder)) {
            encoder.appendLong(value);
        }
    }

    // S7475: Palantir formatter does not accept the bare `_` unnamed pattern; keep the typed unnamed binding.
    @SuppressWarnings("java:S7475")
    private void appendToDictionaryIfActive(float value) {
        if (dictionary
                instanceof DictionaryAttempt.NumericAttempt(PrimitiveKind _, PrimitiveDictionaryEncoder encoder)) {
            encoder.appendFloat(value);
        }
    }

    // S7475: Palantir formatter does not accept the bare `_` unnamed pattern; keep the typed unnamed binding.
    @SuppressWarnings("java:S7475")
    private void appendToDictionaryIfActive(double value) {
        if (dictionary
                instanceof DictionaryAttempt.NumericAttempt(PrimitiveKind _, PrimitiveDictionaryEncoder encoder)) {
            encoder.appendDouble(value);
        }
    }

    // S7475: Palantir formatter does not accept the bare `_` unnamed pattern; keep the typed unnamed binding.
    @SuppressWarnings("java:S7475")
    private void appendToDictionaryIfActive(MemorySegment value) {
        if (dictionary
                instanceof DictionaryAttempt.BinaryAttempt(BinaryDictionaryEncoder encoder, Encoder<BinaryPayload> _)) {
            encoder.appendValue(value);
        }
    }

    // S7475: Palantir formatter does not accept the bare `_` unnamed pattern; keep the typed unnamed binding.
    @SuppressWarnings("java:S7475")
    private void appendToDictionaryIfActive(byte[] packed) {
        if (dictionary
                instanceof DictionaryAttempt.BinaryAttempt(BinaryDictionaryEncoder encoder, Encoder<BinaryPayload> _)) {
            encoder.appendValue(packed);
        }
    }

    private static int maxRepetitionLevelFor(SchemaNode.Primitive leaf) {
        return leaf.repetition() == Repetition.REPEATED ? 1 : 0;
    }

    private static int maxDefinitionLevelFor(SchemaNode.Primitive leaf) {
        return leaf.repetition() == Repetition.REQUIRED ? 0 : 1;
    }

    private static int requireTypeLength(SchemaNode.Primitive leaf) {
        return leaf.typeLength()
                .orElseThrow(() -> new ParquetWriteException(FIXED_LEN_TYPE_LENGTH_REQUIRED_MESSAGE + leaf.name()));
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

    private static DictionaryAttempt createDictionaryAttempt(
            WriteOptions options, SchemaNode.Primitive leaf, PrimitiveKind kind) {
        EncodingPolicy policy = resolveEncodingPolicy(options, leaf);
        if (!dictionaryAttemptAllowed(policy, kind)) {
            return null;
        }
        long byteLimit = options.dictionaryByteLimit();
        return switch (kind) {
            case INT32, INT64, FLOAT, DOUBLE -> DictionaryAttempt.NumericAttempt.create(kind, byteLimit);
            case BYTE_ARRAY -> DictionaryAttempt.BinaryAttempt.create(byteLimit, new PlainBinaryEncoder());
            case FIXED_LEN_BYTE_ARRAY -> {
                int len = requireTypeLength(leaf);
                yield DictionaryAttempt.BinaryAttempt.createFixedLen(byteLimit, len);
            }
            case INT96 -> DictionaryAttempt.BinaryAttempt.createFixedLen(byteLimit, 12);
            case BOOLEAN -> null;
        };
    }

    /**
     * Resolves the effective encoding policy for a leaf. An explicit per-column override always wins. Absent one,
     * geometry and geography columns default to {@link EncodingPolicy#FORCE_PLAIN}: their WKB is effectively unique per
     * row, where the dictionary attempt overflows and never pays for itself. Every other column defaults to
     * {@link EncodingPolicy#AUTO}.
     */
    private static EncodingPolicy resolveEncodingPolicy(WriteOptions options, SchemaNode.Primitive leaf) {
        EncodingPolicy explicit = options.encodingPolicies().get(leaf.name());
        if (explicit != null) {
            return explicit;
        }
        if (isGeometryLike(leaf.logicalType().orElse(null))) {
            return EncodingPolicy.FORCE_PLAIN;
        }
        return EncodingPolicy.AUTO;
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
        ByteArrayOutputStream framed = new ByteArrayOutputStream();
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
}
