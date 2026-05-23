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

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteRow;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

import lombok.NonNull;

/**
 * Per-row-group accumulator that fans incoming records out to one {@link ColumnChunkWriter} per leaf column, then
 * consolidates every column's temp file into the caller's output stream at flush time.
 *
 * <p>Flat schemas only in this phase: every column is at the root group and repetition is {@link Repetition#REQUIRED}
 * or {@link Repetition#OPTIONAL}. Nested groups and repeated columns fail fast at construction. A dataset-level writer
 * layer wires this writer to record sources.
 *
 * <p>Consolidation runs sequentially in schema order. {@code ColumnChunkWriter.finishChunk()} is bookkeeping plus a
 * final partial-page flush; parallelising it via {@link java.util.concurrent.StructuredTaskScope} buys nothing until
 * the dataset-level benchmark says otherwise. The per-column writers themselves are still single-threaded and
 * independently owned: a future move to parallel consolidation is a local change at this layer.
 *
 * <p>The writer copies each column's temp file straight into the supplied {@link WritableByteChannel} and rewrites
 * every {@link OffsetIndex} entry to absolute file offsets in the process. Bloom filter blobs and column / offset
 * indexes are returned in the {@link RowGroupFlushResult}; the dataset-level writer is responsible for placing them in
 * the file between row groups and the footer.
 */
public final class RowGroupWriter implements AutoCloseable {

    private final ParquetSchema schema;
    private final Path tempDir;

    private final List<LeafBinding> leaves;
    private final Map<ColumnPath, LeafBinding> leafByPath;

    private long rowCount;
    private long currentFileOffset;
    private boolean flushed;
    private boolean closed;

    public RowGroupWriter(WriteOptions options, ParquetSchema schema, Path tempDir) throws IOException {
        this(options, schema, tempDir, 0L);
    }

    /**
     * Variant that places this row group at {@code baseFileOffset} bytes into the surrounding file. The dataset-level
     * writer passes the post-magic, post-prior-row-group cursor so the absolute offsets baked into every
     * {@link ColumnMetaData} and {@link OffsetIndex} entry match the file's true byte layout.
     */
    public RowGroupWriter(
            @NonNull WriteOptions options, @NonNull ParquetSchema schema, @NonNull Path tempDir, long baseFileOffset)
            throws IOException {
        if (baseFileOffset < 0L) {
            throw new IllegalArgumentException("baseFileOffset must be non-negative: " + baseFileOffset);
        }
        this.schema = schema;
        this.tempDir = tempDir;
        this.currentFileOffset = baseFileOffset;

        Files.createDirectories(tempDir);

        this.leaves = openLeafBindings(options, schema, tempDir);
        this.leafByPath = indexByPath(leaves);
    }

    /**
     * Appends one row by pulling each leaf's value out of {@code row} and dispatching it to the matching
     * {@link ColumnChunkWriter}. Required columns must carry a non-null value; optional columns may return {@code null}
     * from {@link WriteRow#value(ColumnPath)} to record an absent leaf.
     */
    public void append(@NonNull WriteRow row) {
        if (flushed) {
            throw new ParquetWriteException("Cannot append after flushTo on RowGroupWriter");
        }
        for (LeafBinding binding : leaves) {
            appendOne(binding, row.value(binding.path));
        }
        rowCount++;
    }

    /**
     * Appends one whole batch of rows by walking each column vector cell by cell and dispatching to the matching typed
     * {@link ColumnChunkWriter} method. Nulls go through {@link ColumnChunkWriter#appendNull}; non-nulls go through the
     * primitive-kind-specialised {@code appendXxx} so the dictionary attempt, statistics, and bloom filter stay in sync
     * with the per-row write path.
     */
    public void appendBatch(@NonNull ParquetRecordBatch batch) {
        if (flushed) {
            throw new ParquetWriteException("Cannot appendBatch after flushTo on RowGroupWriter");
        }
        int batchRows = batch.rowCount();
        java.util.Map<ColumnPath, io.tileverse.parquetry.batch.ColumnVector> columns = batch.columns();
        for (LeafBinding binding : leaves) {
            io.tileverse.parquetry.batch.ColumnVector vector = columns.get(binding.path);
            if (vector == null) {
                throw new ParquetWriteException("Batch is missing column " + binding.path.dot());
            }
            if (vector.size() != batchRows) {
                throw new ParquetWriteException("Batch column " + binding.path.dot() + " size " + vector.size()
                        + " does not match batch rowCount " + batchRows);
            }
            binding.writer.appendVector(binding.leaf, vector);
        }
        rowCount += batchRows;
    }

    /**
     * Consolidates every column chunk into {@code out}, returning the assembled descriptor.
     *
     * <p>Throws {@link ParquetWriteException} when called on a row group that received no appends: Parquet readers
     * reject empty row groups, and silently skipping the flush would lose the writer state.
     */
    public RowGroupFlushResult flushTo(@NonNull WritableByteChannel out) throws IOException {
        if (flushed) {
            throw new ParquetWriteException("flushTo already called on RowGroupWriter");
        }
        if (rowCount == 0L) {
            throw new ParquetWriteException(
                    "Cannot flush an empty row group: parquet-format readers reject row groups with zero rows");
        }
        flushed = true;

        long rowGroupBaseOffset = currentFileOffset;
        List<ColumnChunk> chunks = new ArrayList<>(leaves.size());
        List<RowGroupFlushResult.ColumnArtifacts> artifacts = new ArrayList<>(leaves.size());
        long totalCompressed = 0L;
        long totalUncompressed = 0L;
        long dictionaryBytes = 0L;

        for (LeafBinding binding : leaves) {
            ColumnChunkResult result = binding.writer.finishChunk();
            long absoluteFirstDataPageOffset = currentFileOffset + result.firstDataPageOffset();
            long absoluteDictionaryPageOffset =
                    result.dictionaryPageOffset() < 0L ? -1L : currentFileOffset + result.dictionaryPageOffset();

            copyTempFileTo(result.tempFile(), out, result.compressedBytes());
            deleteTempFile(result.tempFile());

            OffsetIndex absoluteOffsetIndex = rebaseOffsetIndex(result.offsetIndex(), currentFileOffset);

            ColumnMetaData meta =
                    buildColumnMetaData(binding, result, absoluteFirstDataPageOffset, absoluteDictionaryPageOffset);

            ColumnChunk chunk = ColumnChunk.builder()
                    .fileOffset(absoluteFirstDataPageOffset)
                    .metaData(Optional.of(meta))
                    .build();
            chunks.add(chunk);

            artifacts.add(new RowGroupFlushResult.ColumnArtifacts(
                    binding.path,
                    result.columnIndex(),
                    absoluteOffsetIndex,
                    result.bloomFilterBytes(),
                    result.geospatialStatistics()));

            currentFileOffset += result.compressedBytes();
            totalCompressed += result.compressedBytes();
            totalUncompressed += result.uncompressedBytes();
            if (result.dictionaryPageOffset() >= 0L) {
                dictionaryBytes += result.firstDataPageOffset() - result.dictionaryPageOffset();
            }
        }

        RowGroup rowGroup = RowGroup.builder()
                .columns(chunks)
                .totalByteSize(totalUncompressed)
                .numRows(rowCount)
                .fileOffset(OptionalLong.of(rowGroupBaseOffset))
                .totalCompressedSize(OptionalLong.of(totalCompressed))
                .build();

        return new RowGroupFlushResult(rowGroup, totalCompressed, dictionaryBytes, artifacts);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (flushed) {
            return;
        }
        IOException firstFailure = null;
        for (LeafBinding binding : leaves) {
            try {
                discardUnflushedWriter(binding);
            } catch (IOException e) {
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

    /** Schema-driven dispatch from a boxed value to the matching typed {@code appendXxx} on the column writer. */
    private void appendOne(LeafBinding binding, Object value) {
        ColumnChunkWriter writer = binding.writer;
        SchemaNode.Primitive leaf = binding.leaf;
        if (value == null) {
            if (leaf.repetition() == Repetition.REQUIRED) {
                throw new ParquetWriteException("Null value for required column " + leaf.name());
            }
            writer.appendNull(0, 0);
            return;
        }
        int defLevel = leaf.repetition() == Repetition.REQUIRED ? 0 : 1;
        switch (leaf.kind()) {
            case INT32 -> writer.appendInt(asInt(value, leaf), 0, defLevel);
            case INT64 -> writer.appendLong(asLong(value, leaf), 0, defLevel);
            case FLOAT -> writer.appendFloat(asFloat(value, leaf), 0, defLevel);
            case DOUBLE -> writer.appendDouble(asDouble(value, leaf), 0, defLevel);
            case BOOLEAN -> writer.appendBoolean(asBoolean(value, leaf), 0, defLevel);
            case BYTE_ARRAY -> writer.appendBinary(asReadOnlySegment(value, leaf), 0, defLevel);
            case FIXED_LEN_BYTE_ARRAY -> writer.appendFixedLenBinary(asReadOnlySegment(value, leaf), 0, defLevel);
            case INT96 ->
                throw new ParquetWriteException(
                        "INT96 columns require typed appendInt96 on the column writer; not supported via WriteRow");
        }
    }

    private ColumnMetaData buildColumnMetaData(
            LeafBinding binding,
            ColumnChunkResult result,
            long absoluteFirstDataPageOffset,
            long absoluteDictionaryPageOffset) {
        return ColumnMetaData.builder()
                .type(toPhysicalType(binding.leaf.kind()))
                .encodings(result.encodings())
                .pathInSchema(binding.path.parts())
                .codec(binding.compression.wireCodec())
                .numValues(result.numValues())
                .totalUncompressedSize(result.uncompressedBytes())
                .totalCompressedSize(result.compressedBytes())
                .dataPageOffset(absoluteFirstDataPageOffset)
                .dictionaryPageOffset(
                        absoluteDictionaryPageOffset < 0L
                                ? OptionalLong.empty()
                                : OptionalLong.of(absoluteDictionaryPageOffset))
                .statistics(Optional.ofNullable(result.chunkStatistics()))
                .geospatialStatistics(Optional.ofNullable(result.geospatialStatistics()))
                .build();
    }

    private void copyTempFileTo(Path tempFile, WritableByteChannel out, long expectedBytes) throws IOException {
        try (FileChannel in = FileChannel.open(tempFile, StandardOpenOption.READ)) {
            long actual = in.size();
            if (actual != expectedBytes) {
                throw new ParquetWriteException("Temp file " + tempFile + " size " + actual + " does not match "
                        + "ColumnChunkResult.compressedBytes " + expectedBytes);
            }
            transferAll(in, out, expectedBytes);
        }
    }

    private static void transferAll(FileChannel src, WritableByteChannel dst, long expectedBytes) throws IOException {
        long position = 0L;
        long remaining = expectedBytes;
        while (remaining > 0L) {
            long transferred = src.transferTo(position, remaining, dst);
            if (transferred == 0L) {
                throw new IOException("FileChannel.transferTo made no progress");
            }
            position += transferred;
            remaining -= transferred;
        }
    }

    private void deleteTempFile(Path tempFile) throws IOException {
        Files.deleteIfExists(tempFile);
    }

    private OffsetIndex rebaseOffsetIndex(OffsetIndex offsetIndex, long base) {
        if (offsetIndex == null) {
            return null;
        }
        List<PageLocation> shifted = new ArrayList<>(offsetIndex.pageLocations().size());
        for (PageLocation loc : offsetIndex.pageLocations()) {
            shifted.add(new PageLocation(loc.offset() + base, loc.compressedPageSize(), loc.firstRowIndex()));
        }
        return new OffsetIndex(shifted, offsetIndex.unencodedByteArrayDataBytes());
    }

    private void discardUnflushedWriter(LeafBinding binding) throws IOException {
        try {
            binding.writer.close();
        } catch (ParquetWriteException _) {
            /* writer carried pending values; the explicit close throws but we still want temp-file cleanup */
        } finally {
            Files.deleteIfExists(binding.tempFile);
        }
    }

    private static List<LeafBinding> openLeafBindings(WriteOptions options, ParquetSchema schema, Path tempDir)
            throws IOException {
        List<ColumnPath> leafPaths = schema.leafColumns();
        List<LeafBinding> bindings = new ArrayList<>(leafPaths.size());
        for (ColumnPath path : leafPaths) {
            SchemaNode field = schema.find(path)
                    .orElseThrow(() -> new ParquetWriteException(
                            "Schema is missing leaf column declared by leafColumns(): " + path.dot()));
            if (!(field instanceof SchemaNode.Primitive leaf)) {
                throw new ParquetWriteException("Schema field " + path.dot() + " is a group, not a primitive leaf");
            }
            rejectRepeatedLeaf(path, leaf);
            Path tempFile = Files.createTempFile(tempDir, "rgw-" + safeFileName(path) + "-", ".tmp");
            ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile);
            Compression compression = resolveCompression(options, leaf.name());
            bindings.add(new LeafBinding(path, leaf, writer, tempFile, compression));
        }
        return bindings;
    }

    private static void rejectRepeatedLeaf(ColumnPath path, SchemaNode.Primitive leaf) {
        if (leaf.repetition() == Repetition.REPEATED) {
            throw new ParquetWriteException(
                    "Repeated leaf columns are not supported by RowGroupWriter yet: " + path.dot());
        }
    }

    private static Map<ColumnPath, LeafBinding> indexByPath(List<LeafBinding> bindings) {
        Map<ColumnPath, LeafBinding> index = LinkedHashMap.newLinkedHashMap(bindings.size());
        for (LeafBinding binding : bindings) {
            index.put(binding.path, binding);
        }
        return Map.copyOf(index);
    }

    private static Compression resolveCompression(WriteOptions options, String columnName) {
        Compression override = options.compression().get(columnName);
        if (override != null) {
            return override;
        }
        return options.defaultCompression();
    }

    private static String safeFileName(ColumnPath path) {
        return path.dot().replace('.', '_').replaceAll("[^A-Za-z0-9_-]", "_");
    }

    private static PhysicalType toPhysicalType(PrimitiveKind kind) {
        return switch (kind) {
            case BOOLEAN -> PhysicalType.BOOLEAN;
            case INT32 -> PhysicalType.INT32;
            case INT64 -> PhysicalType.INT64;
            case INT96 -> PhysicalType.INT96;
            case FLOAT -> PhysicalType.FLOAT;
            case DOUBLE -> PhysicalType.DOUBLE;
            case BYTE_ARRAY -> PhysicalType.BYTE_ARRAY;
            case FIXED_LEN_BYTE_ARRAY -> PhysicalType.FIXED_LEN_BYTE_ARRAY;
        };
    }

    private static int asInt(Object value, SchemaNode.Primitive leaf) {
        if (value instanceof Integer i) {
            return i;
        }
        throw typeMismatch(leaf, value, "Integer");
    }

    private static long asLong(Object value, SchemaNode.Primitive leaf) {
        if (value instanceof Long l) {
            return l;
        }
        if (value instanceof Integer i) {
            return i.longValue();
        }
        throw typeMismatch(leaf, value, "Long");
    }

    private static float asFloat(Object value, SchemaNode.Primitive leaf) {
        if (value instanceof Float f) {
            return f;
        }
        throw typeMismatch(leaf, value, "Float");
    }

    private static double asDouble(Object value, SchemaNode.Primitive leaf) {
        if (value instanceof Double d) {
            return d;
        }
        if (value instanceof Float f) {
            return f.doubleValue();
        }
        throw typeMismatch(leaf, value, "Double");
    }

    private static boolean asBoolean(Object value, SchemaNode.Primitive leaf) {
        if (value instanceof Boolean b) {
            return b;
        }
        throw typeMismatch(leaf, value, "Boolean");
    }

    private static MemorySegment asReadOnlySegment(Object value, SchemaNode.Primitive leaf) {
        if (value instanceof MemorySegment segment) {
            return segment.isReadOnly() ? segment : segment.asReadOnly();
        }
        if (value instanceof byte[] bytes) {
            return MemorySegment.ofArray(bytes).asReadOnly();
        }
        if (value instanceof java.nio.ByteBuffer buffer) {
            byte[] copy = new byte[buffer.remaining()];
            buffer.duplicate().get(copy);
            return MemorySegment.ofArray(copy).asReadOnly();
        }
        throw typeMismatch(leaf, value, "MemorySegment");
    }

    private static ParquetWriteException typeMismatch(SchemaNode.Primitive leaf, Object value, String expected) {
        String actual = value == null ? "null" : value.getClass().getName();
        return new ParquetWriteException("Column " + leaf.name() + " expects " + expected + " values, got " + actual);
    }

    /** Returns the schema this writer was constructed with; useful for the dataset-level writer. */
    public ParquetSchema schema() {
        return schema;
    }

    /** Returns the temp directory the writer was constructed with. */
    public Path tempDir() {
        return tempDir;
    }

    /** Returns the number of records appended so far. */
    public long rowCount() {
        return rowCount;
    }

    /**
     * Sum of every column writer's running uncompressed-byte tally. Used by the sizing policy at the dataset writer to
     * decide when to seal this row group.
     */
    public long estimatedUncompressedBytes() {
        long total = 0L;
        for (LeafBinding binding : leaves) {
            total += binding.writer.estimatedUncompressedBytes();
        }
        return total;
    }

    /**
     * Resolves a leaf binding for diagnostic / testing purposes. Returns {@code null} when the path does not name a
     * leaf in the row group's schema.
     */
    ColumnChunkWriter columnWriter(ColumnPath path) {
        LeafBinding binding = leafByPath.get(path);
        return binding == null ? null : binding.writer;
    }

    /**
     * Pairs a leaf column path with its open writer and the temp file the writer accumulates into. Captured up front so
     * flush-time iteration and close-time cleanup share the same state.
     */
    private record LeafBinding(
            ColumnPath path,
            SchemaNode.Primitive leaf,
            ColumnChunkWriter writer,
            Path tempFile,
            Compression compression) {}
}
