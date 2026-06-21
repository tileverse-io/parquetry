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
package io.tileverse.parquetry.internal.write;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
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
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.io.ByteSink;
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
 * <p>The writer copies each column's temp file straight into the supplied {@link ByteSink} and rewrites every
 * {@link OffsetIndex} entry to absolute file offsets in the process. Bloom filter blobs and column / offset indexes are
 * returned in the {@link RowGroupFlushResult}; the dataset-level writer is responsible for placing them in the file
 * between row groups and the footer.
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

    public RowGroupWriter(WriteOptions options, ParquetSchema schema, Path tempDir) {
        this(options, schema, tempDir, 0L);
    }

    /**
     * Variant that places this row group at {@code baseFileOffset} bytes into the surrounding file. The dataset-level
     * writer passes the post-magic, post-prior-row-group cursor so the absolute offsets baked into every
     * {@link ColumnMetaData} and {@link OffsetIndex} entry match the file's true byte layout.
     */
    public RowGroupWriter(
            @NonNull WriteOptions options, @NonNull ParquetSchema schema, @NonNull Path tempDir, long baseFileOffset) {
        if (baseFileOffset < 0L) {
            throw new IllegalArgumentException("baseFileOffset must be non-negative: " + baseFileOffset);
        }
        this.schema = schema;
        this.tempDir = tempDir;
        this.currentFileOffset = baseFileOffset;

        createTempDir(tempDir);

        this.leaves = openLeafBindings(options, schema, tempDir);
        this.leafByPath = indexByPath(leaves);
    }

    private static void createTempDir(Path tempDir) {
        try {
            Files.createDirectories(tempDir);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create row-group temp directory " + tempDir, e);
        }
    }

    /**
     * Appends one whole batch of rows by walking each column vector cell by cell and dispatching to the matching typed
     * {@link ColumnChunkWriter} method. Nulls go through {@link ColumnChunkWriter#appendNull}; non-nulls go through the
     * primitive-kind-specialised {@code appendXxx}, keeping the dictionary attempt, statistics, and bloom filter in
     * sync.
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
    public RowGroupFlushResult flushTo(@NonNull ByteSink out) {
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
            ColumnChunkResult result = finishChunk(binding);
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

    private static ColumnChunkResult finishChunk(LeafBinding binding) {
        try {
            return binding.writer.finishChunk();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to finish column chunk " + binding.path.dot(), e);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (flushed) {
            return;
        }
        RuntimeException firstFailure = null;
        for (LeafBinding binding : leaves) {
            try {
                discardUnflushedWriter(binding);
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

    private ColumnMetaData buildColumnMetaData(
            LeafBinding binding,
            ColumnChunkResult result,
            long absoluteFirstDataPageOffset,
            long absoluteDictionaryPageOffset) {
        return ColumnMetaData.builder()
                .type(toPhysicalType(binding.leaf.kind()))
                .encodings(result.encodings())
                .pathInSchema(pathInSchema(binding.path))
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

    private static List<String> pathInSchema(ColumnPath path) {
        List<String> segments = new ArrayList<>(path.numParts());
        for (int i = 0; i < path.numParts(); i++) {
            segments.add(path.part(i));
        }
        return segments;
    }

    private void copyTempFileTo(Path tempFile, ByteSink out, long expectedBytes) {
        try (FileChannel in = FileChannel.open(tempFile, StandardOpenOption.READ)) {
            long actual = in.size();
            if (actual != expectedBytes) {
                throw new ParquetWriteException("Temp file " + tempFile + " size " + actual + " does not match "
                        + "ColumnChunkResult.compressedBytes " + expectedBytes);
            }
            out.transferFrom(in, 0L, expectedBytes);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to copy temp file " + tempFile + " into the sink", e);
        }
    }

    private void deleteTempFile(Path tempFile) {
        try {
            Files.deleteIfExists(tempFile);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to delete temp file " + tempFile, e);
        }
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

    private void discardUnflushedWriter(LeafBinding binding) {
        try {
            binding.writer.close();
        } catch (ParquetWriteException _) {
            /* writer held pending values; the explicit close throws but we still want temp-file cleanup */
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close column writer for " + binding.path.dot(), e);
        } finally {
            deleteTempFile(binding.tempFile);
        }
    }

    private static List<LeafBinding> openLeafBindings(WriteOptions options, ParquetSchema schema, Path tempDir) {
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
            bindings.add(openLeafBinding(options, tempDir, path, leaf));
        }
        return bindings;
    }

    private static LeafBinding openLeafBinding(
            WriteOptions options, Path tempDir, ColumnPath path, SchemaNode.Primitive leaf) {
        try {
            Path tempFile = Files.createTempFile(tempDir, "rgw-" + safeFileName(path) + "-", ".tmp");
            ColumnChunkWriter writer = new ColumnChunkWriter(options, leaf, tempFile);
            Compression compression = resolveCompression(options, leaf.name());
            return new LeafBinding(path, leaf, writer, tempFile, compression);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open column writer for " + path.dot(), e);
        }
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
