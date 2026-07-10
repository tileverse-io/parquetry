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

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
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
import io.tileverse.parquetry.runtime.ComputeExecutor;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.LevelMaxima;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

import lombok.NonNull;

/**
 * Per-row-group accumulator that fans incoming records out to one {@link ColumnChunkWriter} per leaf column, then
 * consolidates every column's temp file into the caller's output stream at flush time.
 *
 * <p>Flat and struct-nested schemas are both supported. Repeated columns (list / map elements) are rejected at
 * construction. A dataset-level writer layer wires this writer to record sources.
 *
 * <p>{@link #appendBatch} fans each column's append out as one unit on the shared
 * {@link io.tileverse.parquetry.runtime.ComputeExecutor} (submit-or-inline, joined per batch), keeping encode and
 * compress work off the single caller thread. Each {@link ColumnChunkWriter} is touched by at most one thread at a
 * time, and the per-batch join publishes its state back to the caller. Consolidation in {@link #flushTo} stays
 * sequential in schema order, which keeps output bytes independent of encode completion order.
 * {@code ColumnChunkWriter.finishChunk()} is bookkeeping plus a final partial-page flush; parallelising it buys nothing
 * until the dataset-level benchmark says otherwise.
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
    private final ColumnFanOut fanOut;

    private long rowCount;
    private long currentFileOffset;
    private boolean flushed;
    private boolean closed;

    public RowGroupWriter(WriteOptions options, ParquetSchema schema, Path tempDir) {
        this(options, schema, tempDir, 0L, ComputeExecutor.shared());
    }

    /**
     * Places this row group at {@code baseFileOffset} bytes into the surrounding file. The dataset-level writer passes
     * the post-magic, post-prior-row-group cursor; the absolute offsets baked into every {@link ColumnMetaData} and
     * {@link OffsetIndex} entry then match the file's true byte layout.
     */
    public RowGroupWriter(WriteOptions options, ParquetSchema schema, Path tempDir, long baseFileOffset) {
        this(options, schema, tempDir, baseFileOffset, ComputeExecutor.shared());
    }

    /**
     * Canonical constructor: {@code computeExecutor} is the shared pool each batch's per-column appends fan out on,
     * submit-or-inline. The overloads without an executor use {@link ComputeExecutor#shared()}.
     */
    public RowGroupWriter(
            @NonNull WriteOptions options,
            @NonNull ParquetSchema schema,
            @NonNull Path tempDir,
            long baseFileOffset,
            @NonNull ComputeExecutor computeExecutor) {
        if (baseFileOffset < 0L) {
            throw new IllegalArgumentException("baseFileOffset must be non-negative: " + baseFileOffset);
        }
        this.schema = schema;
        this.tempDir = tempDir;
        this.currentFileOffset = baseFileOffset;
        this.fanOut = new ColumnFanOut(computeExecutor);

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
     * Appends one whole batch of rows. Flat leaves (top-level primitives) feed directly to their
     * {@link ColumnChunkWriter} via the per-vector fast path, keeping the flat path byte-identical to the pre-struct
     * writer. Struct leaves (top-level struct vectors) are shredded once per batch by {@link DremelStriper} and then
     * fed cell-by-cell through the level-aware {@code appendXxx} / {@code appendNull} API. Both stages fan out per
     * column on the shared compute pool and join before this method returns; {@code rowCount} advances on the caller
     * after the join.
     */
    public void appendBatch(@NonNull ParquetRecordBatch batch) {
        if (flushed) {
            throw new ParquetWriteException("Cannot appendBatch after flushTo on RowGroupWriter");
        }
        int batchRows = batch.rowCount();
        Map<ColumnPath, ColumnVector> columns = batch.columns();
        checkAllLeavesPresent(columns);
        appendDirectLeaves(columns, batchRows);
        appendStripedLeaves(batch, columns);
        rowCount += batchRows;
    }

    /**
     * Verifies that every leaf declared by the schema is supplied by the batch, either directly (its full path is a
     * primitive vector key) or indirectly (its root path-component is present as a top-level nested vector in the
     * batch). Throws {@link ParquetWriteException} for any leaf whose root is absent from the batch.
     */
    private void checkAllLeavesPresent(Map<ColumnPath, ColumnVector> columns) {
        for (LeafBinding binding : leaves) {
            if (binding.requiresStriping()) {
                checkStripedLeafSupplied(binding, columns);
            } else {
                checkFlatLeafSupplied(binding, columns);
            }
        }
    }

    /**
     * A leaf with an OPTIONAL or REPEATED ancestor must be authored from its enclosing struct / list / map vector,
     * present at the leaf's top-level path. The leaf's own row-aligned vector alone cannot express the ancestor levels,
     * hence supplying only the flat leaf is rejected.
     */
    private static void checkStripedLeafSupplied(LeafBinding binding, Map<ColumnPath, ColumnVector> columns) {
        ColumnPath rootPath = ColumnPath.of(binding.path.part(0));
        if (!columns.containsKey(rootPath)) {
            throw new ParquetWriteException("column " + binding.path.dot()
                    + " has optional or repeated ancestors and cannot be supplied as a flat column; "
                    + "author it through its enclosing struct or list");
        }
    }

    /** A flat leaf is supplied directly by its full path, or through a top-level vector at its root path. */
    private static void checkFlatLeafSupplied(LeafBinding binding, Map<ColumnPath, ColumnVector> columns) {
        if (columns.containsKey(binding.path)) {
            return;
        }
        ColumnPath rootPath = ColumnPath.of(binding.path.part(0));
        if (!columns.containsKey(rootPath)) {
            throw new ParquetWriteException("Batch is missing column " + binding.path.dot());
        }
    }

    /**
     * Writes every leaf the batch supplies directly as its own primitive vector, keyed by the full leaf path. This
     * covers flat columns and pre-flattened nested leaves (a producer that emits {@code bbox.xmin} as a standalone
     * column), and stays byte-identical to the pre-struct writer. Size validation runs on the caller before any column
     * is touched; each surviving leaf's append then runs as one fan-out unit on the shared compute pool, joined before
     * returning. Leaves the batch supplies only through a top-level nested vector are written by
     * {@link #appendStripedLeaves}.
     */
    private void appendDirectLeaves(Map<ColumnPath, ColumnVector> columns, int batchRows) {
        List<Runnable> units = new ArrayList<>(leaves.size());
        for (LeafBinding binding : leaves) {
            if (binding.requiresStriping()) {
                continue;
            }
            ColumnVector vector = columns.get(binding.path);
            if (vector == null) {
                continue;
            }
            if (vector.size() != batchRows) {
                throw new ParquetWriteException("Batch column " + binding.path.dot() + " size " + vector.size()
                        + " does not match batch rowCount " + batchRows);
            }
            units.add(() -> binding.writer.appendVector(binding.leaf, vector));
        }
        fanOut.run(units);
    }

    /**
     * Shreds the leaves the batch supplies only through a top-level nested vector (a struct column) and feeds each
     * leaf's level stream cell by cell. The striper runs once, serially, on the caller: it is shared work over the
     * whole batch. Each shredded leaf's feed then runs as one fan-out unit on the shared compute pool, joined before
     * returning. The striper runs only when at least one leaf is supplied indirectly, which keeps a flat batch from
     * constructing it.
     */
    private void appendStripedLeaves(ParquetRecordBatch batch, Map<ColumnPath, ColumnVector> columns) {
        if (allLeavesWrittenDirectly(columns)) {
            return;
        }
        List<StripedLeaf> striped = new DremelStriper(schema).stripe(batch);
        List<Runnable> units = new ArrayList<>(striped.size());
        for (StripedLeaf sl : striped) {
            LeafBinding binding = leafByPath.get(sl.leaf());
            if (binding == null) {
                continue;
            }
            if (writtenDirectly(binding, columns)) {
                // Already written by the direct-leaf path; skip to avoid double-writing.
                continue;
            }
            units.add(() -> appendStripedLeaf(binding.writer, sl));
        }
        fanOut.run(units);
    }

    private boolean allLeavesWrittenDirectly(Map<ColumnPath, ColumnVector> columns) {
        for (LeafBinding binding : leaves) {
            if (!writtenDirectly(binding, columns)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether the batch authors this leaf through the bulk flat-vector path: it needs no striping and the batch
     * supplies it directly by its full leaf path. A leaf that requires striping is authored from its enclosing struct /
     * list / map vector even when the batch also exposes the leaf's row-aligned vector by path.
     */
    private static boolean writtenDirectly(LeafBinding binding, Map<ColumnPath, ColumnVector> columns) {
        return !binding.requiresStriping() && columns.containsKey(binding.path);
    }

    /**
     * Feeds one shredded leaf to its column chunk writer, cell by cell. Rows whose definition level is below maxDef are
     * nulls; rows at maxDef contribute a value drawn sequentially from the stripped value vector.
     */
    private void appendStripedLeaf(ColumnChunkWriter writer, StripedLeaf sl) {
        int maxDef = sl.maxDefLevel();
        int[] defLevels = sl.defLevels();
        int[] repLevels = sl.repLevels();
        int valueIndex = 0;
        int levelCount = defLevels.length;
        for (int row = 0; row < levelCount; row++) {
            int def = defLevels[row];
            int rep = repLevels[row];
            if (def == maxDef) {
                appendValueAt(writer, sl.values(), valueIndex, rep, def);
                valueIndex++;
            } else {
                writer.appendNull(rep, def);
            }
        }
    }

    /** Dispatches one value from {@code values} at {@code valueIndex} to the appropriate typed append on the writer. */
    private void appendValueAt(ColumnChunkWriter writer, ColumnVector values, int valueIndex, int rep, int def) {
        switch (values) {
            case io.tileverse.parquetry.columnar.IntVector iv -> writer.appendInt(iv.valueAt(valueIndex), rep, def);
            case io.tileverse.parquetry.columnar.LongVector lv -> writer.appendLong(lv.valueAt(valueIndex), rep, def);
            case io.tileverse.parquetry.columnar.FloatVector fv -> writer.appendFloat(fv.valueAt(valueIndex), rep, def);
            case io.tileverse.parquetry.columnar.DoubleVector dv ->
                writer.appendDouble(dv.valueAt(valueIndex), rep, def);
            case io.tileverse.parquetry.columnar.BooleanVector bv ->
                writer.appendBoolean(bv.valueAt(valueIndex), rep, def);
            case io.tileverse.parquetry.columnar.BinaryVector binv ->
                writer.appendBinary(binv.get(valueIndex), rep, def);
            case io.tileverse.parquetry.columnar.FixedLenBinaryVector flbv ->
                writer.appendFixedLenBinary(flbv.get(valueIndex), rep, def);
            default ->
                throw new ParquetWriteException("Unsupported value vector type for nested leaf: "
                        + values.getClass().getSimpleName());
        }
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
                throw new ParquetWriteException("Leaf path " + path.dot() + " resolved to a group node");
            }
            boolean nested = needsSchemaDerivedLevels(path, leaf);
            bindings.add(openLeafBinding(options, schema, tempDir, path, leaf, nested));
        }
        return bindings;
    }

    private static LeafBinding openLeafBinding(
            WriteOptions options,
            ParquetSchema schema,
            Path tempDir,
            ColumnPath path,
            SchemaNode.Primitive leaf,
            boolean nested) {
        try {
            Path tempFile = Files.createTempFile(tempDir, "rgw-" + safeFileName(path) + "-", ".tmp");
            ColumnChunkWriter writer = openLeafWriter(options, schema, leaf, path, tempFile, nested);
            Compression compression = resolveCompression(options, leaf.name());
            boolean requiresStriping = requiresStriping(schema, path, leaf);
            return new LeafBinding(path, leaf, writer, tempFile, compression, nested, requiresStriping);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to open column writer for " + path.dot(), e);
        }
    }

    @SuppressWarnings(
            "java:S125") // explanatory comment describing why the full path is required; not commented-out code
    private static ColumnChunkWriter openLeafWriter(
            WriteOptions options,
            ParquetSchema schema,
            SchemaNode.Primitive leaf,
            ColumnPath path,
            Path tempFile,
            boolean nested)
            throws IOException {
        if (!nested) {
            return new ColumnChunkWriter(options, leaf, tempFile);
        }
        // For nested leaves the definition level is the count of optional ancestors plus the leaf's own optionality;
        // this cannot be derived from the leaf node alone and must be computed from the full path.
        LevelMaxima levelMaxima = schema.maxLevels(path);
        return new ColumnChunkWriter(options, leaf, tempFile, levelMaxima);
    }

    /**
     * Whether the leaf's repetition and definition level maxima must come from the full schema path rather than the
     * leaf's own repetition field. A flat REQUIRED / OPTIONAL leaf (single path part, never repeated) derives both from
     * the leaf node alone. Any leaf nested under a group, or any repeated leaf, accumulates levels across enclosing
     * OPTIONAL and REPEATED ancestors and is fed through the Dremel striper.
     */
    private static boolean needsSchemaDerivedLevels(ColumnPath path, SchemaNode.Primitive leaf) {
        return path.numParts() > 1 || leaf.repetition() == Repetition.REPEATED;
    }

    /**
     * Whether a leaf must be authored through the Dremel striper rather than the bulk flat-vector path. The flat path
     * encodes only a leaf's own optionality; a leaf with an OPTIONAL or REPEATED ancestor accumulates definition or
     * repetition levels that path cannot express and must be shredded from its enclosing struct / list / map vector
     * instead. A flat leaf, or a leaf nested only under REQUIRED groups, writes correctly through the bulk path.
     */
    private static boolean requiresStriping(ParquetSchema schema, ColumnPath path, SchemaNode.Primitive leaf) {
        LevelMaxima levels = schema.maxLevels(path);
        int leafOwnDefinitionLevel = leaf.repetition() == Repetition.REQUIRED ? 0 : 1;
        return levels.maxRepetitionLevel() > 0 || levels.maxDefinitionLevel() > leafOwnDefinitionLevel;
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
     * flush-time iteration and close-time cleanup share the same state. {@code nested} is true when the leaf is inside
     * a struct group (path depth > 1); nested leaves are fed via the Dremel striper, not the bulk vector path.
     */
    private record LeafBinding(
            ColumnPath path,
            SchemaNode.Primitive leaf,
            ColumnChunkWriter writer,
            Path tempFile,
            Compression compression,
            boolean nested,
            boolean requiresStriping) {}
}
