/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SequencedSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.stream.Stream;

import io.tileverse.parquetry.columnar.BatchMaterializer;
import io.tileverse.parquetry.columnar.BatchRows;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.RowGroupOutcome;
import io.tileverse.parquetry.filter.explain.RowGroupPlan;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.filter.FilterPipeline;
import io.tileverse.parquetry.internal.filter.FilterPipeline.BloomFilterLookup;
import io.tileverse.parquetry.internal.filter.FilterPipeline.ColumnPageStatsLookup;
import io.tileverse.parquetry.internal.filter.FilterPipeline.ColumnStatsLookup;
import io.tileverse.parquetry.internal.filter.spatial.BoundsAccumulator;
import io.tileverse.parquetry.internal.filter.spatial.SpatialBoundsSource;
import io.tileverse.parquetry.internal.filter.spatial.SpatialCoveringRewrite;
import io.tileverse.parquetry.internal.read.BatchForm;
import io.tileverse.parquetry.internal.read.BatchPipeline;
import io.tileverse.parquetry.internal.read.DecryptionKeyRetriever;
import io.tileverse.parquetry.internal.read.IndexSectionLoader;
import io.tileverse.parquetry.internal.read.LateMaterialization;
import io.tileverse.parquetry.internal.read.ParallelDecodeCoordinator;
import io.tileverse.parquetry.internal.read.ParallelDecodeCoordinator.DecodeObservation;
import io.tileverse.parquetry.internal.read.RowGroupChunks;
import io.tileverse.parquetry.internal.read.RowGroupGate;
import io.tileverse.parquetry.internal.read.RowGroupSurvivor;
import io.tileverse.parquetry.internal.read.RowMask;
import io.tileverse.parquetry.internal.read.RowPositionColumn;
import io.tileverse.parquetry.internal.read.RowPositionSynthesis;
import io.tileverse.parquetry.internal.read.SpatialDecimationGate;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.observe.FetchPurpose;
import io.tileverse.parquetry.observe.FetchStats;
import io.tileverse.parquetry.observe.QueryObserver;
import io.tileverse.parquetry.observe.QueryStarted;
import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.observe.QueryStatsCollector;
import io.tileverse.parquetry.observe.RowGroupRead;
import io.tileverse.parquetry.observe.SpillAccumulator;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.schema.geo.geoparquet.GeometryColumns;

import lombok.NonNull;

/**
 * Read entry point for a single Parquet file. Opens the footer once via {@link #open(ByteRangeSource)}, caches the
 * decoded {@link FileMetaData}, the parquetry {@link ParquetSchema}, and the collapsed key/value metadata, and exposes
 * {@link #read read}, {@link #readBatches readBatches}, and {@link #explain explain} entry points that share that one
 * footer.
 *
 * <p>Instances are safe to share across threads. Cached footer, schema, and metadata are immutable; every
 * {@code read()} call allocates its own filter survivors, row-group prefetcher, and batch pipeline. The underlying
 * {@link ByteRangeSource} must itself be thread-safe; the reader does not own it, and the caller closes it after the
 * last returned stream has been closed.
 */
public final class ParquetFileReader {

    /**
     * Drives the analyze drain's decode without producing per-row output: it returns one shared constant for every row,
     * allocating nothing. The drain consumes the stream only to force the projected columns to decode and to run the
     * record-level filter, then discards the result.
     */
    private static final Materializer<Boolean> DISCARD_MATERIALIZER = (_, _) -> Boolean.TRUE;

    private final ByteRangeSource source;
    private final FileMetaData footer;
    private final ParquetSchema fileSchema;
    private final Map<String, String> keyValueMetadata;
    private final List<RowGroupSummary> rowGroupView;
    private final Optional<GeoParquetMetadata> geoMetadata;
    private final ParquetRuntime runtime;
    private final ReadResources readResources;
    private final SpatialReadGates spatialReadGates;

    // Held for an encrypted file; the footer-decryption path that reads it is not wired in yet.
    @SuppressWarnings("java:S1068")
    private final Optional<DecryptionKeyRetriever> decryptionKeyRetriever;

    private ParquetFileReader(
            ByteRangeSource source,
            FileMetaData footer,
            ParquetSchema fileSchema,
            Map<String, String> keyValueMetadata,
            List<RowGroupSummary> rowGroupView,
            ParquetRuntime runtime,
            Optional<DecryptionKeyRetriever> decryptionKeyRetriever) {
        this.source = source;
        this.footer = footer;
        this.fileSchema = fileSchema;
        this.keyValueMetadata = keyValueMetadata;
        this.rowGroupView = rowGroupView;
        this.geoMetadata = FooterModel.parseGeoMetadata(keyValueMetadata);
        this.runtime = runtime;
        this.readResources = new ReadResources(source, fileSchema, runtime);
        this.spatialReadGates = new SpatialReadGates(footer, fileSchema, geoMetadata);
        this.decryptionKeyRetriever = decryptionKeyRetriever;
    }

    /**
     * Opens a reader against the default runtime, performing exactly one footer read against {@code source}. Equivalent
     * to {@code open(source, ParquetRuntime.defaultRuntime(), Optional.empty())}.
     */
    public static ParquetFileReader open(@NonNull ByteRangeSource source) {
        return open(source, ParquetRuntime.defaultRuntime(), Optional.empty());
    }

    /**
     * Opens a reader bound to {@code runtime} for read resources, performing exactly one footer read against
     * {@code source}. The {@code decryptionKeyRetriever} is held for an encrypted file.
     */
    public static ParquetFileReader open(
            @NonNull ByteRangeSource source,
            @NonNull ParquetRuntime runtime,
            @NonNull Optional<DecryptionKeyRetriever> decryptionKeyRetriever) {
        FileMetaData footer = ParquetFormat.readFooter(source);
        Map<String, String> kvMetadata = FooterModel.collapseKeyValueMetadata(footer.keyValueMetadata());
        ParquetSchema fileSchema = FooterModel.buildFileSchema(footer, kvMetadata);
        List<RowGroupSummary> rgView = FooterModel.toRowGroupView(footer);
        return new ParquetFileReader(source, footer, fileSchema, kvMetadata, rgView, runtime, decryptionKeyRetriever);
    }

    /** Returns the file schema, with GeoParquet 1.x logical-type annotations folded in. */
    public ParquetSchema schema() {
        return fileSchema;
    }

    /** Returns the file's key/value metadata, collapsed to the last value per key. */
    public Map<String, String> keyValueMetadata() {
        return keyValueMetadata;
    }

    /** Returns a public view of the row groups in file order. */
    public List<RowGroupSummary> rowGroups() {
        return rowGroupView;
    }

    /**
     * The footer-aggregated prunable statistics for this file: per-column min/max/null-count combined across the file's
     * row groups, the geometry bounding box for each geometry column, and the record count. Reads only the cached
     * footer metadata, no page data. The result feeds {@link io.tileverse.parquetry.filter.prune.FilePruner} for
     * whole-file pruning, mirroring the Iceberg manifest-bounds path with the footer as the source.
     */
    public FileStats fileStats() {
        return new FileStatsAggregator(footer, fileSchema, geoMetadata).aggregate(rowGroupChunks());
    }

    /** Streams rows matching {@code predicate} under {@code projection}, materialized via the default record shape. */
    public Stream<ParquetRecord> read(Predicate predicate, Projection projection, ReadOptions options) {
        return read(predicate, projection, Materializer.defaultRecord(), options);
    }

    /** Streams rows matching {@code predicate} under {@code projection}, materialized via {@code materializer}. */
    public <T> Stream<T> read(
            @NonNull Predicate rawPredicate,
            @NonNull Projection projection,
            @NonNull Materializer<T> materializer,
            @NonNull ReadOptions options) {

        ReadObservation observation = ReadObservation.observe(options);
        Stream<T> rows = readRows(rawPredicate, projection, materializer, observation);
        return observation.onClose(rows);
    }

    private <T> Stream<T> readRows(
            Predicate rawPredicate, Projection projection, Materializer<T> materializer, ReadObservation observation) {

        if (needsShaping(projection)) {
            return shapedRows(rawPredicate, projection, materializer, observation);
        }
        return buildRowStream(
                rawPredicate,
                projection,
                materializer,
                observation.effectiveOptions(),
                FetchAccumulator.NONE,
                observation.spillAccumulator(),
                observation::addPipelineNanos);
    }

    private static boolean needsShaping(Projection projection) {
        return projection instanceof Projection.Of of && of.needsShaping();
    }

    /**
     * A shaping read (constants, renames, widenings, or reorders) materializes through the batch produce path, then
     * flattens each produced batch to rows. The optimized {@link #buildRowStream} late-materialization path applies
     * only to a plain physical passthrough.
     */
    private <T> Stream<T> shapedRows(
            Predicate rawPredicate, Projection projection, Materializer<T> materializer, ReadObservation observation) {
        Stream<ParquetRecordBatch> produced =
                readBatchesLowered(rawPredicate, projection, BatchMaterializer.defaultBatch(), observation);
        return BatchRows.rows(produced, materializer);
    }

    /**
     * Builds the per-row decode stream shared by {@link #read} and the analyze drain. The {@code accumulator} tallies
     * the index-section and page fetches; {@code pipelineNanosSink} receives the filter-pipeline time when the observer
     * opted into timings. The {@code read} path passes {@link FetchAccumulator#NONE}, which keeps the decode path
     * byte-identical to the no-accounting wiring.
     */
    private <T> Stream<T> buildRowStream(
            Predicate rawPredicate,
            Projection projection,
            Materializer<T> materializer,
            ReadOptions options,
            FetchAccumulator accumulator,
            SpillAccumulator spillAccumulator,
            LongConsumer pipelineNanosSink) {

        Predicate predicate = lowerSpatialPredicates(rawPredicate);
        List<RowPositionColumn> rowPositionRequests = rowPositionColumns(predicate);
        boolean synthesizeRowPosition = !rowPositionRequests.isEmpty();
        boolean recordLevel = options.useRecordLevelFilter() || synthesizeRowPosition;
        ProjectionPlan projectionPlan =
                ProjectionPlan.resolve(fileSchema, projection, recordLevel ? predicate : Predicate.ALWAYS_TRUE);
        Projection scanProjection = projectionPlan.scanProjection();
        List<RowGroupChunks> rowGroupChunks = rowGroupChunks(accumulator);
        boolean observe = observing(options);
        boolean wantsTimings = observe && options.queryObserver().wantsTimings();
        ExplainPlan plan = timedFilterPipeline(
                predicate, scanProjection, options, rowGroupChunks, wantsTimings, pipelineNanosSink);
        List<RowGroupSurvivor> survivors = survivorsFor(plan, rowGroupChunks);
        ParquetSchema scanSchema = plan.projectedSchema();
        ParquetSchema outputSchema = projectionPlan.physicalOutputSchema();
        List<Optional<RowMask>> decodeMasks = decodeMasksFor(survivors, scanSchema, options);
        Predicate normalized = plan.normalizedPredicate();
        Optional<LateMaterialization> lateMat = synthesizeRowPosition
                ? Optional.empty()
                : lateMaterializationFor(survivors, scanSchema, outputSchema, normalized, options, recordLevel);
        Predicate recordFilter = (recordLevel && lateMat.isEmpty()) ? recordFilterOf(normalized) : null;
        DecodeObservation decodeObservation =
                ReadObservation.decodeObservationFor(plan, observe, options.queryObserver(), false, spillAccumulator);
        List<RowPositionSynthesis> rowPositions =
                rowPositionSynthesesFor(survivors, rowGroupChunks, rowPositionRequests);
        Optional<RowGroupGate> rowGroupGate = spatialReadGates.rowGroupGate(survivors, options);
        ParallelDecodeCoordinator coordinator = readResources.newDecodeCoordinator(
                survivors,
                scanSchema,
                decodeMasks,
                options,
                lateMat,
                rowsForm(recordFilter),
                accumulator,
                decodeObservation,
                rowPositions,
                rowGroupGate);
        Optional<SpatialDecimationGate> leafGate = spatialReadGates.leafGate(options);
        return BatchPipeline.rows(
                coordinator, materializer, outputSchema, recordFilter, observe, wantsTimings, leafGate);
    }

    /** True when an observer is attached; the read paths skip every observability allocation when it is false. */
    private static boolean observing(ReadOptions options) {
        return options.queryObserver() != QueryObserver.NONE;
    }

    /**
     * Picks the batch form for a streaming row read from whether a per-row filter will run. The levels form defers
     * Dremel assembly to a lazy row view, which lets a filtered read skip materializing the cells of rows it discards;
     * the assembled form prepays that assembly on idle decode workers, which an unfiltered full scan reads faster than
     * it would pay for lazy navigation on the consumer's critical path. The condition must match exactly when
     * {@link BatchPipeline#rows} receives a non-null {@code recordFilter}.
     */
    static BatchForm rowsForm(Predicate recordFilter) {
        return recordFilter == null ? BatchForm.ASSEMBLED : BatchForm.LEVELS;
    }

    /**
     * Decides whether the read can decode output columns only for predicate-matching rows, and packages the per-row-
     * group inputs when it can. Returns empty (whole-read fallback to full decode plus post-decode record filtering)
     * unless every condition holds:
     *
     * <ul>
     *   <li>record-level filtering is on,
     *   <li>every scanned leaf is flat (the two-phase reader handles flat columns only),
     *   <li>the predicate is not trivially true (there is something to evaluate),
     *   <li>the predicate references at least one column, and
     *   <li>every survivor has an offset index for every output leaf (phase two needs it to skip-decode).
     * </ul>
     */
    private Optional<LateMaterialization> lateMaterializationFor(
            List<RowGroupSurvivor> survivors,
            ParquetSchema scanSchema,
            ParquetSchema outputSchema,
            Predicate normalized,
            ReadOptions options,
            boolean recordLevel) {
        if (!options.useLateMaterialization()) {
            return Optional.empty();
        }
        if (!recordLevel) {
            return Optional.empty();
        }
        if (!allFlat(fileSchema, scanSchema.leafColumns())) {
            return Optional.empty();
        }
        if (recordFilterOf(normalized) == null) {
            return Optional.empty();
        }
        Set<ColumnPath> predicateLeaves = Predicate.columns(normalized);
        if (predicateLeaves.isEmpty()) {
            return Optional.empty();
        }
        List<ColumnPath> outputLeaves = outputSchema.leafColumns();
        List<LateMaterialization.PerRowGroup> perRowGroup = new ArrayList<>(survivors.size());
        for (RowGroupSurvivor survivor : survivors) {
            Optional<Map<ColumnPath, OffsetIndex>> outputOffsetIndexes = outputOffsetIndexesFor(survivor, outputLeaves);
            if (outputOffsetIndexes.isEmpty()) {
                return Optional.empty();
            }
            perRowGroup.add(new LateMaterialization.PerRowGroup(
                    outputOffsetIndexes.orElseThrow(), survivor.chunks().numRows()));
        }
        return Optional.of(new LateMaterialization(normalized, predicateLeaves, outputSchema, perRowGroup));
    }

    private Optional<Map<ColumnPath, OffsetIndex>> outputOffsetIndexesFor(
            RowGroupSurvivor survivor, List<ColumnPath> outputLeaves) {
        Map<ColumnPath, OffsetIndex> offsetIndexes = LinkedHashMap.newLinkedHashMap(outputLeaves.size());
        for (ColumnPath leaf : outputLeaves) {
            Optional<OffsetIndex> offsetIndex = survivor.chunks().offsetIndex(leaf);
            if (offsetIndex.isEmpty()) {
                return Optional.empty();
            }
            offsetIndexes.put(leaf, offsetIndex.orElseThrow());
        }
        return Optional.of(offsetIndexes);
    }

    /**
     * The physical scan projection for {@code count}: the predicate's physical columns minus the synthesized
     * row-position columns, or a cheap driving leaf when the predicate names only synthesized columns. count produces
     * nothing, hence it needs only the columns the predicate evaluates plus a column to enumerate the file's rows.
     */
    private Projection predicateScanProjection(Predicate predicate) {
        Set<ColumnPath> columns = physicalColumns(predicate);
        if (columns.isEmpty()) {
            columns = Set.of(fileSchema.leafColumns().get(0));
        }
        return Projection.ofPhysical(columns);
    }

    /** The predicate's physical leaf columns: every column it references minus the synthesized row-position columns. */
    private static Set<ColumnPath> physicalColumns(Predicate predicate) {
        Set<ColumnPath> columns = new LinkedHashSet<>(Predicate.columns(predicate));
        Predicate.rowPositionColumns(predicate).forEach(columns::remove);
        return columns;
    }

    /**
     * The caller-named row-position columns the predicate references, after rejecting any the reader cannot synthesize.
     * Empty when the predicate has no positional delete. Validating here, before the read fans out, lets {@link #read},
     * {@link #readBatches}, and {@link #count} reject a bad name uniformly.
     */
    private List<ColumnPath> rowPositionColumnsValidated(Predicate predicate) {
        List<ColumnPath> columns = Predicate.rowPositionColumns(predicate);
        for (ColumnPath column : columns) {
            validateRowPositionColumn(column);
        }
        return columns;
    }

    /**
     * The row-position columns to synthesize for a count or plain passthrough read: every column the predicate
     * references positionally, each presented as a within-file position (a {@code firstRowId} of 0). This is the
     * shaping form with no produce set, since {@link Projection#ALL} adds no row-position outputs.
     */
    private List<RowPositionColumn> rowPositionColumns(Predicate predicate) {
        return rowPositionColumns(predicate, Projection.ALL);
    }

    /**
     * The row-position columns to synthesize for a shaping read: the predicate's positional columns (within-file
     * positions) unioned with the produce set's {@link Projection.Column.RowPosition} outputs (each offset by its own
     * {@code firstRowId}). A name shared by a positional-delete predicate and a row-position output is synthesized
     * once; the two must agree on the offset.
     */
    private List<RowPositionColumn> rowPositionColumns(Predicate predicate, Projection projection) {
        LinkedHashMap<ColumnPath, RowPositionColumn> byName = new LinkedHashMap<>();
        for (ColumnPath column : rowPositionColumnsValidated(predicate)) {
            byName.putIfAbsent(column, RowPositionColumn.position(column, 0L));
        }
        if (projection instanceof Projection.Of(SequencedSet<Projection.Column> columns)) {
            for (Projection.Column column : columns) {
                addSynthesizedColumn(byName, column);
            }
        }
        return new ArrayList<>(byName.values());
    }

    private void addSynthesizedColumn(Map<ColumnPath, RowPositionColumn> byName, Projection.Column column) {
        switch (column) {
            case Projection.Column.RowPosition(ColumnPath name, long firstRowId) ->
                addRowPositionOutput(byName, name, firstRowId);
            case Projection.Column.Coalesce(
                    ColumnPath name,
                    ColumnPath sourceColumn,
                    Projection.Column.Coalesce.Fallback f) -> byName.put(name, coalesceColumn(name, sourceColumn, f));
            default -> {
                // physical, constant, null, and promoted columns synthesize nothing
            }
        }
    }

    /**
     * A coalesce column presented under its source name is exempt from {@link #validateRowPositionColumn}: its name
     * deliberately equals a physical leaf (the materialized lineage column it coalesces with), which the row-position
     * rule forbids for a pure synthesized column. A renamed coalesce presents a new output column and its name must
     * pass the same rule.
     */
    private RowPositionColumn coalesceColumn(
            ColumnPath name, ColumnPath sourceColumn, Projection.Column.Coalesce.Fallback fallback) {
        if (!name.equals(sourceColumn)) {
            validateRowPositionColumn(name);
        }
        return switch (fallback) {
            case Projection.Column.Coalesce.Fallback.Position(long firstRowId) ->
                RowPositionColumn.coalesceWithPosition(name, sourceColumn, firstRowId);
            case Projection.Column.Coalesce.Fallback.Constant(Value value) ->
                RowPositionColumn.coalesceWithConstant(name, sourceColumn, longValue(value));
        };
    }

    private static long longValue(Value value) {
        if (value instanceof Value.LongVal(long v)) {
            return v;
        }
        throw new ParquetSchemaException("A coalesce constant fallback must be a long value, got " + value);
    }

    private void addRowPositionOutput(Map<ColumnPath, RowPositionColumn> byName, ColumnPath name, long firstRowId) {
        validateRowPositionColumn(name);
        RowPositionColumn previous = byName.putIfAbsent(name, RowPositionColumn.position(name, firstRowId));
        if (previous != null && previous.firstRowId() != firstRowId) {
            throw new ParquetSchemaException("Conflicting first row id for the row-position column " + name.dot() + ": "
                    + previous.firstRowId() + " and " + firstRowId);
        }
    }

    /**
     * Rejects a caller-named synthesized column (a row position, or a renamed coalesce's presented name) the reader
     * cannot produce: a nested path (the synthesized column is always a top-level leaf), or a name a physical column of
     * the file already has (the produced column cannot coexist with a real column at that path). The caller must pick a
     * free top-level name (Iceberg uses {@code _pos}).
     */
    private void validateRowPositionColumn(ColumnPath column) {
        if (column.numParts() > 1) {
            throw new ParquetSchemaException(
                    "A synthesized column must be a top-level name, not the nested path " + column.dot());
        }
        if (fileSchema.find(column).isPresent()) {
            throw new ParquetSchemaException("Cannot synthesize the " + column.dot()
                    + " column: the file already has a physical column with that name");
        }
    }

    /**
     * Rewrites GeoParquet bbox-relation leaves into equivalent comparisons on the geometry column's covering columns
     * when this file has covering metadata, letting the numeric stats and column-index tiers prune without decoding the
     * geometry. A file without covering keeps its spatial leaf for the record-level WKB path.
     */
    private Predicate lowerSpatialPredicates(Predicate predicate) {
        return SpatialCoveringRewrite.expand(predicate, fileSchema, geoMetadata);
    }

    /** Returns the per-row filter, or {@code null} when the predicate is trivially true (nothing to evaluate). */
    private static Predicate recordFilterOf(Predicate normalized) {
        if (normalized instanceof Predicate.Always(boolean value) && value) {
            return null;
        }
        return normalized;
    }

    /**
     * Counts rows matching {@code predicate} with no record materialization. Each row group routes by its filter-
     * pipeline outcome: an eliminated group adds nothing, a MATCHED group (statistics proved every row matches) adds
     * its row count with no decode, and a FULL or PARTIAL group decodes the predicate columns only and counts the
     * matches via a columnar popcount. {@link Predicate#ALWAYS_TRUE} and {@link Predicate#ALWAYS_FALSE} short-circuit
     * from metadata without touching the pipeline.
     */
    public long count(@NonNull Predicate rawPredicate, @NonNull ReadOptions options) {
        Predicate predicate = lowerSpatialPredicates(rawPredicate);
        // ALWAYS_TRUE / ALWAYS_FALSE are literals callers pass directly; fold them without running the pipeline.
        if (predicate instanceof Predicate.Always(boolean value)) {
            return value ? totalRows() : 0L;
        }
        ReadObservation observation = ReadObservation.observe(options);
        try {
            return countLowered(
                    predicate,
                    observation.effectiveOptions(),
                    FetchAccumulator.NONE,
                    observation.spillAccumulator(),
                    observation::addPipelineNanos);
        } finally {
            observation.fireFinishedIfObserving();
        }
    }

    /**
     * Counts the rows matching {@code predicate} (already spatial-lowered, never an {@link Predicate.Always} literal),
     * threading {@code accumulator} through the index-section reads and the residual page fetches. The public
     * {@link #count} passes {@link FetchAccumulator#NONE}. {@code pipelineNanosSink} receives the measured
     * filter-pipeline time when the observer opted into timings; it is never called otherwise.
     */
    private long countLowered(
            Predicate predicate,
            ReadOptions options,
            FetchAccumulator accumulator,
            SpillAccumulator spillAccumulator,
            LongConsumer pipelineNanosSink) {
        List<RowPositionColumn> rowPositionRequests = rowPositionColumns(predicate);
        Projection predicateProjection = predicateScanProjection(predicate);
        List<RowGroupChunks> rowGroupChunks = rowGroupChunks(accumulator);
        boolean observe = observing(options);
        boolean wantsTimings = observe && options.queryObserver().wantsTimings();
        ExplainPlan plan = timedFilterPipeline(
                predicate, predicateProjection, options, rowGroupChunks, wantsTimings, pipelineNanosSink);

        long matchedRows = matchedRowCount(plan, observe, options.queryObserver());
        List<RowGroupSurvivor> residual = residualSurvivors(plan, rowGroupChunks);
        if (residual.isEmpty()) {
            return matchedRows;
        }
        List<RowPositionSynthesis> rowPositions =
                rowPositionSynthesesFor(residual, rowGroupChunks, rowPositionRequests);
        return matchedRows
                + countResidual(residual, plan, options, accumulator, spillAccumulator, observe, rowPositions);
    }

    /**
     * Sum of row counts for the MATCHED row groups, added without any decode. When observing, each MATCHED group also
     * emits one {@link RowGroupRead} here: it never becomes a decoded row group, hence the decode-side emission would
     * otherwise miss it. Its rows are all decoded-equivalent and all matched; it decodes no pages.
     */
    private static long matchedRowCount(ExplainPlan plan, boolean observe, QueryObserver observer) {
        long matchedRows = 0L;
        for (RowGroupPlan rgPlan : plan.rowGroups()) {
            if (rgPlan.outcome() == RowGroupOutcome.MATCHED) {
                matchedRows += rgPlan.rowCount();
                if (observe) {
                    observer.onRowGroupRead(matchedRowGroupRead(rgPlan));
                }
            }
        }
        return matchedRows;
    }

    /** The read event for a MATCHED row group: every row matched, none decoded through a page. */
    private static RowGroupRead matchedRowGroupRead(RowGroupPlan rgPlan) {
        return new RowGroupRead(
                rgPlan.index(), rgPlan.rowCount(), rgPlan.rowCount(), 0, 0, FetchStats.EMPTY, Optional.empty());
    }

    /**
     * The FULL and PARTIAL row groups, the only ones count still has to decode. ELIMINATED groups add nothing and
     * MATCHED groups are added from metadata, hence both are excluded here.
     */
    private List<RowGroupSurvivor> residualSurvivors(ExplainPlan plan, List<RowGroupChunks> rowGroupChunks) {
        List<RowGroupSurvivor> residual = new ArrayList<>();
        for (RowGroupPlan rgPlan : plan.rowGroups()) {
            RowGroupChunks chunks = rowGroupChunks.get(rgPlan.index());
            switch (rgPlan.outcome()) {
                case ELIMINATED, MATCHED -> {
                    /* eliminated adds nothing; matched is added from metadata, never decoded */
                }
                case PARTIAL -> residual.add(new RowGroupSurvivor(chunks, rgPlan.survivingRows(), true));
                case FULL -> residual.add(RowGroupSurvivor.full(chunks));
            }
        }
        return residual;
    }

    /** Decodes the predicate columns of the residual row groups and counts the matches via a columnar popcount. */
    private long countResidual(
            List<RowGroupSurvivor> residual,
            ExplainPlan plan,
            ReadOptions options,
            FetchAccumulator accumulator,
            SpillAccumulator spillAccumulator,
            boolean observe,
            List<RowPositionSynthesis> rowPositions) {
        ParquetSchema scanSchema = plan.projectedSchema();
        Predicate normalized = plan.normalizedPredicate();
        List<Optional<RowMask>> masks = decodeMasksFor(residual, scanSchema, options);
        DecodeObservation observation =
                ReadObservation.residualObservationFor(plan, observe, options.queryObserver(), spillAccumulator);
        Optional<RowGroupGate> rowGroupGate = spatialReadGates.rowGroupGate(residual, options);
        ParallelDecodeCoordinator coordinator = readResources.newDecodeCoordinator(
                residual,
                scanSchema,
                masks,
                options,
                Optional.empty(),
                BatchForm.LEVELS,
                accumulator,
                observation,
                rowPositions,
                rowGroupGate);
        Optional<SpatialDecimationGate> leafGate = spatialReadGates.leafGate(options);
        return BatchPipeline.countMatching(coordinator, normalized, observe, leafGate);
    }

    /** Total rows across every row group, read from the per-row-group summaries with no I/O. */
    private long totalRows() {
        long sum = 0L;
        for (RowGroupSummary rowGroup : rowGroupView) {
            sum += rowGroup.rowCount();
        }
        return sum;
    }

    /**
     * The exact bounding box of the primary geometry column over the rows matching {@code rawPredicate}, or empty when
     * the file has no geometry column or no row matches. The box is 2D-exact; its Z and M extents are present only when
     * the whole answer came from metadata boxes, and any scanned row drops them.
     *
     * <p>The box is exact relative to the file's declared geometry statistics, which are trusted as tight; a writer
     * that declared rounded boxes widens the answer accordingly.
     *
     * <p>Cost mirrors {@link #count}: an eliminated row group contributes nothing, a row group whose statistics prove
     * every row matches unions its tight geometry box without decoding, and the rest decode only the geometry and the
     * predicate columns to fold their matching rows' WKB envelopes. A row group whose conservative geometry box the
     * accumulated bounds already cover is skipped before any fetch. A conservative box only ever justifies skipping,
     * while the tight box of an all-match row group is unioned directly.
     *
     * <p>An unfiltered call returns the tight file-level metadata box when the file records one, visiting no row group.
     */
    public Optional<BoundingBox> bounds(@NonNull Predicate rawPredicate, @NonNull ReadOptions options) {
        Optional<ColumnPath> geometryColumn = primaryGeometryColumn();
        if (geometryColumn.isEmpty()) {
            return Optional.empty();
        }
        ColumnPath geometry = geometryColumn.orElseThrow();
        Predicate predicate = lowerSpatialPredicates(rawPredicate);
        if (predicate instanceof Predicate.Always(boolean value)) {
            return value ? unfilteredBounds(geometry, options) : Optional.empty();
        }
        return boundsLowered(predicate, geometry, options);
    }

    /**
     * The file's primary geometry column, or empty when the file exposes none. A GeoParquet {@code "geo"} metadata
     * document names its primary column directly. Without that metadata, a single footer geometry bounding box
     * identifies the column, and failing that a lone geometry logical-type leaf in the schema does.
     */
    private Optional<ColumnPath> primaryGeometryColumn() {
        Optional<ColumnPath> declared = declaredPrimaryGeometryColumn();
        if (declared.isPresent()) {
            return declared;
        }
        Optional<ColumnPath> boundedGeometry =
                fileStats().geometryBounds().keySet().stream().findFirst();
        if (boundedGeometry.isPresent()) {
            return boundedGeometry;
        }
        return soleSchemaGeometryColumn();
    }

    /**
     * The GeoParquet metadata's declared primary geometry column, when the file has {@code "geo"} metadata naming one.
     */
    private Optional<ColumnPath> declaredPrimaryGeometryColumn() {
        return geoMetadata
                .map(GeoParquetMetadata::primaryColumn)
                .filter(name -> !name.isBlank())
                .map(name -> ColumnPath.of(name.split("\\.")));
    }

    /** The file schema's sole geometry logical-type leaf, or empty when there is none or more than one. */
    private Optional<ColumnPath> soleSchemaGeometryColumn() {
        Set<ColumnPath> geometryColumns = GeometryColumns.resolve(fileSchema, geoMetadata);
        if (geometryColumns.size() != 1) {
            return Optional.empty();
        }
        return Optional.of(geometryColumns.iterator().next());
    }

    /**
     * Bounds for an unfiltered read of {@code geometryColumn}: the tight file-level metadata box when the file records
     * one (zero I/O), otherwise the union of every row group's tight metadata box with a scan of the row groups that
     * expose none.
     */
    private Optional<BoundingBox> unfilteredBounds(ColumnPath geometryColumn, ReadOptions options) {
        SpatialBoundsSource spatialBounds = SpatialBoundsSource.of(footer, fileSchema, geoMetadata);
        Optional<BoundingBox> fileBox = spatialBounds.fileBounds(geometryColumn);
        if (fileBox.isPresent()) {
            return fileBox;
        }
        return unionRowGroupBoundsAndScanTheRest(geometryColumn, spatialBounds, options);
    }

    private Optional<BoundingBox> unionRowGroupBoundsAndScanTheRest(
            ColumnPath geometryColumn, SpatialBoundsSource spatialBounds, ReadOptions options) {
        BoundsAccumulator accumulator = new BoundsAccumulator();
        List<RowGroupChunks> rowGroupChunks = rowGroupChunks();
        List<RowGroupSurvivor> boxLess = new ArrayList<>();
        for (int i = 0; i < rowGroupChunks.size(); i++) {
            Optional<BoundingBox> box = spatialBounds.rowGroupBounds(geometryColumn, i);
            if (box.isPresent()) {
                accumulator.union(box.orElseThrow());
            } else {
                boxLess.add(RowGroupSurvivor.full(rowGroupChunks.get(i)));
            }
        }
        if (!boxLess.isEmpty()) {
            ParquetSchema scanSchema = fileSchema.project(Set.of(geometryColumn));
            // An unfiltered box-less scan runs a null predicate, hence it references no row-position column.
            ParallelDecodeCoordinator coordinator =
                    boundsDecodeCoordinator(boxLess, scanSchema, options, DecodeObservation.NONE, List.of());
            BatchPipeline.boundsMatching(coordinator, null, geometryColumn, accumulator);
        }
        return accumulator.snapshot();
    }

    /**
     * Bounds for a filtered read: run the filter pipeline, union the tight box of every all-match row group, then scan
     * the residue - skipping any row group the accumulated bounds already cover - to fold matching rows' envelopes.
     * Threads {@link ReadObservation} as {@link #count} does, letting the residual decode emit the same per-row-group
     * read events.
     */
    private Optional<BoundingBox> boundsLowered(
            Predicate predicate, ColumnPath geometryColumn, ReadOptions rawOptions) {
        ReadObservation observation = ReadObservation.observe(rawOptions);
        try {
            return foldFilteredBounds(predicate, geometryColumn, observation);
        } finally {
            observation.fireFinishedIfObserving();
        }
    }

    private Optional<BoundingBox> foldFilteredBounds(
            Predicate predicate, ColumnPath geometryColumn, ReadObservation observation) {
        ReadOptions options = observation.effectiveOptions();
        BoundsAccumulator accumulator = new BoundsAccumulator();
        SpatialBoundsSource spatialBounds = SpatialBoundsSource.of(footer, fileSchema, geoMetadata);
        List<RowGroupChunks> rowGroupChunks = rowGroupChunks();
        List<RowPositionColumn> rowPositionRequests = rowPositionColumns(predicate);
        boolean observe = observing(options);
        boolean wantsTimings = observe && options.queryObserver().wantsTimings();
        ExplainPlan plan = timedFilterPipeline(
                predicate,
                boundsScanProjection(predicate, geometryColumn),
                options,
                rowGroupChunks,
                wantsTimings,
                observation::addPipelineNanos);
        List<ResidualGroup> residual =
                unionMatchedAndCollectResidual(plan, rowGroupChunks, spatialBounds, geometryColumn, accumulator);
        scanResidualBounds(
                residual,
                plan,
                geometryColumn,
                options,
                observation.spillAccumulator(),
                observe,
                accumulator,
                rowGroupChunks,
                rowPositionRequests);
        return accumulator.snapshot();
    }

    /** The physical scan projection for bounds: the predicate's columns plus the geometry column the residue folds. */
    private Projection boundsScanProjection(Predicate predicate, ColumnPath geometryColumn) {
        Set<ColumnPath> columns = new LinkedHashSet<>(physicalColumns(predicate));
        columns.add(geometryColumn);
        return Projection.ofPhysical(columns);
    }

    /**
     * Pass one: union the tight geometry box of every row group whose statistics prove all rows match, and collect the
     * rest (FULL, PARTIAL, and box-less all-match groups) for the residual scan. A box-less all-match group joins the
     * residual as a full survivor: all its rows match, only its extent is unknown.
     */
    private List<ResidualGroup> unionMatchedAndCollectResidual(
            ExplainPlan plan,
            List<RowGroupChunks> rowGroupChunks,
            SpatialBoundsSource spatialBounds,
            ColumnPath geometryColumn,
            BoundsAccumulator accumulator) {
        List<ResidualGroup> residual = new ArrayList<>();
        for (RowGroupPlan rgPlan : plan.rowGroups()) {
            RowGroupChunks chunks = rowGroupChunks.get(rgPlan.index());
            Optional<BoundingBox> box = spatialBounds.rowGroupBounds(geometryColumn, rgPlan.index());
            switch (rgPlan.outcome()) {
                case ELIMINATED -> {
                    /* pruned; contributes nothing */
                }
                case MATCHED -> unionOrCollectMatched(rgPlan, chunks, box, accumulator, residual);
                case FULL -> residual.add(new ResidualGroup(RowGroupSurvivor.full(chunks), rgPlan.index(), box));
                case PARTIAL ->
                    residual.add(new ResidualGroup(
                            new RowGroupSurvivor(chunks, rgPlan.survivingRows(), true), rgPlan.index(), box));
            }
        }
        return residual;
    }

    private static void unionOrCollectMatched(
            RowGroupPlan rgPlan,
            RowGroupChunks chunks,
            Optional<BoundingBox> box,
            BoundsAccumulator accumulator,
            List<ResidualGroup> residual) {
        if (box.isPresent()) {
            accumulator.union(box.orElseThrow());
        } else {
            residual.add(new ResidualGroup(RowGroupSurvivor.full(chunks), rgPlan.index(), Optional.empty()));
        }
    }

    /**
     * Pass two: scan the residual row groups largest box first, folding their matching rows' WKB envelopes. Before each
     * decode the row group's conservative box is re-checked against the accumulated bounds; a covered row group is
     * skipped without any fetch, and the growing extent lets a big early row group cover later ones. A box-less row
     * group sorts last and is never skipped.
     */
    // The arguments are the same cohesive pipeline threading the count path uses; a bundle would only rename them.
    @SuppressWarnings("java:S107")
    private void scanResidualBounds(
            List<ResidualGroup> residual,
            ExplainPlan plan,
            ColumnPath geometryColumn,
            ReadOptions options,
            SpillAccumulator spillAccumulator,
            boolean observe,
            BoundsAccumulator accumulator,
            List<RowGroupChunks> rowGroupChunks,
            List<RowPositionColumn> rowPositionRequests) {
        Predicate predicate = plan.normalizedPredicate();
        ParquetSchema scanSchema = plan.projectedSchema();
        for (ResidualGroup group : orderedByDescendingArea(residual)) {
            if (isCovered(group, accumulator)) {
                continue;
            }
            DecodeObservation observation =
                    boundsDecodeObservation(group.rowGroupIndex(), observe, options, spillAccumulator);
            scanResidualGroup(
                    group.survivor(),
                    predicate,
                    geometryColumn,
                    scanSchema,
                    options,
                    observation,
                    accumulator,
                    rowGroupChunks,
                    rowPositionRequests);
        }
    }

    // The arguments are the same cohesive pipeline threading the count path uses; a bundle would only rename them.
    @SuppressWarnings("java:S107")
    private void scanResidualGroup(
            RowGroupSurvivor survivor,
            Predicate predicate,
            ColumnPath geometryColumn,
            ParquetSchema scanSchema,
            ReadOptions options,
            DecodeObservation observation,
            BoundsAccumulator accumulator,
            List<RowGroupChunks> rowGroupChunks,
            List<RowPositionColumn> rowPositionRequests) {
        List<RowPositionSynthesis> rowPositions =
                rowPositionSynthesesFor(List.of(survivor), rowGroupChunks, rowPositionRequests);
        ParallelDecodeCoordinator coordinator =
                boundsDecodeCoordinator(List.of(survivor), scanSchema, options, observation, rowPositions);
        BatchPipeline.boundsMatching(coordinator, predicate, geometryColumn, accumulator);
    }

    /**
     * Builds a decode coordinator over {@code survivors} that decodes only {@code scanSchema}, as count's residual
     * does.
     */
    private ParallelDecodeCoordinator boundsDecodeCoordinator(
            List<RowGroupSurvivor> survivors,
            ParquetSchema scanSchema,
            ReadOptions options,
            DecodeObservation observation,
            List<RowPositionSynthesis> rowPositions) {
        List<Optional<RowMask>> masks = decodeMasksFor(survivors, scanSchema, options);
        Optional<RowGroupGate> rowGroupGate = spatialReadGates.rowGroupGate(survivors, options);
        return readResources.newDecodeCoordinator(
                survivors,
                scanSchema,
                masks,
                options,
                Optional.empty(),
                BatchForm.LEVELS,
                FetchAccumulator.NONE,
                observation,
                rowPositions,
                rowGroupGate);
    }

    /** The read event context for one residual row group: the observer and the group's true file ordinal, or none. */
    private static DecodeObservation boundsDecodeObservation(
            int rowGroupIndex, boolean observe, ReadOptions options, SpillAccumulator spillAccumulator) {
        if (!observe) {
            return DecodeObservation.NONE;
        }
        QueryObserver observer = options.queryObserver();
        return new DecodeObservation(
                observer, List.of(rowGroupIndex), false, observer.wantsTimings(), spillAccumulator);
    }

    private static List<ResidualGroup> orderedByDescendingArea(List<ResidualGroup> residual) {
        List<ResidualGroup> ordered = new ArrayList<>(residual);
        ordered.sort(Comparator.comparingDouble(ResidualGroup::area).reversed());
        return ordered;
    }

    private static boolean isCovered(ResidualGroup group, BoundsAccumulator accumulator) {
        Optional<BoundingBox> box = group.box();
        return box.isPresent() && accumulator.covers(box.orElseThrow());
    }

    /**
     * One residual row group to fold into the bounds: its materialization-ready survivor, its file ordinal (the true
     * index reported to an observer), and its conservative metadata box when the file records one. A box-less row group
     * reports a {@link Double#NEGATIVE_INFINITY} area that sorts it last, and is never containment-skipped.
     */
    private record ResidualGroup(RowGroupSurvivor survivor, int rowGroupIndex, Optional<BoundingBox> box) {

        double area() {
            if (box.isEmpty()) {
                return Double.NEGATIVE_INFINITY;
            }
            BoundingBox b = box.orElseThrow();
            return (b.xmax() - b.xmin()) * (b.ymax() - b.ymin());
        }
    }

    /**
     * Streams record batches matching {@code predicate} under {@code projection} via the default batch shape. The
     * predicate is applied exactly (when record-level filtering is enabled, the default; with it disabled only metadata
     * pruning applies): each emitted batch holds only matching rows, narrowed to {@code projection}.
     */
    public Stream<ParquetRecordBatch> readBatches(Predicate predicate, Projection projection, ReadOptions options) {
        return readBatches(predicate, projection, BatchMaterializer.defaultBatch(), options);
    }

    /**
     * Streams record batches matching {@code predicate} under {@code projection}, materialized via
     * {@code materializer}. The predicate is applied exactly (when record-level filtering is enabled, the default; with
     * it disabled only metadata pruning applies): each emitted batch holds only matching rows, narrowed to
     * {@code projection}.
     */
    public <T> Stream<T> readBatches(
            @NonNull Predicate rawPredicate,
            @NonNull Projection projection,
            @NonNull BatchMaterializer<T> materializer,
            @NonNull ReadOptions options) {

        ReadObservation observation = ReadObservation.observe(options);
        Stream<T> batches = readBatchesLowered(rawPredicate, projection, materializer, observation);
        return observation.onClose(batches);
    }

    private <T> Stream<T> readBatchesLowered(
            Predicate rawPredicate,
            Projection projection,
            BatchMaterializer<T> materializer,
            ReadObservation observation) {

        ReadOptions options = observation.effectiveOptions();
        Predicate predicate = lowerSpatialPredicates(rawPredicate);
        List<RowPositionColumn> rowPositionRequests = rowPositionColumns(predicate, projection);
        boolean synthesizeRowPosition = !rowPositionRequests.isEmpty();
        boolean recordLevel = options.useRecordLevelFilter() || synthesizeRowPosition;
        ProjectionPlan projectionPlan =
                ProjectionPlan.resolve(fileSchema, projection, recordLevel ? predicate : Predicate.ALWAYS_TRUE);
        Projection scanProjection = projectionPlan.scanProjection();
        List<RowGroupChunks> rowGroupChunks = rowGroupChunks();
        boolean observe = observing(options);
        boolean wantsTimings = observe && options.queryObserver().wantsTimings();
        ExplainPlan plan = timedFilterPipeline(
                predicate, scanProjection, options, rowGroupChunks, wantsTimings, observation::addPipelineNanos);
        List<RowGroupSurvivor> survivors = survivorsFor(plan, rowGroupChunks);
        ParquetSchema scanSchema = plan.projectedSchema();
        ParquetSchema outputSchema = projectionPlan.physicalOutputSchema();
        List<Optional<RowMask>> decodeMasks = decodeMasksFor(survivors, scanSchema, options);
        Predicate normalized = plan.normalizedPredicate();
        Optional<LateMaterialization> lateMat = synthesizeRowPosition
                ? Optional.empty()
                : lateMaterializationFor(survivors, scanSchema, outputSchema, normalized, options, recordLevel);
        Predicate recordFilter = (recordLevel && lateMat.isEmpty()) ? recordFilterOf(normalized) : null;
        DecodeObservation decodeObservation = ReadObservation.decodeObservationFor(
                plan, observe, options.queryObserver(), true, observation.spillAccumulator());
        List<RowPositionSynthesis> rowPositions =
                rowPositionSynthesesFor(survivors, rowGroupChunks, rowPositionRequests);
        Optional<RowGroupGate> rowGroupGate = spatialReadGates.rowGroupGate(survivors, options);
        ParallelDecodeCoordinator coordinator = readResources.newDecodeCoordinator(
                survivors,
                scanSchema,
                decodeMasks,
                options,
                lateMat,
                BatchForm.ASSEMBLED,
                FetchAccumulator.NONE,
                decodeObservation,
                rowPositions,
                rowGroupGate);
        Optional<SpatialDecimationGate> leafGate = spatialReadGates.leafGate(options);
        Stream<ParquetRecordBatch> batches = (recordFilter == null && leafGate.isEmpty())
                ? BatchPipeline.batches(coordinator)
                : BatchPipeline.batches(coordinator, recordFilter, outputSchema, leafGate);
        Stream<ParquetRecordBatch> produced =
                projectionPlan.needsShaping() ? batches.map(projectionPlan::produce) : batches;
        ParquetSchema producedSchema = projectionPlan.producedSchema();
        return produced.map(batch -> materializer.materialize(producedSchema, batch));
    }

    /** Runs the filter pipeline without reading data and returns the explain plan. */
    public ExplainPlan explain(
            @NonNull Predicate rawPredicate, @NonNull Projection projection, @NonNull ReadOptions options) {
        Predicate predicate = lowerSpatialPredicates(rawPredicate);
        return runFilterPipeline(predicate, projection, options, rowGroupChunks());
    }

    /**
     * Returns the explain plan annotated with the execution stats of one drain of {@code predicate} under
     * {@code projection}. The drain decodes the projected columns (plus the column-index, offset-index, and
     * bloom-filter sections the filter pipeline reads), runs the record-level filter, and discards every value; it
     * allocates no per-row output. This holds for any predicate, including an always-true full scan, whose execution
     * annotation reports the real cost of reading the projected columns.
     *
     * <p>The plan's {@link ExplainPlan#estimatedBytesRead() estimatedBytesRead} is the planner's projection-based upper
     * bound and is a different measure from the {@link QueryStats#totalFetch() totalFetch} the drain actually read.
     * Per-row-group fetch bytes are not yet attributed and read as zero; the query-level fetch total is authoritative.
     */
    public ExplainPlan explainAnalyze(
            @NonNull Predicate predicate, @NonNull Projection projection, @NonNull ReadOptions options) {
        // The drain below re-plans identically (planning is in-footer metadata work, no I/O); only its execution stats
        // are kept, never its plan. This explicit plan is the one annotated and returned.
        ExplainPlan plan = explain(predicate, projection, withoutObserver(options));

        QueryStatsCollector collector = new QueryStatsCollector();
        Map<Integer, RowGroupRead> perRowGroup = new LinkedHashMap<>();
        QueryObserver capture = capturingObserver(perRowGroup);
        QueryObserver combined = QueryObserver.composite(collector, capture, options.queryObserver());
        ReadOptions drainOptions = options.toBuilder().queryObserver(combined).build();

        AtomicLong pipelineNanos = new AtomicLong();
        FetchAccumulator accumulator = FetchAccumulator.active();
        SpillAccumulator spillAccumulator = SpillAccumulator.active();
        drainForAnalyze(predicate, projection, drainOptions, accumulator, spillAccumulator, pipelineNanos::addAndGet);
        FetchStats fetch = accumulator.snapshot();

        collector.onQueryFinished(null);
        QueryStats stats = collector.snapshot().withTotalFetch(fetch).withSpillStats(spillAccumulator.snapshot());
        if (pipelineNanos.get() > 0L) {
            stats = ReadObservation.withPipelineNanos(stats, pipelineNanos.get());
        }
        options.queryObserver().onQueryFinished(stats);

        return plan.withExecution(stats, perRowGroup);
    }

    /**
     * Drives the analyze drain: decodes the projected columns for every surviving row, runs the record-level filter,
     * and discards the values. Consuming the whole stream forces the decode and fires every per-row-group read event
     * into the {@code drainOptions} observer; the {@code accumulator} tallies the index-section and page fetches.
     */
    private void drainForAnalyze(
            Predicate predicate,
            Projection projection,
            ReadOptions drainOptions,
            FetchAccumulator accumulator,
            SpillAccumulator spillAccumulator,
            LongConsumer pipelineNanosSink) {
        try (Stream<Boolean> rows = buildRowStream(
                predicate,
                projection,
                DISCARD_MATERIALIZER,
                drainOptions,
                accumulator,
                spillAccumulator,
                pipelineNanosSink)) {
            rows.forEach(_ -> {});
        }
    }

    /**
     * The options with the user observer suppressed, used to build the plan without firing the planned-decision events
     * a second time (the drain fires them). Returns the same options unchanged when no observer is attached.
     */
    private static ReadOptions withoutObserver(ReadOptions options) {
        if (options.queryObserver() == QueryObserver.NONE) {
            return options;
        }
        return options.toBuilder().queryObserver(QueryObserver.NONE).build();
    }

    /** An observer that records each row-group read into {@code perRowGroup}, merging any duplicate by index. */
    private static QueryObserver capturingObserver(Map<Integer, RowGroupRead> perRowGroup) {
        return new QueryObserver() {
            @Override
            public void onRowGroupRead(RowGroupRead event) {
                perRowGroup.merge(event.rowGroupIndex(), event, RowGroupRead::combine);
            }
        };
    }

    /**
     * Binds index-section reads to this reader's {@link ByteRangeSource}. Index, offset-index, and bloom reads do not
     * record into a {@link FetchAccumulator}; the accumulating form lives in
     * {@link #indexSectionLoader(FetchAccumulator)}, which the read entry points use.
     */
    private IndexSectionLoader indexSectionLoader() {
        return indexSectionLoader(FetchAccumulator.NONE);
    }

    /**
     * Binds index-section reads to this reader's {@link ByteRangeSource} and records each section's bytes into
     * {@code accumulator}: a column-index read as {@link FetchPurpose#COLUMN_INDEX}, an offset-index read as
     * {@link FetchPurpose#OFFSET_INDEX}, and a bloom-filter read as {@link FetchPurpose#BLOOM_FILTER}. The recorded
     * byte count is the section's on-disk length. A bloom filter whose length the writer recorded counts the full chunk
     * (header plus bitset); one whose length is absent counts the bitset byte size discovered from the filter header,
     * the only figure cheaply available on that path. Passing {@link FetchAccumulator#NONE} reduces every record call
     * to a no-op, matching {@link #indexSectionLoader()}.
     */
    private IndexSectionLoader indexSectionLoader(FetchAccumulator accumulator) {
        return new FooterIndexSectionLoader(source, accumulator);
    }

    /**
     * Builds one {@link RowGroupChunks} per footer row group, in file order, reused across the filter and decode
     * phases. Index-section reads do not record into a {@link FetchAccumulator}; the accumulating form is
     * {@link #rowGroupChunks(FetchAccumulator)}.
     */
    private List<RowGroupChunks> rowGroupChunks() {
        return rowGroupChunks(FetchAccumulator.NONE);
    }

    /**
     * Builds one {@link RowGroupChunks} per footer row group, in file order, with index-section reads recording into
     * {@code accumulator}.
     */
    private List<RowGroupChunks> rowGroupChunks(FetchAccumulator accumulator) {
        IndexSectionLoader loader = indexSectionLoader(accumulator);
        List<RowGroup> rgs = footer.rowGroups();
        List<RowGroupChunks> chunks = new ArrayList<>(rgs.size());
        for (RowGroup rg : rgs) {
            chunks.add(RowGroupChunks.of(rg, fileSchema, loader));
        }
        return chunks;
    }

    /**
     * Runs {@link #runFilterPipeline} and, when the observer opted into timings, measures the run and reports the
     * elapsed nanoseconds to {@code pipelineNanosSink}. When timings are off, no clock is read and the sink stays
     * untouched.
     */
    private ExplainPlan timedFilterPipeline(
            Predicate predicate,
            Projection projection,
            ReadOptions options,
            List<RowGroupChunks> rowGroupChunks,
            boolean wantsTimings,
            LongConsumer pipelineNanosSink) {
        if (!wantsTimings) {
            return runFilterPipeline(predicate, projection, options, rowGroupChunks);
        }
        long start = System.nanoTime();
        ExplainPlan plan = runFilterPipeline(predicate, projection, options, rowGroupChunks);
        pipelineNanosSink.accept(System.nanoTime() - start);
        return plan;
    }

    /** Builds inputs for and runs the filter pipeline. */
    private ExplainPlan runFilterPipeline(
            Predicate predicate, Projection projection, ReadOptions options, List<RowGroupChunks> rowGroupChunks) {
        List<ColumnPath> projectedLeaves = projectedLeafColumns(projection);
        boolean rowPositionDeletes = !Predicate.rowPositionColumns(predicate).isEmpty();
        List<FilterPipeline.RowGroupInputs> inputs =
                filterInputsFor(rowGroupChunks, options, projectedLeaves, rowPositionDeletes);
        FilterPipeline.FilterToggles toggles =
                new FilterPipeline.FilterToggles(options.useStatsFilter(), options.useDictionaryFilter());
        ExplainPlan plan = FilterPipeline.evaluate(fileSchema, projection, predicate, inputs, toggles);
        notifyPlanned(plan, options.queryObserver());
        return plan;
    }

    /**
     * Replays the planning outcome to {@code observer}: one {@code onQueryStarted} for the query and one
     * {@code onRowGroupPlanned} per tier decision of every row group. The {@link QueryObserver#NONE} branch returns
     * before any allocation, keeping the read path free of observability overhead when no observer is attached.
     */
    private static void notifyPlanned(ExplainPlan plan, QueryObserver observer) {
        if (observer == QueryObserver.NONE) {
            return;
        }
        observer.onQueryStarted(new QueryStarted(
                plan.normalizedPredicate(),
                plan.projectedSchema(),
                plan.rowGroups().size()));
        for (RowGroupPlan rowGroup : plan.rowGroups()) {
            for (PruningDecision decision : rowGroup.tiers()) {
                observer.onRowGroupPlanned(rowGroup.index(), decision);
            }
        }
    }

    /**
     * Builds one {@link FilterPipeline.RowGroupInputs} per row group from the pre-built chunk views.
     *
     * @param projectedLeaves the projected leaf column paths, used to size each row group's projected compressed bytes
     * @param rowPositionDeletes whether the predicate has a positional delete; only then are each row group's file
     *     offset and page boundaries (the row-position pruning inputs) computed, which keeps them off the common path
     */
    private List<FilterPipeline.RowGroupInputs> filterInputsFor(
            List<RowGroupChunks> rowGroupChunks,
            ReadOptions options,
            List<ColumnPath> projectedLeaves,
            boolean rowPositionDeletes) {
        SpatialBoundsSource spatialBounds = SpatialBoundsSource.of(footer, fileSchema, geoMetadata);
        Map<RowGroupChunks, Long> fileOffsets = rowPositionDeletes ? rowGroupFileOffsets(rowGroupChunks) : Map.of();
        List<FilterPipeline.RowGroupInputs> inputs = new ArrayList<>(rowGroupChunks.size());
        for (RowGroupChunks chunks : rowGroupChunks) {
            inputs.add(new FilterPipeline.RowGroupInputs(
                    chunks.numRows(),
                    statsLookup(chunks),
                    spatialBounds,
                    FilterPipeline.noDictionaryLookup(),
                    pageStatsLookupFor(chunks, options),
                    bloomLookupFor(chunks, options),
                    projectedCompressedBytes(chunks, projectedLeaves),
                    rowPositionDeletes ? fileOffsets.get(chunks) : 0L,
                    rowPositionDeletes ? rowPositionPageFirstRows(chunks, options) : List.of()));
        }
        return inputs;
    }

    /**
     * The row-group-relative first-row index of each page, taken from any one leaf column's offset index, used by the
     * row-position delete tier to find fully-deleted pages. The synthesized {@code $pos} column has no offset index of
     * its own; every column in a row group shares the same row boundaries, hence any real leaf's page first-row indexes
     * partition the row group into spans the position math can reason about. Returns an empty list when the
     * column-index filter is off or no leaf exposes an offset index, in which case the page tier stays inert.
     */
    private List<Long> rowPositionPageFirstRows(RowGroupChunks chunks, ReadOptions options) {
        if (!options.useColumnIndexFilter()) {
            return List.of();
        }
        for (ColumnPath leaf : fileSchema.leafColumns()) {
            Optional<OffsetIndex> offsetIndex = chunks.offsetIndex(leaf);
            if (offsetIndex.isPresent()) {
                return pageFirstRowIndexes(offsetIndex.orElseThrow());
            }
        }
        return List.of();
    }

    private static List<Long> pageFirstRowIndexes(OffsetIndex offsetIndex) {
        List<PageLocation> pageLocations = offsetIndex.pageLocations();
        List<Long> firstRows = new ArrayList<>(pageLocations.size());
        for (PageLocation page : pageLocations) {
            firstRows.add(page.firstRowIndex());
        }
        return firstRows;
    }

    /**
     * The projected leaf column paths for {@code projection}: every leaf when {@link Projection#ALL}, else the subset.
     */
    private List<ColumnPath> projectedLeafColumns(Projection projection) {
        return switch (projection) {
            case Projection.All _ -> fileSchema.leafColumns();
            case Projection.Of _ -> projectionOf(projection).leafColumns();
        };
    }

    private ParquetSchema projectionOf(Projection projection) {
        return switch (projection) {
            case Projection.All _ -> fileSchema;
            case Projection.Of of -> fileSchema.project(of.physicalColumns());
        };
    }

    /**
     * Sums the total compressed size of {@code chunks}' projected leaf column chunks. A projected leaf absent from the
     * row group is skipped. The result feeds {@link ExplainPlan#estimatedBytesRead()} as the bytes a read would fetch
     * from this row group when it survives pruning.
     */
    private static long projectedCompressedBytes(RowGroupChunks chunks, List<ColumnPath> projectedLeaves) {
        long total = 0L;
        for (ColumnPath leaf : projectedLeaves) {
            total += chunks.meta(leaf).map(ColumnMetaData::totalCompressedSize).orElse(0L);
        }
        return total;
    }

    /**
     * Returns the inline statistics lookup for {@code chunks}. Each call to the returned lookup delegates to
     * {@link RowGroupChunks#stats}, which reads from the in-footer column metadata without any I/O.
     */
    private ColumnStatsLookup statsLookup(RowGroupChunks chunks) {
        return chunks::stats;
    }

    /**
     * Returns the column-index tier lookup for {@code chunks}, or the no-op lookup when {@code useColumnIndexFilter} is
     * off. Each call to the returned lookup delegates to {@link RowGroupChunks#pageStats}, which memoizes the result so
     * each column's index sections are read at most once per call.
     */
    private ColumnPageStatsLookup pageStatsLookupFor(RowGroupChunks chunks, ReadOptions options) {
        if (!options.useColumnIndexFilter()) {
            return noColumnPageStatsLookup();
        }
        return chunks::pageStats;
    }

    /**
     * Returns the bloom-filter lookup for {@code chunks}. Each call to the returned lookup delegates to
     * {@link RowGroupChunks#bloom}, which memoizes the result so each column's bloom filter is read at most once per
     * call. Returns {@link FilterPipeline#emptyBloomLookup()} when {@code options.useBloomFilter()} is off; the bloom
     * tier degrades gracefully without forcing the evaluator to handle nulls.
     */
    private BloomFilterLookup bloomLookupFor(RowGroupChunks chunks, ReadOptions options) {
        if (!options.useBloomFilter()) {
            return FilterPipeline.emptyBloomLookup();
        }
        return chunks::bloom;
    }

    /**
     * The absolute file row offset of each row group, keyed by identity: the running sum of {@code num_rows} over the
     * row groups that precede it in file order, pruned ones included (a pruned group still consumes position space).
     * One definition shared by the row-position synthesis and the filter pipeline's row-position tier, which keeps the
     * two offset walks from drifting.
     */
    private static Map<RowGroupChunks, Long> rowGroupFileOffsets(List<RowGroupChunks> rowGroupChunks) {
        Map<RowGroupChunks, Long> offsets = new IdentityHashMap<>(rowGroupChunks.size());
        long runningOffset = 0L;
        for (RowGroupChunks chunks : rowGroupChunks) {
            offsets.put(chunks, runningOffset);
            runningOffset += chunks.numRows();
        }
        return offsets;
    }

    /**
     * The per-survivor row-position synthesis inputs, parallel to {@code survivors}: each survivor's file row offset
     * paired with the caller-named columns. Returns an empty list when the predicate has no positional delete, which
     * keeps the decode path untouched. Keying the offsets by {@link RowGroupChunks} identity rather than by re-walking
     * the plan maps each survivor back to its true file offset after {@link #survivorsFor} has dropped the eliminated
     * groups.
     */
    private static List<RowPositionSynthesis> rowPositionSynthesesFor(
            List<RowGroupSurvivor> survivors,
            List<RowGroupChunks> rowGroupChunks,
            List<RowPositionColumn> rowPositionColumns) {
        if (rowPositionColumns.isEmpty()) {
            return List.of();
        }
        Map<RowGroupChunks, Long> baseByChunks = rowGroupFileOffsets(rowGroupChunks);
        List<RowPositionSynthesis> syntheses = new ArrayList<>(survivors.size());
        for (RowGroupSurvivor survivor : survivors) {
            Long base = baseByChunks.get(survivor.chunks());
            if (base == null) {
                throw new IllegalStateException("Survivor row group is not part of the file's row groups");
            }
            syntheses.add(new RowPositionSynthesis(base, rowPositionColumns));
        }
        return syntheses;
    }

    /**
     * Translates the explain plan's row-group outcomes into materialization-ready {@link RowGroupSurvivor survivors}.
     */
    private List<RowGroupSurvivor> survivorsFor(ExplainPlan plan, List<RowGroupChunks> rowGroupChunks) {
        List<RowGroupSurvivor> survivors = new ArrayList<>(plan.rowGroups().size());
        for (RowGroupPlan rgPlan : plan.rowGroups()) {
            RowGroupChunks chunks = rowGroupChunks.get(rgPlan.index());
            switch (rgPlan.outcome()) {
                case ELIMINATED -> {
                    /* drop */
                }
                case PARTIAL -> survivors.add(new RowGroupSurvivor(chunks, rgPlan.survivingRows(), true));
                case FULL -> survivors.add(RowGroupSurvivor.full(chunks));
                case MATCHED -> survivors.add(RowGroupSurvivor.matched(chunks));
            }
        }
        return survivors;
    }

    /**
     * Builds one decode mask per survivor, parallel to {@code survivors}. A mask is present only when page-skip is both
     * wanted and safe for that row group: {@code useColumnIndexFilter} on, the column-index tier narrowed the row set,
     * every scanned column flat, and every scanned column has an offset index. Otherwise the entry is empty and the row
     * group decodes in full.
     */
    private List<Optional<RowMask>> decodeMasksFor(
            List<RowGroupSurvivor> survivors, ParquetSchema scanSchema, ReadOptions options) {
        List<Optional<RowMask>> masks = new ArrayList<>(survivors.size());
        boolean scanFlat = options.useColumnIndexFilter() && allFlat(fileSchema, scanSchema.leafColumns());
        for (RowGroupSurvivor survivor : survivors) {
            masks.add(scanFlat ? maskFor(survivor, scanSchema) : Optional.empty());
        }
        return masks;
    }

    private Optional<RowMask> maskFor(RowGroupSurvivor survivor, ParquetSchema scanSchema) {
        if (survivor.survivingRows().isEmpty()) {
            return Optional.empty();
        }
        RowRanges surviving = survivor.survivingRows().orElseThrow();
        RowGroupChunks chunks = survivor.chunks();
        Map<ColumnPath, OffsetIndex> offsetIndexes =
                LinkedHashMap.newLinkedHashMap(scanSchema.leafColumns().size());
        for (ColumnPath leaf : scanSchema.leafColumns()) {
            Optional<OffsetIndex> offsetIndex = chunks.offsetIndex(leaf);
            if (offsetIndex.isEmpty()) {
                return Optional.empty();
            }
            offsetIndexes.put(leaf, offsetIndex.orElseThrow());
        }
        return Optional.of(new RowMask(surviving, offsetIndexes));
    }

    /** True when every {@code leaf} is non-repeated (max repetition level 0) in {@code schema}. Pure; testable. */
    static boolean allFlat(ParquetSchema schema, List<ColumnPath> leaves) {
        for (ColumnPath leaf : leaves) {
            if (schema.maxLevels(leaf).maxRepetitionLevel() > 0) {
                return false;
            }
        }
        return true;
    }

    private static ColumnPageStatsLookup noColumnPageStatsLookup() {
        return _ -> Optional.empty();
    }
}
