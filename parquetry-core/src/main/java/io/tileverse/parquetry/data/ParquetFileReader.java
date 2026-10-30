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
package io.tileverse.parquetry.data;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import io.tileverse.parquetry.columnar.BatchMaterializer;
import io.tileverse.parquetry.columnar.BatchRows;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.RowGroupOutcome;
import io.tileverse.parquetry.filter.explain.RowGroupPlan;
import io.tileverse.parquetry.filter.explain.Tier;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.internal.filter.FilterPipeline;
import io.tileverse.parquetry.internal.filter.FilterPipeline.BloomFilterLookup;
import io.tileverse.parquetry.internal.filter.FilterPipeline.PageIndexLookup;
import io.tileverse.parquetry.internal.filter.PageIndex;
import io.tileverse.parquetry.internal.filter.PredicateNormalizer;
import io.tileverse.parquetry.internal.filter.StatsEvaluator;
import io.tileverse.parquetry.internal.filter.spatial.BoundsAccumulator;
import io.tileverse.parquetry.internal.filter.spatial.SpatialBoundsSource;
import io.tileverse.parquetry.internal.filter.spatial.SpatialCoveringRewrite;
import io.tileverse.parquetry.internal.footer.ChunkMeta;
import io.tileverse.parquetry.internal.footer.CompactFooter;
import io.tileverse.parquetry.internal.footer.LeafIndex;
import io.tileverse.parquetry.internal.read.BatchForm;
import io.tileverse.parquetry.internal.read.BatchPipeline;
import io.tileverse.parquetry.internal.read.DecryptionKeyRetriever;
import io.tileverse.parquetry.internal.read.IndexSectionLoader;
import io.tileverse.parquetry.internal.read.MaskedScan;
import io.tileverse.parquetry.internal.read.ParallelDecodeCoordinator;
import io.tileverse.parquetry.internal.read.ParallelDecodeCoordinator.DecodeObservation;
import io.tileverse.parquetry.internal.read.RowGroupChunks;
import io.tileverse.parquetry.internal.read.RowGroupGate;
import io.tileverse.parquetry.internal.read.RowGroupPlanner;
import io.tileverse.parquetry.internal.read.RowGroupSurvivor;
import io.tileverse.parquetry.internal.read.RowMask;
import io.tileverse.parquetry.internal.read.RowMasks;
import io.tileverse.parquetry.internal.read.RowPositionColumn;
import io.tileverse.parquetry.internal.read.RowPositionSynthesis;
import io.tileverse.parquetry.internal.read.SpatialDecimationGate;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.observe.FetchPurpose;
import io.tileverse.parquetry.observe.QueryObserver;
import io.tileverse.parquetry.observe.QueryStarted;
import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.schema.geo.geoparquet.GeometryColumns;

import lombok.NonNull;

/**
 * Read entry point for a single Parquet file. {@link #open(ByteRangeSource)} reads the footer once, unless
 * {@link FooterMetadataCache} already holds the file's metadata, and retains the forms derived from it. The
 * {@link #read read}, {@link #readBatches readBatches}, and {@link #explain explain} entry points all share that one
 * footer.
 *
 * <p>The decoded wire records live only for the duration of the parse triggered by {@link #open open}, which derives
 * every retained form from them and lets them go; the reader keeps a packed {@code CompactFooter} instead. A dataset of
 * several hundred open files multiplies the footprint of a single reader, and the wire records are by far the heaviest
 * part of a parsed footer. Reopening a file skips even that parse: {@link FooterMetadataCache} holds the derived forms
 * - the packed footer, the schema, the leaf index, the GeoParquet metadata, the spatial bounds, the key/value metadata,
 * and the row-group view - and every reader over one file shares all of them.
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
    private final ParquetSchema fileSchema;
    private final CompactFooter compactFooter;
    private final LeafIndex leafIndex;
    private final Map<String, String> keyValueMetadata;
    private final List<RowGroupSummary> rowGroupView;
    private final long columnChunkBytes;
    private final Optional<GeoParquetMetadata> geoMetadata;
    private final SpatialBoundsSource spatialBounds;
    private final ParquetRuntime runtime;
    private final ReadResources readResources;
    private final SpatialReadGates spatialReadGates;

    // Held for an encrypted file; the footer-decryption path that reads it is not wired in yet.
    @SuppressWarnings("java:S1068")
    private final Optional<DecryptionKeyRetriever> decryptionKeyRetriever;

    private ParquetFileReader(
            ByteRangeSource source,
            FooterMetadata footerMetadata,
            ParquetRuntime runtime,
            Optional<DecryptionKeyRetriever> decryptionKeyRetriever) {
        this.source = source;
        this.fileSchema = footerMetadata.fileSchema();
        this.compactFooter = footerMetadata.compactFooter();
        this.leafIndex = footerMetadata.leafIndex();
        this.keyValueMetadata = footerMetadata.keyValueMetadata();
        this.rowGroupView = footerMetadata.rowGroups();
        this.columnChunkBytes = footerMetadata.columnChunkBytes();
        this.geoMetadata = footerMetadata.geoMetadata();
        this.spatialBounds = footerMetadata.spatialBounds();
        this.runtime = runtime;
        this.readResources = new ReadResources(source, fileSchema, runtime);
        this.spatialReadGates = new SpatialReadGates(spatialBounds, fileSchema, geoMetadata);
        this.decryptionKeyRetriever = decryptionKeyRetriever;
    }

    /**
     * Opens a reader against the default runtime, reading {@code source}'s footer unless {@link FooterMetadataCache}
     * already holds its metadata. Equivalent to {@code open(source, ParquetRuntime.defaultRuntime(),
     * Optional.empty())}.
     */
    public static ParquetFileReader open(@NonNull ByteRangeSource source) {
        return open(source, ParquetRuntime.defaultRuntime(), Optional.empty());
    }

    /**
     * Opens a reader bound to {@code runtime} for read resources, reading {@code source}'s footer unless
     * {@link FooterMetadataCache} already holds its metadata. The {@code decryptionKeyRetriever} is held for an
     * encrypted file.
     */
    public static ParquetFileReader open(
            @NonNull ByteRangeSource source,
            @NonNull ParquetRuntime runtime,
            @NonNull Optional<DecryptionKeyRetriever> decryptionKeyRetriever) {
        FooterMetadata footerMetadata = footerMetadataOf(source, decryptionKeyRetriever);
        return new ParquetFileReader(source, footerMetadata, runtime, decryptionKeyRetriever);
    }

    /**
     * The derived footer forms for {@code source}, served from the shared cache when the source names itself. Two kinds
     * of open derive their own instead: one that received a decryption key, because metadata decrypted with a caller's
     * key must not outlive the reader holding that key, and one over an unnamed source, which offers no stable identity
     * to key on.
     */
    private static FooterMetadata footerMetadataOf(
            ByteRangeSource source, Optional<DecryptionKeyRetriever> decryptionKeyRetriever) {
        Supplier<FooterMetadata> loader = () -> FooterMetadata.parse(source);
        if (decryptionKeyRetriever.isPresent()) {
            return loader.get();
        }
        Optional<String> sourceId = source.sourceIdentifier();
        return sourceId.map(id -> FooterMetadataCache.get(id, source.size(), loader))
                .orElseGet(loader);
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
     * The packed planning form of this file's footer, held in place of the decoded wire records. It is immutable and
     * freely shareable between readers opened on the same file.
     */
    CompactFooter compactFooter() {
        return compactFooter;
    }

    /**
     * The mapping between this file's leaf columns and the ordinals addressing them in the packed planning footer.
     * Derived once with that footer and shared between readers opened on the same file.
     */
    LeafIndex leafIndex() {
        return leafIndex;
    }

    /**
     * The most precise bounds tier applicable to this file, answering a geometry column's file-level and per-row-group
     * extents. Derived once with the packed planning footer and shared between readers opened on the same file.
     */
    SpatialBoundsSource spatialBounds() {
        return spatialBounds;
    }

    /**
     * The footer-aggregated prunable statistics for this file: per-column min/max/null-count combined across the file's
     * row groups, the geometry bounding box for each geometry column, and the record count. Reads only the cached
     * footer metadata, no page data. The result feeds {@link io.tileverse.parquetry.filter.prune.FilePruner} for
     * whole-file pruning, mirroring the Iceberg manifest-bounds path with the footer as the source.
     */
    public FileStats fileStats() {
        return new FileStatsAggregator(spatialBounds, fileSchema, geoMetadata).aggregate(rowGroupChunks());
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

        QueryObservation observation = QueryObservation.observe(options);
        Stream<T> rows =
                readRows(rawPredicate, projection, materializer, observation).rows();
        return observation.onClose(rows);
    }

    /**
     * The rows of a read with its plan, shared by {@link #read} and the analyze drain. A shaping projection reads
     * through the batch path.
     */
    private <T> PlannedRows<T> readRows(
            Predicate rawPredicate, Projection projection, Materializer<T> materializer, QueryObservation observation) {

        if (needsShaping(projection)) {
            return shapedRows(rawPredicate, projection, materializer, observation);
        }
        return buildRowStream(rawPredicate, projection, materializer, observation);
    }

    private static boolean needsShaping(Projection projection) {
        return projection instanceof Projection.Of of && of.needsShaping();
    }

    /**
     * A shaping read (constants, renames, widenings, or reorders) materializes through the batch produce path, then
     * flattens each produced batch to rows. {@link #buildRowStream} serves a plain physical passthrough.
     */
    private <T> PlannedRows<T> shapedRows(
            Predicate rawPredicate, Projection projection, Materializer<T> materializer, QueryObservation observation) {
        PlannedRows<ParquetRecordBatch> produced =
                readBatchesLowered(rawPredicate, projection, BatchMaterializer.defaultBatch(), observation);
        return new PlannedRows<>(produced.plan(), BatchRows.rows(produced.rows(), materializer));
    }

    /**
     * Builds the per-row decode stream of a plain physical passthrough, with its plan. The fetch accumulator of
     * {@code observation} tallies the index-section and page fetches of the analyze drain.
     */
    private <T> PlannedRows<T> buildRowStream(
            Predicate rawPredicate, Projection projection, Materializer<T> materializer, QueryObservation observation) {

        ReadOptions options = observation.options();
        Predicate predicate = lowerSpatialPredicates(rawPredicate);
        ProjectionPlan projectionPlan =
                ProjectionPlan.resolve(fileSchema, projection, predicate, options.useRecordLevelFilter());
        List<RowPositionColumn> rowPositionRequests = projectionPlan.synthesizedColumns();
        boolean recordLevel = projectionPlan.recordLevel();
        Projection scanProjection = projectionPlan.scanProjection();
        FetchAccumulator accumulator = observation.fetchAccumulator();
        List<RowGroupChunks> rowGroupChunks = rowGroupChunks(accumulator);
        ExplainPlan plan = timedFilterPipeline(predicate, scanProjection, rowGroupChunks, observation);
        List<RowGroupSurvivor> survivors = survivorsFor(plan, rowGroupChunks);
        ParquetSchema scanSchema = plan.projectedSchema();
        ParquetSchema outputSchema = projectionPlan.physicalOutputSchema();
        List<Optional<RowMask>> decodeMasks = decodeMasksFor(survivors, scanSchema, options);
        Predicate normalized = plan.normalizedPredicate();
        Optional<MaskedScan> maskedScan = maskedScanFor(scanSchema, outputSchema, normalized, recordLevel);
        Predicate recordFilter = (recordLevel && maskedScan.isEmpty()) ? recordFilterOf(normalized) : null;
        BatchForm rowsBatchForm = maskedScan.isPresent() ? BatchForm.LEVELS : rowsForm(recordFilter);
        DecodeObservation decodeObservation = observation.decodeObservation(false);
        List<RowPositionSynthesis> rowPositions =
                rowPositionSynthesesFor(survivors, rowGroupChunks, rowPositionRequests);
        Optional<RowGroupGate> rowGroupGate = spatialReadGates.rowGroupGate(survivors, options);
        Optional<RowGroupPlanner> rowGroupPlanner =
                spatialRowGroupPlanner(survivors, rawPredicate, options, outputSchema);
        ParallelDecodeCoordinator coordinator = readResources.newDecodeCoordinator(
                survivors,
                scanSchema,
                decodeMasks,
                options,
                maskedScan,
                rowsBatchForm,
                accumulator,
                decodeObservation,
                rowPositions,
                rowGroupGate,
                rowGroupPlanner);
        Optional<SpatialDecimationGate> leafGate = spatialReadGates.leafGate(options);
        Stream<T> rows = BatchPipeline.rows(
                coordinator,
                materializer,
                outputSchema,
                recordFilter,
                observation.observing(),
                observation.wantsTimings(),
                leafGate);
        return new PlannedRows<>(plan, rows);
    }

    /** A row stream and its plan. */
    private record PlannedRows<T>(ExplainPlan plan, Stream<T> rows) {}

    /**
     * The per-row-group spatial planner of a read that produces rows, present only when the read supplies a probe and
     * {@code outputSchema} holds the file's primary geometry column. It plans against the normalized raw predicate,
     * before the covering rewrite: the comparisons added by that rewrite are necessary conditions of the spatial leaf
     * that they were derived from, hence a unit accepted on the raw leaf satisfies them too.
     */
    private Optional<RowGroupPlanner> spatialRowGroupPlanner(
            List<RowGroupSurvivor> survivors, Predicate rawPredicate, ReadOptions options, ParquetSchema outputSchema) {
        Predicate normalizedRaw = PredicateNormalizer.normalize(rawPredicate);
        return spatialReadGates.rowGroupPlanner(survivors, normalizedRaw, options, outputSchema);
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
     * Packages the per-read inputs of the masked scan decode path, or empty when the read has no per-row predicate to
     * apply. The scan needs a normalized predicate that is not trivially true and at least one physical column to
     * evaluate it over; a predicate that names only synthesized columns borrows the scan's first leaf to drive the
     * walk.
     */
    private Optional<MaskedScan> maskedScanFor(
            ParquetSchema scanSchema, ParquetSchema outputSchema, Predicate normalized, boolean recordLevel) {
        if (!recordLevel) {
            return Optional.empty();
        }
        if (recordFilterOf(normalized) == null) {
            return Optional.empty();
        }
        if (Predicate.columns(normalized).isEmpty()) {
            return Optional.empty();
        }
        Set<ColumnPath> filterLeaves = filterLeavesFor(normalized, scanSchema);
        return Optional.of(new MaskedScan(normalized, filterLeaves, outputSchema));
    }

    /**
     * The physical leaves the predicate reads. A predicate over synthesized columns alone reads none, and the scan's
     * first leaf stands in to enumerate the rows the synthesis is derived from.
     */
    private static Set<ColumnPath> filterLeavesFor(Predicate normalized, ParquetSchema scanSchema) {
        Set<ColumnPath> columns = physicalColumns(normalized);
        if (columns.isEmpty()) {
            return Set.of(scanSchema.leafColumns().get(0));
        }
        return columns;
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
        QueryObservation observation = QueryObservation.observe(options);
        try {
            return countLowered(predicate, observation);
        } finally {
            observation.finishIfObserving();
        }
    }

    /**
     * Counts the rows matching {@code predicate} (already spatial-lowered, never an {@link Predicate.Always} literal),
     * reporting to {@code observation}.
     */
    private long countLowered(Predicate predicate, QueryObservation observation) {
        List<RowPositionColumn> rowPositionRequests = ProjectionPlan.rowPositionColumns(fileSchema, predicate);
        Projection predicateProjection = predicateScanProjection(predicate);
        List<RowGroupChunks> rowGroupChunks = rowGroupChunks(observation.fetchAccumulator());
        ExplainPlan plan = timedFilterPipeline(predicate, predicateProjection, rowGroupChunks, observation);

        long matchedRows = matchedRowCount(plan, observation);
        List<RowGroupSurvivor> residual = residualSurvivors(plan, rowGroupChunks);
        if (residual.isEmpty()) {
            return matchedRows;
        }
        List<RowPositionSynthesis> rowPositions =
                rowPositionSynthesesFor(residual, rowGroupChunks, rowPositionRequests);
        return matchedRows + countResidual(residual, plan, observation, rowPositions);
    }

    /**
     * Sum of row counts for the MATCHED row groups, added without any decode. Each of them is reported to
     * {@code observation} here, as the decode never sees it.
     */
    private static long matchedRowCount(ExplainPlan plan, QueryObservation observation) {
        long matchedRows = 0L;
        for (RowGroupPlan rgPlan : plan.rowGroups()) {
            if (rgPlan.outcome() == RowGroupOutcome.MATCHED) {
                matchedRows += rgPlan.rowCount();
                observation.matchedRowGroup(rgPlan);
            }
        }
        return matchedRows;
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

    /**
     * Decodes the predicate columns of the residual row groups and counts the matches. The masked scan applies the
     * predicate during decode and produces nothing but row counts; where it does not run, each decoded batch is tested
     * by a columnar popcount.
     */
    private long countResidual(
            List<RowGroupSurvivor> residual,
            ExplainPlan plan,
            QueryObservation observation,
            List<RowPositionSynthesis> rowPositions) {
        ReadOptions options = observation.options();
        ParquetSchema scanSchema = plan.projectedSchema();
        Predicate normalized = plan.normalizedPredicate();
        List<Optional<RowMask>> masks = decodeMasksFor(residual, scanSchema, options);
        Optional<SpatialDecimationGate> leafGate = spatialReadGates.leafGate(options);
        Optional<MaskedScan> maskedScan = countMaskedScan(scanSchema, normalized, leafGate);
        DecodeObservation decodeObservation = observation.decodeObservation(false);
        Optional<RowGroupGate> rowGroupGate = spatialReadGates.rowGroupGate(residual, options);
        ParallelDecodeCoordinator coordinator = readResources.newDecodeCoordinator(
                residual,
                scanSchema,
                masks,
                options,
                maskedScan,
                BatchForm.LEVELS,
                observation.fetchAccumulator(),
                decodeObservation,
                rowPositions,
                rowGroupGate,
                Optional.empty());
        boolean observe = observation.observing();
        if (maskedScan.isPresent()) {
            return BatchPipeline.countEmitted(coordinator, observe);
        }
        return BatchPipeline.countMatching(coordinator, normalized, observe, leafGate);
    }

    /**
     * The masked scan a count evaluates through: its output set is empty, since count materializes no value and folds
     * nothing but each emitted batch's row count. It is absent when a spatial decimation gate is in play, whose per-row
     * test reads the geometry column off each batch and hence needs the columns a full decode produces.
     */
    private Optional<MaskedScan> countMaskedScan(
            ParquetSchema scanSchema, Predicate normalized, Optional<SpatialDecimationGate> leafGate) {
        if (leafGate.isPresent()) {
            return Optional.empty();
        }
        ParquetSchema noOutputColumns = fileSchema.project(Set.of());
        return maskedScanFor(scanSchema, noOutputColumns, normalized, true);
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
     * the file has no geometry column or no matching row holds a geometry with an extent (null and empty geometries add
     * nothing). The box is 2D-exact; its Z and M extents are present only when the whole answer came from metadata
     * boxes, and any scanned row drops them.
     *
     * <p>The box is exact relative to the file's declared geometry statistics, which are trusted as tight; a writer
     * that declared rounded boxes widens the answer accordingly. The box has finite edges and never wraps the
     * antimeridian: a declared box wrapping it contributes its {@link BoundingBox#planarEnclosure() planar enclosure}.
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
     * expose none. A metadata box wrapping the antimeridian is answered as its planar enclosure, and a file box without
     * an extent is treated as absent. A row group without rows has no box and nothing to scan: it is skipped.
     */
    private Optional<BoundingBox> unfilteredBounds(ColumnPath geometryColumn, ReadOptions options) {
        Optional<BoundingBox> fileBox = spatialBounds.fileBounds(geometryColumn).filter(BoundingBox::hasExtent);
        if (fileBox.isPresent()) {
            return fileBox.map(BoundingBox::planarEnclosure);
        }
        return unionRowGroupBoundsAndScanTheRest(geometryColumn, options);
    }

    private Optional<BoundingBox> unionRowGroupBoundsAndScanTheRest(ColumnPath geometryColumn, ReadOptions options) {
        BoundsAccumulator accumulator = new BoundsAccumulator();
        List<RowGroupChunks> rowGroupChunks = rowGroupChunks();
        List<RowGroupSurvivor> boxLess = new ArrayList<>();
        for (int i = 0; i < rowGroupChunks.size(); i++) {
            RowGroupChunks chunks = rowGroupChunks.get(i);
            Optional<BoundingBox> box = spatialBounds.rowGroupBounds(geometryColumn, i);
            if (box.isPresent()) {
                accumulator.union(box.orElseThrow());
            } else if (chunks.numRows() != 0L) {
                boxLess.add(RowGroupSurvivor.full(chunks));
            }
        }
        if (!boxLess.isEmpty()) {
            ParquetSchema scanSchema = fileSchema.project(Set.of(geometryColumn));
            List<Optional<RowMask>> masks = decodeMasksFor(boxLess, scanSchema, options);
            // An unfiltered box-less scan runs a null predicate: nothing to mask a scan on, and no row-position column.
            ParallelDecodeCoordinator coordinator = boundsDecodeCoordinator(
                    boxLess, scanSchema, masks, options, DecodeObservation.NONE, List.of(), Optional.empty());
            BatchPipeline.boundsMatching(coordinator, null, geometryColumn, accumulator);
        }
        return accumulator.snapshot();
    }

    /**
     * Bounds for a filtered read: run the filter pipeline, union the tight box of every all-match row group, then scan
     * the residue - skipping any row group the accumulated bounds already cover - to fold matching rows' envelopes.
     * Reports to a {@link QueryObservation} as {@link #count} does, letting the residual decode emit the same
     * per-row-group read events.
     */
    private Optional<BoundingBox> boundsLowered(
            Predicate predicate, ColumnPath geometryColumn, ReadOptions rawOptions) {
        QueryObservation observation = QueryObservation.observe(rawOptions);
        try {
            return foldFilteredBounds(predicate, geometryColumn, observation);
        } finally {
            observation.finishIfObserving();
        }
    }

    private Optional<BoundingBox> foldFilteredBounds(
            Predicate predicate, ColumnPath geometryColumn, QueryObservation observation) {
        BoundsAccumulator accumulator = new BoundsAccumulator();
        List<RowGroupChunks> rowGroupChunks = rowGroupChunks(observation.fetchAccumulator());
        List<RowPositionColumn> rowPositionRequests = ProjectionPlan.rowPositionColumns(fileSchema, predicate);
        Projection scanProjection = boundsScanProjection(predicate, geometryColumn);
        ExplainPlan plan = timedFilterPipeline(predicate, scanProjection, rowGroupChunks, observation);
        List<ResidualGroup> residual =
                unionMatchedAndCollectResidual(plan, rowGroupChunks, geometryColumn, accumulator);
        scanResidualBounds(
                residual, plan, geometryColumn, observation, accumulator, rowGroupChunks, rowPositionRequests);
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
                case MATCHED -> unionOrCollectMatched(chunks, box, accumulator, residual);
                case FULL -> residual.add(new ResidualGroup(RowGroupSurvivor.full(chunks), box));
                case PARTIAL ->
                    residual.add(new ResidualGroup(new RowGroupSurvivor(chunks, rgPlan.survivingRows(), true), box));
            }
        }
        return residual;
    }

    private static void unionOrCollectMatched(
            RowGroupChunks chunks,
            Optional<BoundingBox> box,
            BoundsAccumulator accumulator,
            List<ResidualGroup> residual) {
        if (box.isPresent()) {
            accumulator.union(box.orElseThrow());
        } else {
            residual.add(new ResidualGroup(RowGroupSurvivor.full(chunks), Optional.empty()));
        }
    }

    /**
     * Pass two: scan the residual row groups largest box first, folding their matching rows' WKB envelopes. Before each
     * decode the row group's conservative box is re-checked against the accumulated bounds; a covered row group is
     * skipped without any fetch, and the growing extent lets a big early row group cover later ones. A box-less row
     * group sorts last and is never skipped.
     */
    private void scanResidualBounds(
            List<ResidualGroup> residual,
            ExplainPlan plan,
            ColumnPath geometryColumn,
            QueryObservation observation,
            BoundsAccumulator accumulator,
            List<RowGroupChunks> rowGroupChunks,
            List<RowPositionColumn> rowPositionRequests) {
        Predicate predicate = plan.normalizedPredicate();
        ParquetSchema scanSchema = plan.projectedSchema();
        ReadOptions options = observation.options();
        DecodeObservation decodeObservation = observation.decodeObservation(false);
        for (ResidualGroup group : orderedByDescendingArea(residual)) {
            if (isCovered(group, accumulator)) {
                continue;
            }
            scanResidualGroup(
                    group.survivor(),
                    predicate,
                    geometryColumn,
                    scanSchema,
                    options,
                    decodeObservation,
                    accumulator,
                    rowGroupChunks,
                    rowPositionRequests);
        }
    }

    /**
     * Folds one residual row group's matching rows into {@code accumulator}. The masked scan applies the predicate
     * during decode and emits the geometry of the surviving rows alone, which the fold then unions row by row; where it
     * does not run, every decoded row is tested as it is folded.
     */
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
        List<RowGroupSurvivor> survivors = List.of(survivor);
        List<RowPositionSynthesis> rowPositions =
                rowPositionSynthesesFor(survivors, rowGroupChunks, rowPositionRequests);
        List<Optional<RowMask>> masks = decodeMasksFor(survivors, scanSchema, options);
        ParquetSchema geometryOnly = fileSchema.project(Set.of(geometryColumn));
        Optional<MaskedScan> maskedScan = maskedScanFor(scanSchema, geometryOnly, predicate, true);
        ParallelDecodeCoordinator coordinator =
                boundsDecodeCoordinator(survivors, scanSchema, masks, options, observation, rowPositions, maskedScan);
        Predicate foldFilter = maskedScan.isPresent() ? null : predicate;
        BatchPipeline.boundsMatching(coordinator, foldFilter, geometryColumn, accumulator);
    }

    /**
     * Builds a decode coordinator over {@code survivors} that decodes only {@code scanSchema}, as count's residual
     * does.
     */
    private ParallelDecodeCoordinator boundsDecodeCoordinator(
            List<RowGroupSurvivor> survivors,
            ParquetSchema scanSchema,
            List<Optional<RowMask>> masks,
            ReadOptions options,
            DecodeObservation observation,
            List<RowPositionSynthesis> rowPositions,
            Optional<MaskedScan> maskedScan) {
        Optional<RowGroupGate> rowGroupGate = spatialReadGates.rowGroupGate(survivors, options);
        return readResources.newDecodeCoordinator(
                survivors,
                scanSchema,
                masks,
                options,
                maskedScan,
                BatchForm.LEVELS,
                FetchAccumulator.NONE,
                observation,
                rowPositions,
                rowGroupGate,
                Optional.empty());
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
     * One residual row group to fold into the bounds: its materialization-ready survivor, and its conservative metadata
     * box when the file records one. A box-less row group reports a {@link Double#NEGATIVE_INFINITY} area that sorts it
     * last, and is never containment-skipped. The area of a box is that of its planar enclosure, the full longitude
     * band for a box wrapping the antimeridian; a box without a finite area sorts with the smallest real boxes.
     */
    private record ResidualGroup(RowGroupSurvivor survivor, Optional<BoundingBox> box) {

        double area() {
            if (box.isEmpty()) {
                return Double.NEGATIVE_INFINITY;
            }
            BoundingBox planar = box.orElseThrow().planarEnclosure();
            double area = (planar.xmax() - planar.xmin()) * (planar.ymax() - planar.ymin());
            return Double.isFinite(area) ? area : 0;
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

        QueryObservation observation = QueryObservation.observe(options);
        Stream<T> batches = readBatchesLowered(rawPredicate, projection, materializer, observation)
                .rows();
        return observation.onClose(batches);
    }

    private <T> PlannedRows<T> readBatchesLowered(
            Predicate rawPredicate,
            Projection projection,
            BatchMaterializer<T> materializer,
            QueryObservation observation) {

        ReadOptions options = observation.options();
        Predicate predicate = lowerSpatialPredicates(rawPredicate);
        ProjectionPlan projectionPlan =
                ProjectionPlan.resolve(fileSchema, projection, predicate, options.useRecordLevelFilter());
        List<RowPositionColumn> rowPositionRequests = projectionPlan.synthesizedColumns();
        boolean recordLevel = projectionPlan.recordLevel();
        Projection scanProjection = projectionPlan.scanProjection();
        List<RowGroupChunks> rowGroupChunks = rowGroupChunks(observation.fetchAccumulator());
        ExplainPlan plan = timedFilterPipeline(predicate, scanProjection, rowGroupChunks, observation);
        List<RowGroupSurvivor> survivors = survivorsFor(plan, rowGroupChunks);
        ParquetSchema scanSchema = plan.projectedSchema();
        ParquetSchema outputSchema = projectionPlan.physicalOutputSchema();
        List<Optional<RowMask>> decodeMasks = decodeMasksFor(survivors, scanSchema, options);
        Predicate normalized = plan.normalizedPredicate();
        Optional<MaskedScan> maskedScan = maskedScanFor(scanSchema, outputSchema, normalized, recordLevel);
        Predicate recordFilter = (recordLevel && maskedScan.isEmpty()) ? recordFilterOf(normalized) : null;
        Optional<SpatialDecimationGate> leafGate = spatialReadGates.leafGate(options);
        // A masked scan still narrows: a MATCHED row group takes the full-decode driver, whose batches expose the scan
        // columns rather than the caller's output columns. Emitting the decoded shape is therefore also the one case
        // where nothing downstream drops a row, which lets every decoded row be reported as matched; any other shape
        // tallies the rows it emits.
        boolean emitDecodedShape = recordFilter == null && leafGate.isEmpty() && maskedScan.isEmpty();
        DecodeObservation decodeObservation = observation.decodeObservation(emitDecodedShape);
        List<RowPositionSynthesis> rowPositions =
                rowPositionSynthesesFor(survivors, rowGroupChunks, rowPositionRequests);
        Optional<RowGroupGate> rowGroupGate = spatialReadGates.rowGroupGate(survivors, options);
        Optional<RowGroupPlanner> rowGroupPlanner =
                spatialRowGroupPlanner(survivors, rawPredicate, options, outputSchema);
        ParallelDecodeCoordinator coordinator = readResources.newDecodeCoordinator(
                survivors,
                scanSchema,
                decodeMasks,
                options,
                maskedScan,
                BatchForm.ASSEMBLED,
                observation.fetchAccumulator(),
                decodeObservation,
                rowPositions,
                rowGroupGate,
                rowGroupPlanner);
        Stream<ParquetRecordBatch> batches = emitDecodedShape
                ? BatchPipeline.batches(coordinator)
                : BatchPipeline.batches(coordinator, recordFilter, outputSchema, leafGate);
        Stream<ParquetRecordBatch> produced =
                projectionPlan.needsShaping() ? batches.map(projectionPlan::produce) : batches;
        ParquetSchema producedSchema = projectionPlan.producedSchema();
        return new PlannedRows<>(plan, produced.map(batch -> materializer.materialize(producedSchema, batch)));
    }

    /**
     * Runs the filter pipeline without reading data and returns the explain plan. The plan covers the columns scanned
     * by a read of {@code projection}: with the record-level filter on, they include the columns of the predicate.
     */
    public ExplainPlan explain(
            @NonNull Predicate rawPredicate, @NonNull Projection projection, @NonNull ReadOptions options) {
        Predicate predicate = lowerSpatialPredicates(rawPredicate);
        ProjectionPlan projectionPlan =
                ProjectionPlan.resolve(fileSchema, projection, predicate, options.useRecordLevelFilter());
        Projection scanProjection = projectionPlan.scanProjection();
        return runFilterPipeline(predicate, scanProjection, options, rowGroupChunks());
    }

    /**
     * Returns the plan executed by one drain of {@code predicate} under {@code projection}, annotated with the
     * execution stats of the drain. The drain runs the read of {@link #read} and discards each row: it decodes the
     * projected columns (plus the column-index, offset-index, and bloom-filter sections read by the filter pipeline),
     * runs the record-level filter, and shapes the rows of a shaping projection, allocating no per-row output. This
     * holds for any predicate, including an always-true full scan, whose execution annotation reports the real cost of
     * reading the projected columns.
     *
     * <p>The plan's {@link ExplainPlan#estimatedBytesRead() estimatedBytesRead} is the planner's projection-based upper
     * bound and is a different measure from the {@link QueryStats#totalFetch() totalFetch} the drain actually read.
     * Per-row-group fetch bytes are not yet attributed and read as zero; the query-level fetch total is authoritative.
     */
    public ExplainPlan explainAnalyze(
            @NonNull Predicate predicate, @NonNull Projection projection, @NonNull ReadOptions options) {
        QueryObservation observation = QueryObservation.analyze(options);
        PlannedRows<Boolean> drained = readRows(predicate, projection, DISCARD_MATERIALIZER, observation);
        try (Stream<Boolean> rows = drained.rows()) {
            rows.forEach(_ -> {});
        }
        QueryStats stats = observation.finish();
        return drained.plan().withExecution(stats, observation.rowGroupReads());
    }

    /**
     * Binds index-section reads to this reader's {@link ByteRangeSource} and records each section's bytes into
     * {@code accumulator}: a column-index read as {@link FetchPurpose#COLUMN_INDEX}, an offset-index read as
     * {@link FetchPurpose#OFFSET_INDEX}, and a bloom-filter read as {@link FetchPurpose#BLOOM_FILTER}. The recorded
     * byte count is the section's on-disk length. A bloom filter whose length the writer recorded counts the full chunk
     * (header plus bitset); one whose length is absent counts the bitset byte size discovered from the filter header,
     * the only figure cheaply available on that path. Passing {@link FetchAccumulator#NONE} reduces every record call
     * to a no-op.
     */
    private IndexSectionLoader indexSectionLoader(FetchAccumulator accumulator) {
        return new FooterIndexSectionLoader(source, accumulator, columnChunkBytes);
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
        int rowGroupCount = compactFooter.rowGroupCount();
        List<RowGroupChunks> chunks = new ArrayList<>(rowGroupCount);
        for (int rowGroupIndex = 0; rowGroupIndex < rowGroupCount; rowGroupIndex++) {
            chunks.add(RowGroupChunks.of(compactFooter, rowGroupIndex, leafIndex, fileSchema, loader));
        }
        return chunks;
    }

    /**
     * Runs {@link #runFilterPipeline} with the options of {@code observation} and, when the observers asked for
     * timings, reports the elapsed nanoseconds to it. When timings are off, no clock is read.
     */
    private ExplainPlan timedFilterPipeline(
            Predicate predicate,
            Projection projection,
            List<RowGroupChunks> rowGroupChunks,
            QueryObservation observation) {
        ReadOptions options = observation.options();
        if (!observation.wantsTimings()) {
            return runFilterPipeline(predicate, projection, options, rowGroupChunks);
        }
        long start = System.nanoTime();
        ExplainPlan plan = runFilterPipeline(predicate, projection, options, rowGroupChunks);
        observation.addPipelineNanos(System.nanoTime() - start);
        return plan;
    }

    /** Builds inputs for and runs the filter pipeline. */
    private ExplainPlan runFilterPipeline(
            Predicate predicate, Projection projection, ReadOptions options, List<RowGroupChunks> rowGroupChunks) {
        List<ColumnPath> projectedLeaves = projectedLeafColumns(projection);
        boolean rowPositionDeletes = !Predicate.rowPositionColumns(predicate).isEmpty();
        List<FilterPipeline.RowGroupInputs> inputs =
                filterInputsFor(rowGroupChunks, options, projectedLeaves, rowPositionDeletes);
        Set<Tier> disabledTiers = disabledTiers(options);
        ExplainPlan plan = FilterPipeline.evaluate(fileSchema, projection, predicate, inputs, disabledTiers);
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
        Map<RowGroupChunks, Long> fileOffsets = rowPositionDeletes ? rowGroupFileOffsets(rowGroupChunks) : Map.of();
        List<FilterPipeline.RowGroupInputs> inputs = new ArrayList<>(rowGroupChunks.size());
        for (RowGroupChunks chunks : rowGroupChunks) {
            inputs.add(new FilterPipeline.RowGroupInputs(
                    chunks.numRows(),
                    statsLookup(chunks),
                    spatialBounds,
                    pageIndexLookupFor(chunks),
                    bloomLookupFor(chunks),
                    projectedCompressedBytes(chunks, projectedLeaves),
                    rowPositionDeletes ? fileOffsets.get(chunks) : 0L,
                    rowPositionDeletes ? rowPositionPages(chunks, options) : Optional.empty()));
        }
        return inputs;
    }

    /**
     * The pages of any one leaf column, used by the row-position delete tier to find fully-deleted pages. The
     * synthesized {@code $pos} column has no page index of its own. The columns of a row group share the same row
     * boundaries, hence the pages of any real leaf partition the row group into spans usable by the position math.
     * Empty when the column-index filter is off or no leaf has a page index, in which case the page tier stays inert.
     */
    private Optional<PageIndex> rowPositionPages(RowGroupChunks chunks, ReadOptions options) {
        // The page boundaries cost an offset-index read, skipped when the page tier is off.
        if (!options.useColumnIndexFilter()) {
            return Optional.empty();
        }
        for (ColumnPath leaf : fileSchema.leafColumns()) {
            Optional<PageIndex> pages = chunks.pageLocations(leaf);
            if (pages.isPresent()) {
                return pages;
            }
        }
        return Optional.empty();
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
            total += chunks.chunk(leaf).map(ParquetFileReader::estimableBytes).orElse(0);
        }
        return total;
    }

    /**
     * The bytes read by a fetch of {@code chunk}, counted toward the estimate alone. Any recorded length that no fetch
     * can span - the unaddressable placeholder included - counts as nothing: an estimate short of the truth beats one
     * dragged below zero.
     */
    private static int estimableBytes(ChunkMeta chunk) {
        return Math.max(0, chunk.totalCompressedSize());
    }

    /**
     * Returns the inline statistics lookup for {@code chunks}. Each call to the returned lookup delegates to
     * {@link RowGroupChunks#stats}, which reads from the in-footer column metadata without any I/O.
     */
    private StatsEvaluator.TypedColumns statsLookup(RowGroupChunks chunks) {
        return chunks::summary;
    }

    /**
     * Returns the column-index tier lookup for {@code chunks}. Each call to the returned lookup delegates to
     * {@link RowGroupChunks#pageIndex}, which memoizes the result: each column's index sections are read at most once.
     * The tier warms each consultable column before the first lookup, and the row group reads their index sections in
     * one call.
     */
    private PageIndexLookup pageIndexLookupFor(RowGroupChunks chunks) {
        return new PageIndexLookup() {
            @Override
            public Optional<PageIndex> get(ColumnPath path) {
                return chunks.pageIndex(path);
            }

            @Override
            public void warm(List<ColumnPath> paths) {
                chunks.warmPageIndexes(paths);
            }
        };
    }

    /**
     * Returns the bloom-filter lookup for {@code chunks}. Each call to the returned lookup delegates to
     * {@link RowGroupChunks#bloom}, which memoizes the result: each column's bloom filter is read at most once.
     */
    private BloomFilterLookup bloomLookupFor(RowGroupChunks chunks) {
        return chunks::bloom;
    }

    /** The tiers turned off by {@code options}; the pipeline reports them as not applied on each row group. */
    private static Set<Tier> disabledTiers(ReadOptions options) {
        Set<Tier> disabled = EnumSet.noneOf(Tier.class);
        if (!options.useStatsFilter()) {
            disabled.add(Tier.STATS);
        }
        if (!options.useColumnIndexFilter()) {
            disabled.add(Tier.COLUMN_INDEX);
        }
        if (!options.useBloomFilter()) {
            disabled.add(Tier.BLOOM_FILTER);
        }
        if (!options.useRecordLevelFilter()) {
            disabled.add(Tier.RECORD_LEVEL);
        }
        return disabled;
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
     * and the scanned columns flat, each with a page index. Otherwise the entry is empty and the row group decodes in
     * full.
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

    private static Optional<RowMask> maskFor(RowGroupSurvivor survivor, ParquetSchema scanSchema) {
        if (survivor.survivingRows().isEmpty()) {
            return Optional.empty();
        }
        RowRanges surviving = survivor.survivingRows().orElseThrow();
        return RowMasks.maskFor(survivor.chunks(), surviving, scanSchema.leafColumns());
    }

    /** True when every {@code leaf} is non-repeated (max repetition level 0) in {@code schema}. Pure; testable. */
    static boolean allFlat(ParquetSchema schema, List<ColumnPath> leaves) {
        return RowMasks.allFlat(schema, leaves);
    }
}
