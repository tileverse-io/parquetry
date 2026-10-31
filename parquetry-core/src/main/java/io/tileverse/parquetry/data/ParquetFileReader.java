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
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.RowGroupOutcome;
import io.tileverse.parquetry.filter.explain.RowGroupPlan;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.internal.filter.spatial.BoundsAccumulator;
import io.tileverse.parquetry.internal.filter.spatial.SpatialCoveringRewrite;
import io.tileverse.parquetry.internal.read.BatchForm;
import io.tileverse.parquetry.internal.read.BatchPipeline;
import io.tileverse.parquetry.internal.read.DecryptionKeyRetriever;
import io.tileverse.parquetry.internal.read.MaskedScan;
import io.tileverse.parquetry.internal.read.ParallelDecodeCoordinator;
import io.tileverse.parquetry.internal.read.RowGroupChunks;
import io.tileverse.parquetry.internal.read.RowGroupSurvivor;
import io.tileverse.parquetry.internal.read.RowPositionColumn;
import io.tileverse.parquetry.internal.read.SpatialDecimationGate;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
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

    private final FooterMetadata footer;
    private final ReadPlanner readPlanner;
    private final SpatialReadGates spatialReadGates;

    // Held for an encrypted file; the footer-decryption path that reads it is not wired in yet.
    @SuppressWarnings("java:S1068")
    private final Optional<DecryptionKeyRetriever> decryptionKeyRetriever;

    private ParquetFileReader(
            ByteRangeSource source,
            FooterMetadata footerMetadata,
            ParquetRuntime runtime,
            Optional<DecryptionKeyRetriever> decryptionKeyRetriever) {
        this.footer = footerMetadata;
        this.spatialReadGates = new SpatialReadGates(
                footer.primaryGeometry(), footer.spatialBounds(), footer.fileSchema(), footer.geoMetadata());
        ReadResources readResources = new ReadResources(source, footer.fileSchema(), runtime);
        this.readPlanner = new ReadPlanner(source, footer, readResources, spatialReadGates);
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
        return footer.fileSchema();
    }

    /** Returns the file's key/value metadata, collapsed to the last value per key. */
    public Map<String, String> keyValueMetadata() {
        return footer.keyValueMetadata();
    }

    /** Returns a public view of the row groups in file order. */
    public List<RowGroupSummary> rowGroups() {
        return footer.rowGroups();
    }

    /**
     * The forms derived from the footer of this file and held in place of its decoded wire records: the packed planning
     * footer, the leaf index and the spatial bounds among them. They are immutable and shared between the readers
     * opened on the same file.
     */
    FooterMetadata footer() {
        return footer;
    }

    /**
     * The footer-aggregated prunable statistics for this file: per-column min/max/null-count combined across the file's
     * row groups, the geometry bounding box for each geometry column, and the record count. Reads only the cached
     * footer metadata, no page data. The result feeds {@link io.tileverse.parquetry.filter.prune.FilePruner} for
     * whole-file pruning, mirroring the Iceberg manifest-bounds path with the footer as the source.
     */
    public FileStats fileStats() {
        return new FileStatsAggregator(footer.spatialBounds(), footer.fileSchema(), footer.geoMetadata())
                .aggregate(readPlanner.chunks());
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
                ProjectionPlan.resolve(footer.fileSchema(), projection, predicate, options.useRecordLevelFilter());
        List<RowPositionColumn> rowPositionRequests = projectionPlan.synthesizedColumns();
        boolean recordLevel = projectionPlan.recordLevel();
        Projection scanProjection = projectionPlan.scanProjection();
        ReadPlan plan = readPlanner.plan(predicate, scanProjection, observation);
        List<RowGroupSurvivor> survivors = plan.survivors();
        ParquetSchema outputSchema = projectionPlan.physicalOutputSchema();
        Optional<MaskedScan> maskedScan = recordLevel ? plan.maskedScan(outputSchema) : Optional.empty();
        Predicate recordFilter = (recordLevel && maskedScan.isEmpty()) ? plan.recordFilter() : null;
        BatchForm rowsBatchForm = maskedScan.isPresent() ? BatchForm.LEVELS : rowsForm(recordFilter);
        DecodeShape.SpatialPlanning spatialPlanning = new DecodeShape.SpatialPlanning(rawPredicate, outputSchema);
        DecodeShape shape =
                new DecodeShape(maskedScan, rowsBatchForm, false, rowPositionRequests, Optional.of(spatialPlanning));
        ParallelDecodeCoordinator coordinator = plan.decode(survivors, shape);
        Optional<SpatialDecimationGate> leafGate = spatialReadGates.leafGate(options);
        Stream<T> rows = BatchPipeline.rows(
                coordinator,
                materializer,
                outputSchema,
                recordFilter,
                observation.observing(),
                observation.wantsTimings(),
                leafGate);
        return new PlannedRows<>(plan.explainPlan(), rows);
    }

    /** A row stream and its plan. */
    private record PlannedRows<T>(ExplainPlan plan, Stream<T> rows) {}

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
     * The physical scan projection for {@code count}: the predicate's physical columns minus the synthesized
     * row-position columns, or a cheap driving leaf when the predicate names only synthesized columns. count produces
     * nothing, hence it needs only the columns the predicate evaluates plus a column to enumerate the file's rows.
     */
    private Projection predicateScanProjection(Predicate predicate) {
        Set<ColumnPath> columns = ReadPlanner.physicalColumns(predicate);
        if (columns.isEmpty()) {
            columns = Set.of(footer.fileSchema().leafColumns().get(0));
        }
        return Projection.ofPhysical(columns);
    }

    /**
     * Rewrites GeoParquet bbox-relation leaves into equivalent comparisons on the geometry column's covering columns
     * when this file has covering metadata, letting the numeric stats and column-index tiers prune without decoding the
     * geometry. A file without covering keeps its spatial leaf for the record-level WKB path.
     */
    private Predicate lowerSpatialPredicates(Predicate predicate) {
        return SpatialCoveringRewrite.expand(predicate, footer.fileSchema(), footer.geoMetadata());
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
        List<RowPositionColumn> rowPositionRequests = ProjectionPlan.rowPositionColumns(footer.fileSchema(), predicate);
        Projection predicateProjection = predicateScanProjection(predicate);
        ReadPlan plan = readPlanner.plan(predicate, predicateProjection, observation);

        long matchedRows = matchedRowCount(plan.explainPlan(), observation);
        List<RowGroupSurvivor> residual = plan.residual();
        if (residual.isEmpty()) {
            return matchedRows;
        }
        return matchedRows + countResidual(residual, plan, observation, rowPositionRequests);
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
     * Decodes the predicate columns of the residual row groups and counts the matches. The masked scan applies the
     * predicate during decode and produces nothing but row counts; where it does not run, each decoded batch is tested
     * by a columnar popcount.
     */
    private long countResidual(
            List<RowGroupSurvivor> residual,
            ReadPlan plan,
            QueryObservation observation,
            List<RowPositionColumn> rowPositionRequests) {
        ReadOptions options = observation.options();
        Predicate normalized = plan.normalized();
        Optional<SpatialDecimationGate> leafGate = spatialReadGates.leafGate(options);
        Optional<MaskedScan> maskedScan = countMaskedScan(plan, leafGate);
        DecodeShape shape = new DecodeShape(maskedScan, BatchForm.LEVELS, false, rowPositionRequests, Optional.empty());
        ParallelDecodeCoordinator coordinator = plan.decode(residual, shape);
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
    private Optional<MaskedScan> countMaskedScan(ReadPlan plan, Optional<SpatialDecimationGate> leafGate) {
        if (leafGate.isPresent()) {
            return Optional.empty();
        }
        ParquetSchema noOutputColumns = footer.fileSchema().project(Set.of());
        return plan.maskedScan(noOutputColumns);
    }

    /** Total rows across every row group, read from the per-row-group summaries with no I/O. */
    private long totalRows() {
        long sum = 0L;
        for (RowGroupSummary rowGroup : footer.rowGroups()) {
            sum += rowGroup.rowCount();
        }
        return sum;
    }

    /**
     * The exact bounding box of the primary geometry column over the rows matching {@code rawPredicate}, or empty when
     * the file has no geometry column or no matching row holds a geometry with an extent (null and empty geometries add
     * nothing). The box is 2D-exact; its Z and M extents are present only when the whole answer came from metadata
     * boxes, and any scanned row drops them. The primary geometry column is the one declared by the GeoParquet metadata
     * when it is a geometry column, else the first geometry column in schema order (see
     * {@link GeometryColumns#primary}).
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
        Optional<ColumnPath> geometryColumn = footer.primaryGeometry();
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
     * Bounds for an unfiltered read of {@code geometryColumn}: the tight file-level metadata box when the file records
     * one (zero I/O), otherwise the union of every row group's tight metadata box with a scan of the row groups that
     * expose none. A metadata box wrapping the antimeridian is answered as its planar enclosure, and a file box without
     * an extent is treated as absent. A row group without rows has no box and nothing to scan: it is skipped.
     */
    private Optional<BoundingBox> unfilteredBounds(ColumnPath geometryColumn, ReadOptions options) {
        Optional<BoundingBox> fileBox =
                footer.spatialBounds().fileBounds(geometryColumn).filter(BoundingBox::hasExtent);
        if (fileBox.isPresent()) {
            return fileBox.map(BoundingBox::planarEnclosure);
        }
        return unionRowGroupBoundsAndScanTheRest(geometryColumn, options);
    }

    private Optional<BoundingBox> unionRowGroupBoundsAndScanTheRest(ColumnPath geometryColumn, ReadOptions options) {
        BoundsAccumulator accumulator = new BoundsAccumulator();
        List<RowGroupChunks> rowGroupChunks = readPlanner.chunks();
        List<RowGroupSurvivor> boxLess = new ArrayList<>();
        for (int i = 0; i < rowGroupChunks.size(); i++) {
            RowGroupChunks chunks = rowGroupChunks.get(i);
            Optional<BoundingBox> box = footer.spatialBounds().rowGroupBounds(geometryColumn, i);
            if (box.isPresent()) {
                accumulator.union(box.orElseThrow());
            } else if (chunks.numRows() != 0L) {
                boxLess.add(RowGroupSurvivor.full(chunks));
            }
        }
        if (!boxLess.isEmpty()) {
            ParquetSchema scanSchema = footer.fileSchema().project(Set.of(geometryColumn));
            ParallelDecodeCoordinator coordinator = readPlanner.decodeUnplanned(boxLess, scanSchema, options);
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
        List<RowPositionColumn> rowPositionRequests = ProjectionPlan.rowPositionColumns(footer.fileSchema(), predicate);
        Projection scanProjection = boundsScanProjection(predicate, geometryColumn);
        ReadPlan plan = readPlanner.plan(predicate, scanProjection, observation);
        List<ResidualGroup> residual = unionMatchedAndCollectResidual(plan, geometryColumn, accumulator);
        scanResidualBounds(residual, plan, geometryColumn, accumulator, rowPositionRequests);
        return accumulator.snapshot();
    }

    /** The physical scan projection for bounds: the predicate's columns plus the geometry column the residue folds. */
    private Projection boundsScanProjection(Predicate predicate, ColumnPath geometryColumn) {
        Set<ColumnPath> columns = new LinkedHashSet<>(ReadPlanner.physicalColumns(predicate));
        columns.add(geometryColumn);
        return Projection.ofPhysical(columns);
    }

    /**
     * Pass one: union the tight geometry box of every row group whose statistics prove all rows match, and collect the
     * rest (FULL, PARTIAL, and box-less all-match groups) for the residual scan. A box-less all-match group joins the
     * residual as a full survivor: all its rows match, only its extent is unknown.
     */
    private List<ResidualGroup> unionMatchedAndCollectResidual(
            ReadPlan plan, ColumnPath geometryColumn, BoundsAccumulator accumulator) {
        List<ResidualGroup> residual = new ArrayList<>();
        for (RowGroupPlan rgPlan : plan.explainPlan().rowGroups()) {
            Optional<BoundingBox> box = footer.spatialBounds().rowGroupBounds(geometryColumn, rgPlan.index());
            switch (rgPlan.outcome()) {
                case ELIMINATED -> {
                    /* pruned; contributes nothing */
                }
                case MATCHED -> {
                    RowGroupChunks chunks = plan.chunks().get(rgPlan.index());
                    unionOrCollectMatched(chunks, box, accumulator, residual);
                }
                case FULL, PARTIAL -> residual.add(new ResidualGroup(plan.survivorFor(rgPlan), box));
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
     *
     * <p>The masked scan applies the predicate during decode and emits the geometry of the surviving rows alone. Where
     * it does not run, the fold tests each decoded row.
     */
    private void scanResidualBounds(
            List<ResidualGroup> residual,
            ReadPlan plan,
            ColumnPath geometryColumn,
            BoundsAccumulator accumulator,
            List<RowPositionColumn> rowPositionRequests) {
        ParquetSchema geometryOnly = footer.fileSchema().project(Set.of(geometryColumn));
        Optional<MaskedScan> maskedScan = plan.maskedScan(geometryOnly);
        Predicate foldFilter = maskedScan.isPresent() ? null : plan.normalized();
        DecodeShape shape = new DecodeShape(maskedScan, BatchForm.LEVELS, false, rowPositionRequests, Optional.empty());
        for (ResidualGroup group : orderedByDescendingArea(residual)) {
            if (isCovered(group, accumulator)) {
                continue;
            }
            ParallelDecodeCoordinator coordinator = plan.decode(List.of(group.survivor()), shape);
            BatchPipeline.boundsMatching(coordinator, foldFilter, geometryColumn, accumulator);
        }
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
                ProjectionPlan.resolve(footer.fileSchema(), projection, predicate, options.useRecordLevelFilter());
        List<RowPositionColumn> rowPositionRequests = projectionPlan.synthesizedColumns();
        boolean recordLevel = projectionPlan.recordLevel();
        Projection scanProjection = projectionPlan.scanProjection();
        ReadPlan plan = readPlanner.plan(predicate, scanProjection, observation);
        List<RowGroupSurvivor> survivors = plan.survivors();
        ParquetSchema outputSchema = projectionPlan.physicalOutputSchema();
        Optional<MaskedScan> maskedScan = recordLevel ? plan.maskedScan(outputSchema) : Optional.empty();
        Predicate recordFilter = (recordLevel && maskedScan.isEmpty()) ? plan.recordFilter() : null;
        Optional<SpatialDecimationGate> leafGate = spatialReadGates.leafGate(options);
        // A masked scan still narrows: a MATCHED row group takes the full-decode driver, whose batches expose the scan
        // columns rather than the caller's output columns. Emitting the decoded shape is therefore also the one case
        // where nothing downstream drops a row, which lets every decoded row be reported as matched; any other shape
        // tallies the rows it emits.
        boolean emitDecodedShape = recordFilter == null && leafGate.isEmpty() && maskedScan.isEmpty();
        DecodeShape.SpatialPlanning spatialPlanning = new DecodeShape.SpatialPlanning(rawPredicate, outputSchema);
        DecodeShape shape = new DecodeShape(
                maskedScan, BatchForm.ASSEMBLED, emitDecodedShape, rowPositionRequests, Optional.of(spatialPlanning));
        ParallelDecodeCoordinator coordinator = plan.decode(survivors, shape);
        Stream<ParquetRecordBatch> batches = emitDecodedShape
                ? BatchPipeline.batches(coordinator)
                : BatchPipeline.batches(coordinator, recordFilter, outputSchema, leafGate);
        Stream<ParquetRecordBatch> produced =
                projectionPlan.needsShaping() ? batches.map(projectionPlan::produce) : batches;
        ParquetSchema producedSchema = projectionPlan.producedSchema();
        return new PlannedRows<>(
                plan.explainPlan(), produced.map(batch -> materializer.materialize(producedSchema, batch)));
    }

    /**
     * Runs the filter pipeline without reading data and returns the explain plan. The plan covers the columns scanned
     * by a read of {@code projection}: with the record-level filter on, they include the columns of the predicate.
     */
    public ExplainPlan explain(
            @NonNull Predicate rawPredicate, @NonNull Projection projection, @NonNull ReadOptions options) {
        Predicate predicate = lowerSpatialPredicates(rawPredicate);
        ProjectionPlan projectionPlan =
                ProjectionPlan.resolve(footer.fileSchema(), projection, predicate, options.useRecordLevelFilter());
        Projection scanProjection = projectionPlan.scanProjection();
        return readPlanner.explain(predicate, scanProjection, options);
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
}
