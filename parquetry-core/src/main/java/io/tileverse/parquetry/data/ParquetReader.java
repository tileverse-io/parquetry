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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.stream.Stream;

import io.tileverse.parquetry.batch.BatchMaterializer;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.RowGroupOutcome;
import io.tileverse.parquetry.filter.explain.RowGroupPlan;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.KeyValue;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.filter.FilterPipeline;
import io.tileverse.parquetry.internal.filter.FilterPipeline.BloomFilterLookup;
import io.tileverse.parquetry.internal.filter.FilterPipeline.ColumnPageStatsLookup;
import io.tileverse.parquetry.internal.filter.FilterPipeline.ColumnStatsLookup;
import io.tileverse.parquetry.internal.filter.StatsEvaluator;
import io.tileverse.parquetry.internal.filter.StatsEvaluator.ColumnSummary;
import io.tileverse.parquetry.internal.filter.bloom.BloomFilterReader;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.internal.filter.prune.FooterStatsAggregator;
import io.tileverse.parquetry.internal.filter.spatial.SpatialBoundsSource;
import io.tileverse.parquetry.internal.filter.spatial.SpatialCoveringRewrite;
import io.tileverse.parquetry.internal.read.BatchForm;
import io.tileverse.parquetry.internal.read.BatchPipeline;
import io.tileverse.parquetry.internal.read.DecodeBufferAllocator;
import io.tileverse.parquetry.internal.read.DecryptionKeyRetriever;
import io.tileverse.parquetry.internal.read.FetchBufferAllocator;
import io.tileverse.parquetry.internal.read.FetchSpillStore;
import io.tileverse.parquetry.internal.read.IndexSectionLoader;
import io.tileverse.parquetry.internal.read.LateMaterialization;
import io.tileverse.parquetry.internal.read.ParallelDecodeCoordinator;
import io.tileverse.parquetry.internal.read.ParallelDecodeCoordinator.DecodeObservation;
import io.tileverse.parquetry.internal.read.RowGroupChunks;
import io.tileverse.parquetry.internal.read.RowGroupFetcher;
import io.tileverse.parquetry.internal.read.RowGroupPrefetcher;
import io.tileverse.parquetry.internal.read.RowGroupSurvivor;
import io.tileverse.parquetry.internal.read.RowMask;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.observe.FetchPurpose;
import io.tileverse.parquetry.observe.FetchStats;
import io.tileverse.parquetry.observe.PhaseTimings;
import io.tileverse.parquetry.observe.QueryObserver;
import io.tileverse.parquetry.observe.QueryStarted;
import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.observe.QueryStatsCollector;
import io.tileverse.parquetry.observe.RowGroupRead;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaBuilder;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

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
 *
 * <p>The class is non-final and the per-row-group filter helpers ({@link #runFilterPipeline runFilterPipeline},
 * {@link #filterInputsFor filterInputsFor}, {@link #bloomLookupFor bloomLookupFor}, {@link #statsLookup statsLookup},
 * {@link #survivorsFor survivorsFor}) are {@code protected}. A subclass can specialize how stats, bloom filters, or
 * column page indexes are sourced (for example, to plug in a cached or remote lookup) without re-implementing the rest
 * of the read pipeline.
 */
public class ParquetReader {

    private static final String GEO_KEY = "geo";

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

    // Held for an encrypted file; the footer-decryption path that reads it is not wired in yet.
    @SuppressWarnings("java:S1068")
    private final Optional<DecryptionKeyRetriever> decryptionKeyRetriever;

    protected ParquetReader(
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
        this.geoMetadata = parseGeoMetadata(keyValueMetadata);
        this.runtime = runtime;
        this.decryptionKeyRetriever = decryptionKeyRetriever;
    }

    /**
     * Opens a reader against the default runtime, performing exactly one footer read against {@code source}. Equivalent
     * to {@code open(source, ParquetRuntime.defaultRuntime(), Optional.empty())}.
     */
    public static ParquetReader open(@NonNull ByteRangeSource source) {
        return open(source, ParquetRuntime.defaultRuntime(), Optional.empty());
    }

    /**
     * Opens a reader bound to {@code runtime} for read resources, performing exactly one footer read against
     * {@code source}. The {@code decryptionKeyRetriever} is held for an encrypted file.
     */
    public static ParquetReader open(
            @NonNull ByteRangeSource source,
            @NonNull ParquetRuntime runtime,
            @NonNull Optional<DecryptionKeyRetriever> decryptionKeyRetriever) {
        FileMetaData footer = ParquetFormat.readFooter(source);
        Map<String, String> kvMetadata = collapseKeyValueMetadata(footer.keyValueMetadata());
        ParquetSchema fileSchema = buildFileSchema(footer, kvMetadata);
        List<RowGroupSummary> rgView = toRowGroupView(footer);
        return new ParquetReader(source, footer, fileSchema, kvMetadata, rgView, runtime, decryptionKeyRetriever);
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
        List<RowGroupChunks> groups = rowGroupChunks();
        FileStats.Builder builder = FileStats.builder().recordCount(recordCount(groups));
        Set<ColumnPath> geometryColumns = geometryColumnPaths();
        for (ColumnPath leaf : fileSchema.leafColumns()) {
            if (geometryColumns.contains(leaf)) {
                continue;
            }
            FooterStatsAggregator.aggregate(columnSummaries(groups, leaf))
                    .ifPresent(stats -> builder.column(leaf, stats));
        }
        addGeometryBounds(builder, geometryColumns);
        return builder.build();
    }

    private static long recordCount(List<RowGroupChunks> groups) {
        long total = 0L;
        for (RowGroupChunks group : groups) {
            total += group.numRows();
        }
        return total;
    }

    private static List<Optional<ColumnSummary>> columnSummaries(List<RowGroupChunks> groups, ColumnPath leaf) {
        List<Optional<ColumnSummary>> summaries = new ArrayList<>(groups.size());
        for (RowGroupChunks group : groups) {
            summaries.add(safeSummary(group, leaf));
        }
        return summaries;
    }

    /**
     * Decodes one row group's statistics for {@code leaf}, best-effort: a truncated or malformed footer value degrades
     * that row group's column to no statistics rather than failing the read. The aggregate then leaves the column out
     * of pruning. This mirrors the Iceberg manifest path, which never lets an unreadable bound fail a read.
     */
    private static Optional<ColumnSummary> safeSummary(RowGroupChunks group, ColumnPath leaf) {
        Optional<FilterPipeline.ColumnStats> stats = group.stats(leaf);
        if (stats.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(StatsEvaluator.summarize(stats.orElseThrow()));
        } catch (RuntimeException _) {
            // A malformed or truncated footer value disables pruning for this column; it never fails the read.
            return Optional.empty();
        }
    }

    /**
     * The geometry leaf columns, kept out of the numeric statistics. A GeoParquet 1.x file names its (top-level)
     * geometry columns only in the {@code geo} metadata; a GeoParquet 2.0 file annotates the leaf with a
     * GEOMETRY/GEOGRAPHY logical type. Honoring both keeps a WKB column out of {@code columns()} regardless of which
     * encoding wrote it, even when the two disagree.
     */
    private Set<ColumnPath> geometryColumnPaths() {
        Set<ColumnPath> paths = new LinkedHashSet<>();
        geoMetadata.ifPresent(geo -> geo.columns().keySet().forEach(name -> paths.add(ColumnPath.of(name))));
        for (ColumnPath leaf : fileSchema.leafColumns()) {
            if (isGeometryLeaf(leaf)) {
                paths.add(leaf);
            }
        }
        return paths;
    }

    private boolean isGeometryLeaf(ColumnPath leaf) {
        return fileSchema.find(leaf).orElse(null) instanceof SchemaNode.Primitive primitive
                && primitive.logicalType().map(ParquetReader::isGeometryLike).orElse(false);
    }

    private static boolean isGeometryLike(LogicalType logicalType) {
        return logicalType instanceof LogicalType.Geometry || logicalType instanceof LogicalType.Geography;
    }

    private void addGeometryBounds(FileStats.Builder builder, Set<ColumnPath> geometryColumns) {
        if (geometryColumns.isEmpty()) {
            return;
        }
        SpatialBoundsSource bounds = SpatialBoundsSource.of(footer, fileSchema, geoMetadata);
        for (ColumnPath geometry : geometryColumns) {
            bounds.fileBounds(geometry).ifPresent(box -> builder.geometryBounds(geometry, box));
        }
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

        Observation observation = observe(options);
        Stream<T> rows = readRows(rawPredicate, projection, materializer, observation);
        return observation.onClose(rows);
    }

    private <T> Stream<T> readRows(
            Predicate rawPredicate, Projection projection, Materializer<T> materializer, Observation observation) {

        return buildRowStream(
                rawPredicate,
                projection,
                materializer,
                observation.effectiveOptions(),
                FetchAccumulator.NONE,
                observation::addPipelineNanos);
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
            LongConsumer pipelineNanosSink) {

        Predicate predicate = lowerSpatialPredicates(rawPredicate);
        boolean recordLevel = options.useRecordLevelFilter();
        Projection scanProjection = recordLevel ? scanProjectionFor(projection, predicate) : projection;
        List<RowGroupChunks> rowGroupChunks = rowGroupChunks(accumulator);
        boolean observe = observing(options);
        boolean wantsTimings = observe && options.queryObserver().wantsTimings();
        ExplainPlan plan = timedFilterPipeline(
                predicate, scanProjection, options, rowGroupChunks, wantsTimings, pipelineNanosSink);
        List<RowGroupSurvivor> survivors = survivorsFor(plan, rowGroupChunks);
        ParquetSchema scanSchema = plan.projectedSchema();
        ParquetSchema outputSchema = outputSchemaFor(projection);
        List<Optional<RowMask>> decodeMasks = decodeMasksFor(survivors, scanSchema, options);
        Predicate normalized = plan.normalizedPredicate();
        Optional<LateMaterialization> lateMat =
                lateMaterializationFor(survivors, scanSchema, outputSchema, normalized, options, recordLevel);
        Predicate recordFilter = (recordLevel && lateMat.isEmpty()) ? recordFilterOf(normalized) : null;
        DecodeObservation decodeObservation = decodeObservationFor(plan, observe, options.queryObserver(), false);
        ParallelDecodeCoordinator coordinator = newDecodeCoordinator(
                survivors,
                scanSchema,
                decodeMasks,
                options,
                lateMat,
                rowsForm(recordFilter),
                accumulator,
                decodeObservation);
        return BatchPipeline.rows(coordinator, materializer, outputSchema, recordFilter, observe, wantsTimings);
    }

    /** True when an observer is attached; the read paths skip every observability allocation when it is false. */
    private static boolean observing(ReadOptions options) {
        return options.queryObserver() != QueryObserver.NONE;
    }

    /**
     * The per-query observability binding for a decoding entry point. When an observer is attached it composes an
     * internal {@link QueryStatsCollector} ahead of the user observer (the read runs against the composite, feeding
     * both) and, at query end, stamps the collector's finish, snapshots it, and delivers the aggregate to the user
     * observer exactly once. When no observer is attached it is a pass-through: the original options run unchanged and
     * nothing fires, which keeps the decode path byte-identical.
     */
    private static Observation observe(ReadOptions options) {
        if (!observing(options)) {
            return Observation.passThrough(options);
        }
        QueryObserver userObserver = options.queryObserver();
        QueryStatsCollector internal = new QueryStatsCollector();
        QueryObserver effectiveObserver = QueryObserver.composite(internal, userObserver);
        ReadOptions effectiveOptions =
                options.toBuilder().queryObserver(effectiveObserver).build();
        return new Observation(effectiveOptions, internal, userObserver);
    }

    private static final class Observation {

        private final ReadOptions effectiveOptions;
        private final QueryStatsCollector internal;
        private final QueryObserver userObserver;

        // The query-level filter-pipeline time, recorded once on the reading thread before the stream is consumed and
        // folded into the final stats on the same thread at the finish. Stays zero when timings are off.
        private long pipelineNanos;

        private Observation(ReadOptions effectiveOptions, QueryStatsCollector internal, QueryObserver userObserver) {
            this.effectiveOptions = effectiveOptions;
            this.internal = internal;
            this.userObserver = userObserver;
        }

        static Observation passThrough(ReadOptions options) {
            return new Observation(options, null, null);
        }

        ReadOptions effectiveOptions() {
            return effectiveOptions;
        }

        /** Records the measured filter-pipeline nanoseconds, folded into the final stats at the finish. */
        void addPipelineNanos(long nanos) {
            pipelineNanos += nanos;
        }

        /** Chains the finish onto the stream's close for the lazy {@code read}/{@code readBatches} paths. */
        <T> Stream<T> onClose(Stream<T> stream) {
            if (userObserver == null) {
                return stream;
            }
            return stream.onClose(this::fireFinished);
        }

        /** Fires the finish for the eager {@code count} path; a no-op on the pass-through binding. */
        void fireFinishedIfObserving() {
            if (userObserver != null) {
                fireFinished();
            }
        }

        /** Stamps the internal collector's finish and delivers its snapshot to the user observer exactly once. */
        void fireFinished() {
            internal.onQueryFinished(null);
            QueryStats stats = internal.snapshot();
            if (pipelineNanos > 0L) {
                stats = withPipelineNanos(stats, pipelineNanos);
            }
            userObserver.onQueryFinished(stats);
        }
    }

    /** Folds the query-level pipeline time into {@code stats}' cpu timings, creating them when absent. */
    private static QueryStats withPipelineNanos(QueryStats stats, long pipelineNanos) {
        PhaseTimings pipeline = new PhaseTimings(pipelineNanos, 0L, 0L, 0L);
        PhaseTimings folded = stats.cpuTimings().map(pipeline::combine).orElse(pipeline);
        return stats.withCpuTimings(Optional.of(folded));
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
     * Expands {@code projection} to also include the predicate's columns, and hence record-level evaluation can read
     * them even when the caller did not project them. {@link Projection.All} already decodes every column.
     */
    private static Projection scanProjectionFor(Projection projection, Predicate predicate) {
        return switch (projection) {
            case Projection.All _ -> projection;
            case Projection.Columns(Set<ColumnPath> kept) -> {
                Set<ColumnPath> union = new LinkedHashSet<>(kept);
                union.addAll(Predicate.columns(predicate));
                yield Projection.of(union);
            }
        };
    }

    /** The schema rows are materialized through: exactly the caller's projection, not the expanded scan set. */
    private ParquetSchema outputSchemaFor(Projection projection) {
        return switch (projection) {
            case Projection.All _ -> fileSchema;
            case Projection.Columns(Set<ColumnPath> kept) -> fileSchema.project(kept);
        };
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
        Observation observation = observe(options);
        try {
            return countLowered(
                    predicate, observation.effectiveOptions(), FetchAccumulator.NONE, observation::addPipelineNanos);
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
            Predicate predicate, ReadOptions options, FetchAccumulator accumulator, LongConsumer pipelineNanosSink) {
        Projection predicateProjection = Projection.of(Predicate.columns(predicate));
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
        return matchedRows + countResidual(residual, plan, options, accumulator, observe);
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
            boolean observe) {
        ParquetSchema scanSchema = plan.projectedSchema();
        Predicate normalized = plan.normalizedPredicate();
        List<Optional<RowMask>> masks = decodeMasksFor(residual, scanSchema, options);
        DecodeObservation observation = residualObservationFor(plan, observe, options.queryObserver());
        ParallelDecodeCoordinator coordinator = newDecodeCoordinator(
                residual, scanSchema, masks, options, Optional.empty(), BatchForm.LEVELS, accumulator, observation);
        return BatchPipeline.countMatching(coordinator, normalized, observe);
    }

    /** Total rows across every row group, read from the per-row-group summaries with no I/O. */
    private long totalRows() {
        long sum = 0L;
        for (RowGroupSummary rowGroup : rowGroupView) {
            sum += rowGroup.rowCount();
        }
        return sum;
    }

    /** Streams record batches matching {@code predicate} under {@code projection} via the default batch shape. */
    public Stream<ParquetRecordBatch> readBatches(Predicate predicate, Projection projection, ReadOptions options) {
        return readBatches(predicate, projection, BatchMaterializer.defaultBatch(), options);
    }

    /**
     * Streams record batches matching {@code predicate} under {@code projection}, materialized via
     * {@code materializer}.
     */
    public <T> Stream<T> readBatches(
            @NonNull Predicate rawPredicate,
            @NonNull Projection projection,
            @NonNull BatchMaterializer<T> materializer,
            @NonNull ReadOptions options) {

        Observation observation = observe(options);
        Stream<T> batches = readBatchesLowered(rawPredicate, projection, materializer, observation);
        return observation.onClose(batches);
    }

    private <T> Stream<T> readBatchesLowered(
            Predicate rawPredicate, Projection projection, BatchMaterializer<T> materializer, Observation observation) {

        ReadOptions options = observation.effectiveOptions();
        Predicate predicate = lowerSpatialPredicates(rawPredicate);
        List<RowGroupChunks> rowGroupChunks = rowGroupChunks();
        boolean observe = observing(options);
        boolean wantsTimings = observe && options.queryObserver().wantsTimings();
        ExplainPlan plan = timedFilterPipeline(
                predicate, projection, options, rowGroupChunks, wantsTimings, observation::addPipelineNanos);
        List<RowGroupSurvivor> survivors = survivorsFor(plan, rowGroupChunks);
        ParquetSchema projectedSchema = plan.projectedSchema();
        List<Optional<RowMask>> decodeMasks = decodeMasksFor(survivors, projectedSchema, options);
        DecodeObservation decodeObservation = decodeObservationFor(plan, observe, options.queryObserver(), true);
        ParallelDecodeCoordinator coordinator = newDecodeCoordinator(
                survivors,
                projectedSchema,
                decodeMasks,
                options,
                Optional.empty(),
                BatchForm.ASSEMBLED,
                FetchAccumulator.NONE,
                decodeObservation);
        Stream<ParquetRecordBatch> batches = BatchPipeline.batches(coordinator);
        return batches.map(batch -> materializer.materialize(projectedSchema, batch));
    }

    /**
     * Builds the coalescing fetcher and the prefetch pipeline for one read. The prefetcher owns a fresh per-read
     * virtual-thread executor; closing the returned stream cascades to {@link RowGroupPrefetcher#close()}, which shuts
     * the executor down. No executor outlives the read.
     */
    private RowGroupPrefetcher newPrefetcher(
            List<RowGroupSurvivor> survivors,
            ParquetSchema projectedSchema,
            FetchAccumulator accumulator,
            boolean wantsTimings) {
        FetchSpillStore spillStore = new FetchSpillStore(runtime.spillDir(), runtime.diskBudget());
        FetchBufferAllocator mandatoryAllocator =
                new FetchBufferAllocator(runtime.segmentPool(), runtime.fetchBudget(), spillStore);
        RowGroupFetcher fetcher = new RowGroupFetcher(
                source,
                fileSchema,
                projectedSchema,
                runtime.segmentPool(),
                mandatoryAllocator,
                runtime.maxCoalesceGap(),
                runtime.maxCoalescedSpan(),
                accumulator);
        ExecutorService executor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("parquetry-fetch-", 0).factory());
        try {
            return new RowGroupPrefetcher(
                    survivors,
                    fetcher,
                    runtime.fetchBudget(),
                    executor,
                    runtime.prefetchDepth(),
                    runtime.maxConcurrentFetchesPerRead(),
                    wantsTimings);
        } catch (RuntimeException e) {
            executor.shutdownNow();
            throw e;
        }
    }

    /**
     * Wraps the fetch prefetcher in a {@link ParallelDecodeCoordinator} that decodes row groups in parallel on the
     * shared decode pool while preserving file order. Closing the returned coordinator drains in-flight decodes and
     * cascades to {@link RowGroupPrefetcher#close()}; the per-read fetch executor still shuts down with the read.
     */
    private ParallelDecodeCoordinator newDecodeCoordinator(
            List<RowGroupSurvivor> survivors,
            ParquetSchema projectedSchema,
            List<Optional<RowMask>> decodeMasks,
            ReadOptions options,
            Optional<LateMaterialization> lateMat,
            BatchForm batchForm,
            FetchAccumulator accumulator,
            DecodeObservation observation) {
        RowGroupPrefetcher prefetcher =
                newPrefetcher(survivors, projectedSchema, accumulator, observation.wantsTimings());
        List<Boolean> recordEvalRequired = recordEvalFlagsFor(survivors);
        DecodeBufferAllocator decodeBufferAllocator = newDecodeBufferAllocator();
        return new ParallelDecodeCoordinator(
                prefetcher,
                runtime.decodeExecutor(),
                runtime.decodeBudget(),
                decodeBufferAllocator,
                runtime.diskBudget(),
                runtime.spillDir(),
                runtime.spillEnabled(),
                runtime.maxDecodeAhead(),
                projectedSchema,
                fileSchema,
                options.batchSize(),
                decodeMasks,
                recordEvalRequired,
                lateMat,
                batchForm,
                observation);
    }

    /**
     * The RAM-or-mmap valve a decode reserves its off-heap value buffers through. The RAM path reserves against the
     * runtime's off-heap decode budget; the spill path maps an on-disk file under the runtime's disk budget while the
     * native decode budget has no room.
     */
    private DecodeBufferAllocator newDecodeBufferAllocator() {
        FetchSpillStore decodeSpillStore = new FetchSpillStore(runtime.spillDir(), runtime.diskBudget());
        return new DecodeBufferAllocator(runtime.segmentPool(), runtime.offHeapDecodeBudget(), decodeSpillStore);
    }

    /**
     * Returns one flag per survivor, in survivor order: whether the read still has to test each decoded row against the
     * predicate. A MATCHED survivor reports {@code false}; its statistics already proved every row matches, hence the
     * row pipeline skips per-row evaluation for it.
     */
    private static List<Boolean> recordEvalFlagsFor(List<RowGroupSurvivor> survivors) {
        List<Boolean> flags = new ArrayList<>(survivors.size());
        for (RowGroupSurvivor survivor : survivors) {
            flags.add(survivor.recordEvalRequired());
        }
        return flags;
    }

    /**
     * Builds the decode-side observation for the {@code read} and {@code readBatches} paths: the true file ordinal for
     * each survivor, parallel to {@link #survivorsFor}'s output (every non-eliminated group). Returns
     * {@link DecodeObservation#NONE} when no observer is attached, which keeps the decode path byte-identical.
     */
    private static DecodeObservation decodeObservationFor(
            ExplainPlan plan, boolean observe, QueryObserver observer, boolean matchedEqualsDecoded) {
        if (!observe) {
            return DecodeObservation.NONE;
        }
        List<Integer> indices = new ArrayList<>();
        for (RowGroupPlan rgPlan : plan.rowGroups()) {
            if (rgPlan.outcome() != RowGroupOutcome.ELIMINATED) {
                indices.add(rgPlan.index());
            }
        }
        return new DecodeObservation(observer, indices, matchedEqualsDecoded, observer.wantsTimings());
    }

    /**
     * Builds the decode-side observation for the count residual path: the true file ordinal for each FULL and PARTIAL
     * group, parallel to {@link #residualSurvivors}'s output (MATCHED groups are excluded, having been reported already
     * by {@link #matchedRowCount}). The count path accumulates matches per group, hence {@code matchedEqualsDecoded} is
     * {@code false}.
     */
    private static DecodeObservation residualObservationFor(ExplainPlan plan, boolean observe, QueryObserver observer) {
        if (!observe) {
            return DecodeObservation.NONE;
        }
        List<Integer> indices = new ArrayList<>();
        for (RowGroupPlan rgPlan : plan.rowGroups()) {
            switch (rgPlan.outcome()) {
                case ELIMINATED, MATCHED -> {
                    /* eliminated decodes nothing; matched is reported from metadata, never decoded */
                }
                case PARTIAL, FULL -> indices.add(rgPlan.index());
            }
        }
        return new DecodeObservation(observer, indices, false, observer.wantsTimings());
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
        drainForAnalyze(predicate, projection, drainOptions, accumulator, pipelineNanos::addAndGet);
        FetchStats fetch = accumulator.snapshot();

        collector.onQueryFinished(null);
        QueryStats stats = collector.snapshot().withTotalFetch(fetch);
        if (pipelineNanos.get() > 0L) {
            stats = withPipelineNanos(stats, pipelineNanos.get());
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
            LongConsumer pipelineNanosSink) {
        try (Stream<Boolean> rows = buildRowStream(
                predicate, projection, DISCARD_MATERIALIZER, drainOptions, accumulator, pipelineNanosSink)) {
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
     * Binds index-section reads to this reader's {@link ByteRangeSource}. Subclasses may override to plug a cached
     * source. Index, offset-index, and bloom reads do not record into a {@link FetchAccumulator}; the accumulating form
     * lives in {@link #indexSectionLoader(FetchAccumulator)}, which the read entry points use.
     */
    protected IndexSectionLoader indexSectionLoader() {
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
    protected IndexSectionLoader indexSectionLoader(FetchAccumulator accumulator) {
        return new IndexSectionLoader() {
            @Override
            public OffsetIndex readOffsetIndex(long offset, int length) {
                OffsetIndex offsetIndex = ParquetFormat.readOffsetIndex(source, offset, length);
                accumulator.add(FetchPurpose.OFFSET_INDEX, length);
                return offsetIndex;
            }

            @Override
            public ColumnIndex readColumnIndex(long offset, int length) {
                ColumnIndex columnIndex = ParquetFormat.readColumnIndex(source, offset, length);
                accumulator.add(FetchPurpose.COLUMN_INDEX, length);
                return columnIndex;
            }

            @Override
            public SplitBlockBloomFilter readBloom(long offset, int length) {
                if (length > 0) {
                    SplitBlockBloomFilter filter = BloomFilterReader.read(source, offset, length);
                    accumulator.add(FetchPurpose.BLOOM_FILTER, length);
                    return filter;
                }
                SplitBlockBloomFilter filter = BloomFilterReader.readWithoutLength(source, offset);
                accumulator.add(FetchPurpose.BLOOM_FILTER, filter.byteSize());
                return filter;
            }
        };
    }

    /**
     * Builds one {@link RowGroupChunks} per footer row group, in file order, reused across the filter and decode
     * phases. Index-section reads do not record into a {@link FetchAccumulator}; the accumulating form is
     * {@link #rowGroupChunks(FetchAccumulator)}.
     */
    protected List<RowGroupChunks> rowGroupChunks() {
        return rowGroupChunks(FetchAccumulator.NONE);
    }

    /**
     * Builds one {@link RowGroupChunks} per footer row group, in file order, with index-section reads recording into
     * {@code accumulator}.
     */
    protected List<RowGroupChunks> rowGroupChunks(FetchAccumulator accumulator) {
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

    /**
     * Builds inputs for and runs the filter pipeline. Subclasses may override to inject extra inputs (e.g. an external
     * row-group catalog) before delegation.
     */
    protected ExplainPlan runFilterPipeline(
            Predicate predicate, Projection projection, ReadOptions options, List<RowGroupChunks> rowGroupChunks) {
        List<ColumnPath> projectedLeaves = projectedLeafColumns(projection);
        List<FilterPipeline.RowGroupInputs> inputs = filterInputsFor(rowGroupChunks, options, projectedLeaves);
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
     * Builds one {@link FilterPipeline.RowGroupInputs} per row group from the pre-built chunk views. Subclasses may
     * override to supply richer dictionary or page-stats lookups than the in-footer defaults.
     *
     * @param projectedLeaves the projected leaf column paths, used to size each row group's projected compressed bytes
     */
    protected List<FilterPipeline.RowGroupInputs> filterInputsFor(
            List<RowGroupChunks> rowGroupChunks, ReadOptions options, List<ColumnPath> projectedLeaves) {
        SpatialBoundsSource spatialBounds = SpatialBoundsSource.of(footer, fileSchema, geoMetadata);
        List<FilterPipeline.RowGroupInputs> inputs = new ArrayList<>(rowGroupChunks.size());
        for (RowGroupChunks chunks : rowGroupChunks) {
            inputs.add(new FilterPipeline.RowGroupInputs(
                    chunks.numRows(),
                    statsLookup(chunks),
                    spatialBounds,
                    FilterPipeline.noDictionaryLookup(),
                    pageStatsLookupFor(chunks, options),
                    bloomLookupFor(chunks, options),
                    projectedCompressedBytes(chunks, projectedLeaves)));
        }
        return inputs;
    }

    /**
     * The projected leaf column paths for {@code projection}: every leaf when {@link Projection#ALL}, else the subset.
     */
    private List<ColumnPath> projectedLeafColumns(Projection projection) {
        return switch (projection) {
            case Projection.All _ -> fileSchema.leafColumns();
            case Projection.Columns _ -> projectionOf(projection).leafColumns();
        };
    }

    private ParquetSchema projectionOf(Projection projection) {
        return switch (projection) {
            case Projection.All _ -> fileSchema;
            case Projection.Columns(Set<ColumnPath> kept) -> fileSchema.project(kept);
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
     * {@link RowGroupChunks#stats}, which reads from the in-footer column metadata without any I/O. Subclasses may
     * override to merge external stats sources (e.g. a sidecar index) with the in-footer statistics.
     */
    protected ColumnStatsLookup statsLookup(RowGroupChunks chunks) {
        return chunks::stats;
    }

    /**
     * Returns the column-index tier lookup for {@code chunks}, or the no-op lookup when {@code useColumnIndexFilter} is
     * off. Each call to the returned lookup delegates to {@link RowGroupChunks#pageStats}, which memoizes the result so
     * each column's index sections are read at most once per call.
     */
    protected ColumnPageStatsLookup pageStatsLookupFor(RowGroupChunks chunks, ReadOptions options) {
        if (!options.useColumnIndexFilter()) {
            return noColumnPageStatsLookup();
        }
        return chunks::pageStats;
    }

    /**
     * Returns the bloom-filter lookup for {@code chunks}. Each call to the returned lookup delegates to
     * {@link RowGroupChunks#bloom}, which memoizes the result so each column's bloom filter is read at most once per
     * call. Returns {@link FilterPipeline#emptyBloomLookup()} when {@code options.useBloomFilter()} is off; the bloom
     * tier degrades gracefully without forcing the evaluator to handle nulls. Subclasses may override to plug a cached
     * or remote bloom-filter source.
     */
    protected BloomFilterLookup bloomLookupFor(RowGroupChunks chunks, ReadOptions options) {
        if (!options.useBloomFilter()) {
            return FilterPipeline.emptyBloomLookup();
        }
        return chunks::bloom;
    }

    /**
     * Translates the explain plan's row-group outcomes into materialization-ready {@link RowGroupSurvivor survivors}.
     * Subclasses may override to drop or reorder survivors before they enter the batch pipeline.
     */
    protected List<RowGroupSurvivor> survivorsFor(ExplainPlan plan, List<RowGroupChunks> rowGroupChunks) {
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
    protected List<Optional<RowMask>> decodeMasksFor(
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

    /**
     * Builds the parquetry {@link ParquetSchema} from the footer, folding GeoParquet 1.x's {@code "geo"} key-value
     * metadata into native Geometry / Geography logical-type annotations on WKB columns that lack one. Downstream code
     * (e.g. the JtsMaterializer in parquetry-geo-jts) then sees one shape regardless of file version.
     */
    private static ParquetSchema buildFileSchema(FileMetaData footer, Map<String, String> kvMetadata) {
        return SchemaBuilder.build(footer.schema(), kvMetadata);
    }

    /**
     * Parses the GeoParquet {@code "geo"} key-value entry once, returning empty when it is absent or cannot be parsed.
     * The covering-column lowering consults it to replace spatial predicate leaves.
     */
    private static Optional<GeoParquetMetadata> parseGeoMetadata(Map<String, String> kvMetadata) {
        String geoJson = kvMetadata.get(GEO_KEY);
        if (geoJson == null || geoJson.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(GeoParquetMetadata.parse(geoJson));
        } catch (RuntimeException _) {
            return Optional.empty();
        }
    }

    private static Map<String, String> collapseKeyValueMetadata(List<KeyValue> entries) {
        if (entries.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> collapsed = LinkedHashMap.newLinkedHashMap(entries.size());
        for (KeyValue entry : entries) {
            collapsed.put(entry.key(), entry.value().orElse(""));
        }
        return Collections.unmodifiableMap(collapsed);
    }

    private static List<RowGroupSummary> toRowGroupView(FileMetaData footer) {
        List<RowGroup> rgs = footer.rowGroups();
        List<RowGroupSummary> view = new ArrayList<>(rgs.size());
        for (int i = 0; i < rgs.size(); i++) {
            RowGroup rg = rgs.get(i);
            view.add(new RowGroupSummary(
                    i,
                    rg.numRows(),
                    rg.totalByteSize(),
                    rg.totalCompressedSize().orElse(-1L)));
        }
        return Collections.unmodifiableList(view);
    }
}
