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
import java.util.function.Consumer;
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
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.KeyValue;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.internal.filter.FilterPipeline;
import io.tileverse.parquetry.internal.filter.FilterPipeline.BloomFilterLookup;
import io.tileverse.parquetry.internal.filter.FilterPipeline.ColumnPageStatsLookup;
import io.tileverse.parquetry.internal.filter.FilterPipeline.ColumnStatsLookup;
import io.tileverse.parquetry.internal.filter.FilterPipeline.DictionaryLookup;
import io.tileverse.parquetry.internal.filter.bloom.BloomFilterReader;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.internal.filter.spatial.SpatialBoundsSource;
import io.tileverse.parquetry.internal.filter.spatial.SpatialCoveringRewrite;
import io.tileverse.parquetry.internal.read.BatchPipeline;
import io.tileverse.parquetry.internal.read.IndexSectionLoader;
import io.tileverse.parquetry.internal.read.LateMaterialization;
import io.tileverse.parquetry.internal.read.ParallelDecodeCoordinator;
import io.tileverse.parquetry.internal.read.RowGroupChunks;
import io.tileverse.parquetry.internal.read.RowGroupFetcher;
import io.tileverse.parquetry.internal.read.RowGroupPrefetcher;
import io.tileverse.parquetry.internal.read.RowGroupSurvivor;
import io.tileverse.parquetry.internal.read.RowMask;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaBuilder;
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

    private final ByteRangeSource source;
    private final FileMetaData footer;
    private final ParquetSchema fileSchema;
    private final Map<String, String> keyValueMetadata;
    private final List<RowGroupSummary> rowGroupView;
    private final Optional<GeoParquetMetadata> geoMetadata;

    protected ParquetReader(
            ByteRangeSource source,
            FileMetaData footer,
            ParquetSchema fileSchema,
            Map<String, String> keyValueMetadata,
            List<RowGroupSummary> rowGroupView) {
        this.source = source;
        this.footer = footer;
        this.fileSchema = fileSchema;
        this.keyValueMetadata = keyValueMetadata;
        this.rowGroupView = rowGroupView;
        this.geoMetadata = parseGeoMetadata(keyValueMetadata);
    }

    /** Opens a reader, performing exactly one footer read against {@code source}. */
    public static ParquetReader open(@NonNull ByteRangeSource source) {
        FileMetaData footer = ParquetFormat.readFooter(source);
        Map<String, String> kvMetadata = collapseKeyValueMetadata(footer.keyValueMetadata());
        ParquetSchema fileSchema = buildFileSchema(footer, kvMetadata);
        List<RowGroupSummary> rgView = toRowGroupView(footer);
        return new ParquetReader(source, footer, fileSchema, kvMetadata, rgView);
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

        Predicate predicate = lowerSpatialPredicates(rawPredicate);
        boolean recordLevel = options.useRecordLevelFilter();
        Projection scanProjection = recordLevel ? scanProjectionFor(projection, predicate) : projection;
        List<RowGroupChunks> rowGroupChunks = rowGroupChunks();
        ExplainPlan plan = runFilterPipeline(predicate, scanProjection, options, rowGroupChunks);
        reportPruningDecisions(plan, options);
        List<RowGroupSurvivor> survivors = survivorsFor(plan, rowGroupChunks);
        ParquetSchema scanSchema = plan.projectedSchema();
        ParquetSchema outputSchema = outputSchemaFor(projection);
        List<Optional<RowMask>> decodeMasks = decodeMasksFor(survivors, scanSchema, options);
        Predicate normalized = plan.normalizedPredicate();
        Optional<LateMaterialization> lateMat =
                lateMaterializationFor(survivors, scanSchema, outputSchema, normalized, options, recordLevel);
        ParallelDecodeCoordinator coordinator =
                newDecodeCoordinator(survivors, scanSchema, decodeMasks, options, lateMat);
        Predicate recordFilter = (recordLevel && lateMat.isEmpty()) ? recordFilterOf(normalized) : null;
        return BatchPipeline.rows(coordinator, materializer, outputSchema, recordFilter);
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
        Projection predicateProjection = Projection.of(Predicate.columns(predicate));
        List<RowGroupChunks> rowGroupChunks = rowGroupChunks();
        ExplainPlan plan = runFilterPipeline(predicate, predicateProjection, options, rowGroupChunks);

        long matchedRows = matchedRowCount(plan);
        List<RowGroupSurvivor> residual = residualSurvivors(plan, rowGroupChunks);
        if (residual.isEmpty()) {
            return matchedRows;
        }
        return matchedRows + countResidual(residual, plan, options);
    }

    /** Sum of row counts for the MATCHED row groups, added without any decode. */
    private static long matchedRowCount(ExplainPlan plan) {
        long matchedRows = 0L;
        for (RowGroupPlan rgPlan : plan.rowGroups()) {
            if (rgPlan.outcome() == RowGroupOutcome.MATCHED) {
                matchedRows += rgPlan.rowCount();
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

    /** Decodes the predicate columns of the residual row groups and counts the matches via a columnar popcount. */
    private long countResidual(List<RowGroupSurvivor> residual, ExplainPlan plan, ReadOptions options) {
        ParquetSchema scanSchema = plan.projectedSchema();
        Predicate normalized = plan.normalizedPredicate();
        List<Optional<RowMask>> masks = decodeMasksFor(residual, scanSchema, options);
        ParallelDecodeCoordinator coordinator =
                newDecodeCoordinator(residual, scanSchema, masks, options, Optional.empty());
        return BatchPipeline.countMatching(coordinator, normalized);
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

        Predicate predicate = lowerSpatialPredicates(rawPredicate);
        List<RowGroupChunks> rowGroupChunks = rowGroupChunks();
        ExplainPlan plan = runFilterPipeline(predicate, projection, options, rowGroupChunks);
        List<RowGroupSurvivor> survivors = survivorsFor(plan, rowGroupChunks);
        ParquetSchema projectedSchema = plan.projectedSchema();
        List<Optional<RowMask>> decodeMasks = decodeMasksFor(survivors, projectedSchema, options);
        ParallelDecodeCoordinator coordinator =
                newDecodeCoordinator(survivors, projectedSchema, decodeMasks, options, Optional.empty());
        Stream<ParquetRecordBatch> batches = BatchPipeline.batches(coordinator);
        return batches.map(batch -> materializer.materialize(projectedSchema, batch));
    }

    /**
     * Builds the coalescing fetcher and the prefetch pipeline for one read. The prefetcher owns a fresh per-read
     * virtual-thread executor; closing the returned stream cascades to {@link RowGroupPrefetcher#close()}, which shuts
     * the executor down. No executor outlives the read.
     */
    private RowGroupPrefetcher newPrefetcher(
            List<RowGroupSurvivor> survivors, ParquetSchema projectedSchema, ReadOptions options) {
        RowGroupFetcher fetcher = new RowGroupFetcher(
                source,
                fileSchema,
                projectedSchema,
                options.segmentPool(),
                options.maxCoalesceGap(),
                options.maxCoalescedSpan());
        ExecutorService executor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("parquetry-fetch-", 0).factory());
        try {
            return new RowGroupPrefetcher(
                    survivors,
                    fetcher,
                    options.fetchBudget(),
                    executor,
                    options.prefetchDepth(),
                    options.maxConcurrentFetchesPerRead());
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
            Optional<LateMaterialization> lateMat) {
        RowGroupPrefetcher prefetcher = newPrefetcher(survivors, projectedSchema, options);
        List<Boolean> recordEvalRequired = recordEvalFlagsFor(survivors);
        return new ParallelDecodeCoordinator(
                prefetcher,
                options.decodeExecutor(),
                options.decodeBudget(),
                options.maxDecodeAheadPerRead(),
                projectedSchema,
                fileSchema,
                options.batchSize(),
                decodeMasks,
                recordEvalRequired,
                lateMat);
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

    /** Runs the filter pipeline without reading data and returns the explain plan. */
    public ExplainPlan explain(
            @NonNull Predicate rawPredicate, @NonNull Projection projection, @NonNull ReadOptions options) {
        Predicate predicate = lowerSpatialPredicates(rawPredicate);
        return runFilterPipeline(predicate, projection, options, rowGroupChunks());
    }

    /**
     * Binds index-section reads to this reader's {@link ByteRangeSource}. Subclasses may override to plug a cached
     * source.
     */
    protected IndexSectionLoader indexSectionLoader() {
        return new IndexSectionLoader() {
            @Override
            public OffsetIndex readOffsetIndex(long offset, int length) {
                return ParquetFormat.readOffsetIndex(source, offset, length);
            }

            @Override
            public ColumnIndex readColumnIndex(long offset, int length) {
                return ParquetFormat.readColumnIndex(source, offset, length);
            }

            @Override
            public SplitBlockBloomFilter readBloom(long offset, int length) {
                return length > 0
                        ? BloomFilterReader.read(source, offset, length)
                        : BloomFilterReader.readWithoutLength(source, offset);
            }
        };
    }

    /**
     * Builds one {@link RowGroupChunks} per footer row group, in file order, reused across the filter and decode
     * phases.
     */
    protected List<RowGroupChunks> rowGroupChunks() {
        IndexSectionLoader loader = indexSectionLoader();
        List<RowGroup> rgs = footer.rowGroups();
        List<RowGroupChunks> chunks = new ArrayList<>(rgs.size());
        for (RowGroup rg : rgs) {
            chunks.add(RowGroupChunks.of(rg, fileSchema, loader));
        }
        return chunks;
    }

    /**
     * Builds inputs for and runs the filter pipeline. Subclasses may override to inject extra inputs (e.g. an external
     * row-group catalog) before delegation.
     */
    protected ExplainPlan runFilterPipeline(
            Predicate predicate, Projection projection, ReadOptions options, List<RowGroupChunks> rowGroupChunks) {
        List<FilterPipeline.RowGroupInputs> inputs = filterInputsFor(rowGroupChunks, options);
        return FilterPipeline.evaluate(fileSchema, projection, predicate, inputs);
    }

    /** Replays every per-row-group tier decision to the caller's listener, in row-group then tier order. */
    private static void reportPruningDecisions(ExplainPlan plan, ReadOptions options) {
        Consumer<PruningDecision> listener = options.pruningDecisionListener();
        for (RowGroupPlan rowGroupPlan : plan.rowGroups()) {
            rowGroupPlan.tiers().forEach(listener);
        }
    }

    /**
     * Builds one {@link FilterPipeline.RowGroupInputs} per row group from the pre-built chunk views. Subclasses may
     * override to supply richer dictionary or page-stats lookups than the in-footer defaults.
     */
    protected List<FilterPipeline.RowGroupInputs> filterInputsFor(
            List<RowGroupChunks> rowGroupChunks, ReadOptions options) {
        SpatialBoundsSource spatialBounds = SpatialBoundsSource.of(footer, fileSchema, geoMetadata);
        List<FilterPipeline.RowGroupInputs> inputs = new ArrayList<>(rowGroupChunks.size());
        for (RowGroupChunks chunks : rowGroupChunks) {
            inputs.add(new FilterPipeline.RowGroupInputs(
                    chunks.numRows(),
                    statsLookup(chunks),
                    spatialBounds,
                    noDictionaryLookup(),
                    pageStatsLookupFor(chunks, options),
                    bloomLookupFor(chunks, options)));
        }
        return inputs;
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

    private static DictionaryLookup noDictionaryLookup() {
        return _ -> Optional.empty();
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
