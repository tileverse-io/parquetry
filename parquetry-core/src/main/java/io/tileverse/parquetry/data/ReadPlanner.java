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
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.RowGroupPlan;
import io.tileverse.parquetry.filter.explain.Tier;
import io.tileverse.parquetry.internal.filter.FilterPipeline;
import io.tileverse.parquetry.internal.filter.FilterPipeline.BloomFilterLookup;
import io.tileverse.parquetry.internal.filter.FilterPipeline.PageIndexLookup;
import io.tileverse.parquetry.internal.filter.PageIndex;
import io.tileverse.parquetry.internal.filter.StatsEvaluator;
import io.tileverse.parquetry.internal.footer.ChunkMeta;
import io.tileverse.parquetry.internal.read.BatchForm;
import io.tileverse.parquetry.internal.read.IndexSectionLoader;
import io.tileverse.parquetry.internal.read.ParallelDecodeCoordinator;
import io.tileverse.parquetry.internal.read.ParallelDecodeCoordinator.DecodeObservation;
import io.tileverse.parquetry.internal.read.RowGroupChunks;
import io.tileverse.parquetry.internal.read.RowGroupGate;
import io.tileverse.parquetry.internal.read.RowGroupSurvivor;
import io.tileverse.parquetry.internal.read.RowMask;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.observe.FetchPurpose;
import io.tileverse.parquetry.observe.QueryObserver;
import io.tileverse.parquetry.observe.QueryStarted;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Plans the queries of one file. For a query, it builds the chunk views of the row groups, runs the filter pipeline
 * over them, and returns the {@link ReadPlan} used by the decode. A reader holds one planner, and each query gets a
 * plan of its own. The unfiltered bounds read some row groups without a plan, and the planner assembles that decode
 * too.
 */
final class ReadPlanner {

    private static final long[] NO_OFFSETS = new long[0];

    private final ByteRangeSource source;
    private final FooterMetadata footer;
    private final ReadResources readResources;
    private final SpatialReadGates spatialReadGates;

    ReadPlanner(
            ByteRangeSource source,
            FooterMetadata footer,
            ReadResources readResources,
            SpatialReadGates spatialReadGates) {
        this.source = source;
        this.footer = footer;
        this.readResources = readResources;
        this.spatialReadGates = spatialReadGates;
    }

    /**
     * Plans one query. The chunk views record their index-section reads into the fetch accumulator of
     * {@code observation}, and the filter pipeline runs with its options. The time of the pipeline is reported to
     * {@code observation} only when its observers asked for timings. When the predicate has a positional delete, the
     * filter pipeline and the plan share one computation of the file row offsets.
     */
    ReadPlan plan(Predicate predicate, Projection scanProjection, QueryObservation observation) {
        List<RowGroupChunks> chunks = rowGroupChunks(observation.fetchAccumulator());
        Optional<long[]> fileOffsets = fileOffsetsForRowPositions(predicate, chunks);
        ExplainPlan explainPlan = evaluateTimed(predicate, scanProjection, chunks, fileOffsets, observation);
        return new ReadPlan(
                footer.fileSchema(), readResources, spatialReadGates, chunks, explainPlan, fileOffsets, observation);
    }

    /**
     * Runs the filter pipeline for an explain of {@code predicate} over {@code scanProjection} and returns its plan. It
     * reads no clock and tallies no fetch. The observer of {@code options} receives the planned events and never the
     * finish of the query.
     */
    ExplainPlan explain(Predicate predicate, Projection scanProjection, ReadOptions options) {
        List<RowGroupChunks> chunks = chunks();
        Optional<long[]> fileOffsets = fileOffsetsForRowPositions(predicate, chunks);
        return evaluate(predicate, scanProjection, options, chunks, fileOffsets);
    }

    /**
     * Builds one {@link RowGroupChunks} per footer row group, in file order. No {@link FetchAccumulator} tallies their
     * index-section reads. Only the views built by {@link #plan} record into the fetch accumulator of their query.
     */
    List<RowGroupChunks> chunks() {
        return rowGroupChunks(FetchAccumulator.NONE);
    }

    /**
     * The coordinator of a decode of {@code survivors}, read in full without a filter pipeline. The unfiltered bounds
     * use it for their row groups without a box. No event fires and no fetch is recorded.
     *
     * <p>The unfiltered bounds skip the filter pipeline on purpose, as {@link ParquetFileReader#count} does for an
     * {@link Predicate.Always} literal. Planning {@link Predicate#ALWAYS_TRUE} would fire the planned events to an
     * attached observer, and neither query fires an event.
     */
    ParallelDecodeCoordinator decodeUnplanned(
            List<RowGroupSurvivor> survivors, ParquetSchema scanSchema, ReadOptions options) {
        // A survivor read in full has no row set to mask on.
        List<Optional<RowMask>> masks = Collections.nCopies(survivors.size(), Optional.empty());
        Optional<RowGroupGate> rowGroupGate = spatialReadGates.rowGroupGate(survivors, options);
        return readResources.newDecodeCoordinator(
                survivors,
                scanSchema,
                masks,
                options,
                Optional.empty(),
                BatchForm.LEVELS,
                FetchAccumulator.NONE,
                DecodeObservation.NONE,
                List.of(),
                rowGroupGate,
                Optional.empty());
    }

    /** The leaf columns referenced by {@code predicate}, minus the synthesized row-position columns. */
    static Set<ColumnPath> physicalColumns(Predicate predicate) {
        Set<ColumnPath> columns = new LinkedHashSet<>(Predicate.columns(predicate));
        Predicate.rowPositionColumns(predicate).forEach(columns::remove);
        return columns;
    }

    /**
     * Returns the absolute file row offset of each row group, in an array indexed by row group. The offset of a row
     * group is the running sum of {@code num_rows} over the row groups before it in file order. Pruned row groups count
     * too, because a pruned row group still takes up position space. The row-position synthesis and the row-position
     * tier of the filter pipeline share this one definition, and their two offset walks cannot drift apart.
     */
    static long[] fileOffsets(List<RowGroupChunks> rowGroupChunks) {
        long[] offsets = new long[rowGroupChunks.size()];
        long runningOffset = 0L;
        for (int i = 0; i < rowGroupChunks.size(); i++) {
            offsets[i] = runningOffset;
            runningOffset += rowGroupChunks.get(i).numRows();
        }
        return offsets;
    }

    /**
     * Builds one {@link RowGroupChunks} per footer row group, in file order, with index-section reads recording into
     * {@code accumulator}.
     */
    private List<RowGroupChunks> rowGroupChunks(FetchAccumulator accumulator) {
        IndexSectionLoader loader = indexSectionLoader(accumulator);
        int rowGroupCount = footer.compactFooter().rowGroupCount();
        List<RowGroupChunks> chunks = new ArrayList<>(rowGroupCount);
        for (int rowGroupIndex = 0; rowGroupIndex < rowGroupCount; rowGroupIndex++) {
            chunks.add(RowGroupChunks.of(
                    footer.compactFooter(), rowGroupIndex, footer.leafIndex(), footer.fileSchema(), loader));
        }
        return chunks;
    }

    /**
     * Binds index-section reads to the {@link ByteRangeSource} of the file and records the bytes of each section into
     * {@code accumulator}. A column-index read is recorded as {@link FetchPurpose#COLUMN_INDEX}, an offset-index read
     * as {@link FetchPurpose#OFFSET_INDEX}, and a bloom-filter read as {@link FetchPurpose#BLOOM_FILTER}. The recorded
     * byte count is the on-disk length of the section. A bloom filter with a length recorded by the writer counts the
     * full chunk (header plus bitset). One without a recorded length counts the bitset byte size discovered from the
     * filter header, the only figure cheaply available on that path. Passing {@link FetchAccumulator#NONE} reduces
     * every record call to a no-op.
     */
    private IndexSectionLoader indexSectionLoader(FetchAccumulator accumulator) {
        return new FooterIndexSectionLoader(source, accumulator, footer.columnChunkBytes());
    }

    /**
     * The file row offsets of the row groups when {@code predicate} names a row position (a positional delete), and
     * empty otherwise. The row-position tier of the filter pipeline reads them only then, and the plan reuses them for
     * its synthesized positions. Their computation stays off the common path.
     */
    private static Optional<long[]> fileOffsetsForRowPositions(Predicate predicate, List<RowGroupChunks> chunks) {
        if (Predicate.rowPositionColumns(predicate).isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(fileOffsets(chunks));
    }

    /**
     * Runs {@link #evaluate} with the options of {@code observation} and, when the observers asked for timings, reports
     * the elapsed nanoseconds to it. When timings are off, no clock is read.
     */
    private ExplainPlan evaluateTimed(
            Predicate predicate,
            Projection projection,
            List<RowGroupChunks> rowGroupChunks,
            Optional<long[]> fileOffsets,
            QueryObservation observation) {
        ReadOptions options = observation.options();
        if (!observation.wantsTimings()) {
            return evaluate(predicate, projection, options, rowGroupChunks, fileOffsets);
        }
        long start = System.nanoTime();
        ExplainPlan plan = evaluate(predicate, projection, options, rowGroupChunks, fileOffsets);
        observation.addPipelineNanos(System.nanoTime() - start);
        return plan;
    }

    /** Builds the inputs of the filter pipeline and runs it. */
    private ExplainPlan evaluate(
            Predicate predicate,
            Projection projection,
            ReadOptions options,
            List<RowGroupChunks> rowGroupChunks,
            Optional<long[]> fileOffsets) {
        List<ColumnPath> projectedLeaves = projectedLeafColumns(projection);
        List<FilterPipeline.RowGroupInputs> inputs =
                filterInputs(rowGroupChunks, options, projectedLeaves, fileOffsets);
        Set<Tier> disabledTiers = disabledTiers(options);
        ExplainPlan plan = FilterPipeline.evaluate(footer.fileSchema(), projection, predicate, inputs, disabledTiers);
        notifyPlanned(plan, options.queryObserver());
        return plan;
    }

    /**
     * Replays the planning outcome to {@code observer}. The observer receives one {@code onQueryStarted} for the query,
     * then one {@code onRowGroupPlanned} per tier decision of the row groups. With {@link QueryObserver#NONE}, the
     * method returns before any allocation, and the read path stays free of observability overhead.
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
     * @param fileOffsets the file row offsets of the row groups, present only when the predicate names a row position.
     *     The page boundaries of each row group, the other row-position pruning input, are read only then and stay off
     *     the common path.
     */
    private List<FilterPipeline.RowGroupInputs> filterInputs(
            List<RowGroupChunks> rowGroupChunks,
            ReadOptions options,
            List<ColumnPath> projectedLeaves,
            Optional<long[]> fileOffsets) {
        boolean rowPositionTier = fileOffsets.isPresent();
        long[] offsets = fileOffsets.orElse(NO_OFFSETS);
        List<FilterPipeline.RowGroupInputs> inputs = new ArrayList<>(rowGroupChunks.size());
        for (int i = 0; i < rowGroupChunks.size(); i++) {
            RowGroupChunks chunks = rowGroupChunks.get(i);
            inputs.add(new FilterPipeline.RowGroupInputs(
                    chunks.numRows(),
                    statsLookup(chunks),
                    footer.spatialBounds(),
                    pageIndexLookup(chunks),
                    bloomLookup(chunks),
                    projectedCompressedBytes(chunks, projectedLeaves),
                    rowPositionTier ? offsets[i] : 0L,
                    rowPositionTier ? rowPositionPages(chunks, options) : Optional.empty()));
        }
        return inputs;
    }

    /**
     * The pages of any one leaf column, used by the row-position delete tier to find fully-deleted pages. The
     * synthesized {@code $pos} column has no page index of its own. The columns of a row group share the same row
     * boundaries, hence the pages of any real leaf partition the row group into spans usable by the position math. The
     * result is empty when the column-index filter is off or no leaf has a page index. The page tier then stays inert.
     */
    private Optional<PageIndex> rowPositionPages(RowGroupChunks chunks, ReadOptions options) {
        // The page boundaries cost an offset-index read, skipped when the page tier is off.
        if (!options.useColumnIndexFilter()) {
            return Optional.empty();
        }
        for (ColumnPath leaf : footer.fileSchema().leafColumns()) {
            Optional<PageIndex> pages = chunks.pageLocations(leaf);
            if (pages.isPresent()) {
                return pages;
            }
        }
        return Optional.empty();
    }

    /**
     * Returns the leaf column paths projected by {@code projection}. {@link Projection#ALL} projects all the leaves of
     * the file.
     */
    private List<ColumnPath> projectedLeafColumns(Projection projection) {
        return switch (projection) {
            case Projection.All _ -> footer.fileSchema().leafColumns();
            case Projection.Of _ -> projectionOf(projection).leafColumns();
        };
    }

    private ParquetSchema projectionOf(Projection projection) {
        return switch (projection) {
            case Projection.All _ -> footer.fileSchema();
            case Projection.Of of -> footer.fileSchema().project(of.physicalColumns());
        };
    }

    /**
     * Sums the total compressed size of {@code chunks}' projected leaf column chunks. A projected leaf absent from the
     * row group is skipped. The result feeds {@link ExplainPlan#estimatedBytesRead()} as the bytes fetched by a read of
     * this row group when it survives pruning.
     */
    private static long projectedCompressedBytes(RowGroupChunks chunks, List<ColumnPath> projectedLeaves) {
        long total = 0L;
        for (ColumnPath leaf : projectedLeaves) {
            total += chunks.chunk(leaf).map(ReadPlanner::estimableBytes).orElse(0);
        }
        return total;
    }

    /**
     * The bytes read by a fetch of {@code chunk}, counted toward the estimate alone. A negative recorded length counts
     * as nothing, the placeholder of an unaddressable size included. An estimate short of the truth beats one dragged
     * below zero.
     */
    private static int estimableBytes(ChunkMeta chunk) {
        return Math.max(0, chunk.totalCompressedSize());
    }

    /**
     * Returns the inline statistics lookup for {@code chunks}. Each call to the returned lookup delegates to
     * {@link RowGroupChunks#summary}, served from the column metadata inside the footer without any I/O.
     */
    private StatsEvaluator.TypedColumns statsLookup(RowGroupChunks chunks) {
        return chunks::summary;
    }

    /**
     * Returns the column-index tier lookup for {@code chunks}. Each call to the returned lookup delegates to
     * {@link RowGroupChunks#pageIndex}. The view memoizes the result, and the index sections of a column are read at
     * most once. The tier warms each consultable column before the first lookup, and the row group reads their index
     * sections in one call.
     */
    private PageIndexLookup pageIndexLookup(RowGroupChunks chunks) {
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
     * {@link RowGroupChunks#bloom}. The view memoizes the result, and the bloom filter of a column is read at most
     * once.
     */
    private BloomFilterLookup bloomLookup(RowGroupChunks chunks) {
        return chunks::bloom;
    }

    /** The tiers turned off by {@code options}. The pipeline reports them as not applied on each row group. */
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
}
