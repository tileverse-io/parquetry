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
import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.RowGroupPlan;
import io.tileverse.parquetry.internal.filter.PredicateNormalizer;
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
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * The plan of one query over one file. It holds the chunk views of the row groups and the outcome of the filter
 * pipeline over them. It routes the row groups to the survivors decoded by the query, and derives the record filter,
 * the masked scan and the synthesized row positions used by their decode. It assembles that decode from the
 * {@link DecodeShape} given by the entry point of the query. A plan serves one query on one thread.
 */
final class ReadPlan {

    private final ParquetSchema fileSchema;
    private final ReadResources readResources;
    private final SpatialReadGates spatialReadGates;
    private final List<RowGroupChunks> chunks;
    private final ExplainPlan explainPlan;
    private final QueryObservation observation;

    // The file row offsets of the row groups. When the predicate has a positional delete, the planner computes them for
    // the filter pipeline and hands the same array over. Otherwise they are computed on first use. A plan serves one
    // query on one thread, hence the memo needs no synchronization.
    private long[] fileOffsets;

    ReadPlan(
            ParquetSchema fileSchema,
            ReadResources readResources,
            SpatialReadGates spatialReadGates,
            List<RowGroupChunks> chunks,
            ExplainPlan explainPlan,
            Optional<long[]> fileOffsets,
            QueryObservation observation) {
        this.fileSchema = fileSchema;
        this.readResources = readResources;
        this.spatialReadGates = spatialReadGates;
        this.chunks = List.copyOf(chunks);
        this.explainPlan = explainPlan;
        this.fileOffsets = fileOffsets.orElse(null);
        this.observation = observation;
    }

    /** The outcome of the filter pipeline, one row-group plan per row group in file order. */
    ExplainPlan explainPlan() {
        return explainPlan;
    }

    /** The chunk views of the row groups, in file order, pruned ones included. */
    List<RowGroupChunks> chunks() {
        return chunks;
    }

    /** The schema of the columns decoded by the query. */
    ParquetSchema scanSchema() {
        return explainPlan.projectedSchema();
    }

    /** The predicate of the query, as normalized by the filter pipeline. */
    Predicate normalized() {
        return explainPlan.normalizedPredicate();
    }

    /** Returns the per-row filter, or {@code null} when the predicate is trivially true (nothing to evaluate). */
    Predicate recordFilter() {
        Predicate normalized = normalized();
        if (normalized instanceof Predicate.Always(boolean value) && value) {
            return null;
        }
        return normalized;
    }

    /**
     * Translates the explain plan's row-group outcomes into materialization-ready {@link RowGroupSurvivor survivors}.
     */
    List<RowGroupSurvivor> survivors() {
        List<RowGroupPlan> rowGroups = explainPlan.rowGroups();
        List<RowGroupSurvivor> survivors = new ArrayList<>(rowGroups.size());
        for (RowGroupPlan rowGroup : rowGroups) {
            switch (rowGroup.outcome()) {
                case ELIMINATED -> {
                    /* drop */
                }
                case PARTIAL, FULL, MATCHED -> survivors.add(survivorFor(rowGroup));
            }
        }
        return survivors;
    }

    /**
     * The FULL and PARTIAL row groups, the only row groups still decoded by a count. ELIMINATED groups add nothing and
     * MATCHED groups are added from metadata, hence both are excluded here.
     */
    List<RowGroupSurvivor> residual() {
        List<RowGroupSurvivor> residual = new ArrayList<>();
        for (RowGroupPlan rowGroup : explainPlan.rowGroups()) {
            switch (rowGroup.outcome()) {
                case ELIMINATED, MATCHED -> {
                    /* eliminated adds nothing; matched is added from metadata, never decoded */
                }
                case PARTIAL, FULL -> residual.add(survivorFor(rowGroup));
            }
        }
        return residual;
    }

    /**
     * The materialization-ready survivor of {@code rowGroup}. A PARTIAL row group is narrowed to its surviving rows, a
     * FULL one is read whole, and a MATCHED one is read whole without testing its rows against the predicate.
     *
     * @throws IllegalArgumentException when the filter pipeline eliminated {@code rowGroup}
     */
    RowGroupSurvivor survivorFor(RowGroupPlan rowGroup) {
        RowGroupChunks rowGroupChunks = chunks.get(rowGroup.index());
        return switch (rowGroup.outcome()) {
            case PARTIAL -> new RowGroupSurvivor(rowGroupChunks, rowGroup.survivingRows(), true);
            case FULL -> RowGroupSurvivor.full(rowGroupChunks);
            case MATCHED -> RowGroupSurvivor.matched(rowGroupChunks);
            case ELIMINATED ->
                throw new IllegalArgumentException(
                        "Row group " + rowGroup.index() + " was eliminated by the filter pipeline");
        };
    }

    /**
     * Packages the per-read inputs of the masked scan decode path, or empty when the read has no per-row predicate to
     * apply. The scan needs a normalized predicate other than a trivially true one, and at least one column to evaluate
     * it over. A predicate naming only synthesized columns borrows the first leaf of the scan to drive the walk.
     */
    Optional<MaskedScan> maskedScan(ParquetSchema outputSchema) {
        if (recordFilter() == null) {
            return Optional.empty();
        }
        Predicate normalized = normalized();
        if (Predicate.columns(normalized).isEmpty()) {
            return Optional.empty();
        }
        Set<ColumnPath> filterLeaves = filterLeavesFor(normalized, scanSchema());
        return Optional.of(new MaskedScan(normalized, filterLeaves, outputSchema));
    }

    /**
     * The physical leaves read by the predicate. A predicate over synthesized columns alone reads none, and the first
     * leaf of the scan stands in to enumerate the rows behind the synthesized positions.
     */
    private static Set<ColumnPath> filterLeavesFor(Predicate normalized, ParquetSchema scanSchema) {
        Set<ColumnPath> columns = ReadPlanner.physicalColumns(normalized);
        if (columns.isEmpty()) {
            return Set.of(scanSchema.leafColumns().get(0));
        }
        return columns;
    }

    /**
     * The coordinator of a decode of {@code survivors}, shaped by {@code shape}. No other method assembles a
     * coordinator for a planned query.
     */
    ParallelDecodeCoordinator decode(List<RowGroupSurvivor> survivors, DecodeShape shape) {
        ReadOptions options = observation.options();
        List<Optional<RowMask>> masks = decodeMasks(survivors);
        List<RowPositionSynthesis> rowPositions = rowPositionSyntheses(survivors, shape.rowPositionColumns());
        Optional<RowGroupGate> rowGroupGate = spatialReadGates.rowGroupGate(survivors, options);
        Optional<RowGroupPlanner> planner =
                shape.spatialPlanning().flatMap(planning -> spatialPlanner(survivors, planning, options));
        DecodeObservation decodeObservation = observation.decodeObservation(shape.matchedEqualsDecoded());
        return readResources.newDecodeCoordinator(
                survivors,
                scanSchema(),
                masks,
                options,
                shape.maskedScan(),
                shape.batchForm(),
                observation.fetchAccumulator(),
                decodeObservation,
                rowPositions,
                rowGroupGate,
                planner);
    }

    /**
     * Builds one decode mask per survivor, parallel to {@code survivors}. A mask is present only when page-skip is both
     * wanted and safe for the row group. Page-skip is wanted when {@code useColumnIndexFilter} is on. It is safe when
     * the column-index tier narrowed the row set and the scanned columns are flat, each with a page index. Otherwise
     * the entry is empty and the row group decodes in full.
     */
    private List<Optional<RowMask>> decodeMasks(List<RowGroupSurvivor> survivors) {
        ParquetSchema scanSchema = scanSchema();
        ReadOptions options = observation.options();
        List<Optional<RowMask>> masks = new ArrayList<>(survivors.size());
        boolean scanFlat = options.useColumnIndexFilter() && RowMasks.allFlat(fileSchema, scanSchema.leafColumns());
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

    /**
     * Returns the row-position synthesis inputs, parallel to {@code survivors}. Each input pairs the file row offset of
     * a survivor with the caller-named columns. The list is empty when {@code columns} is empty, and the decode path
     * then stays untouched. A survivor finds its offset by its row-group index, not by its position in
     * {@code survivors}. The eliminated row groups are missing from the survivors, yet their rows still count toward
     * the offsets of the row groups after them.
     */
    List<RowPositionSynthesis> rowPositionSyntheses(List<RowGroupSurvivor> survivors, List<RowPositionColumn> columns) {
        if (columns.isEmpty()) {
            return List.of();
        }
        long[] offsets = fileOffsets();
        List<RowPositionSynthesis> syntheses = new ArrayList<>(survivors.size());
        for (RowGroupSurvivor survivor : survivors) {
            syntheses.add(new RowPositionSynthesis(offsets[survivor.index()], columns));
        }
        return syntheses;
    }

    private long[] fileOffsets() {
        if (fileOffsets == null) {
            fileOffsets = ReadPlanner.fileOffsets(chunks);
        }
        return fileOffsets;
    }

    /**
     * The per-row-group spatial planner of a read that produces rows, present only when the read supplies a probe and
     * the output schema of {@code planning} holds the file's primary geometry column. It plans against the normalized
     * raw predicate, before the covering rewrite: the comparisons added by that rewrite are necessary conditions of the
     * spatial leaf that they were derived from, hence a unit accepted on the raw leaf satisfies them too.
     */
    private Optional<RowGroupPlanner> spatialPlanner(
            List<RowGroupSurvivor> survivors, DecodeShape.SpatialPlanning planning, ReadOptions options) {
        Predicate normalizedRaw = PredicateNormalizer.normalize(planning.rawPredicate());
        return spatialReadGates.rowGroupPlanner(survivors, normalizedRaw, options, planning.outputSchema());
    }
}
