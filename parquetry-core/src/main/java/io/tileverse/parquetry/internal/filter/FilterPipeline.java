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
package io.tileverse.parquetry.internal.filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.RowGroupOutcome;
import io.tileverse.parquetry.filter.explain.RowGroupPlan;
import io.tileverse.parquetry.filter.explain.Tier;
import io.tileverse.parquetry.filter.explain.TierReasons;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.internal.filter.spatial.EmptyBoundsSource;
import io.tileverse.parquetry.internal.filter.spatial.SpatialBoundsSource;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * The a-priori filter pipeline: given a normalized predicate and per-row-group lookups for stats, dictionaries, and
 * page indexes, produces an {@link ExplainPlan} describing what each tier decided. No actual column data is read; the
 * pipeline only consumes metadata that callers have already loaded.
 */
public final class FilterPipeline {

    private FilterPipeline() {}

    /**
     * Per-row-group inputs the pipeline needs to evaluate the a-priori tiers. Any lookup may return empty entries for a
     * given column, in which case the corresponding tier degrades to {@link PruningDecision.NotApplied}.
     *
     * @param rowCount total rows in the row group; surfaces in {@link RowGroupPlan#rowCount()} and lets the stats tier
     *     check counts like null-count vs row-count
     * @param stats lookup for the STATS tier (per-column min/max + null/distinct counts)
     * @param spatialBounds source for the SPATIAL tier (per-row-group geometry bounding boxes from native GeoParquet
     *     2.0 statistics); {@link EmptyBoundsSource#INSTANCE} when the file has no usable geometry bounds
     * @param dictionaries lookup for the DICTIONARY tier; returns the loaded dictionary page when one was written
     * @param pageIndexes lookup for the COLUMN_INDEX tier (per-page min/max + offsets)
     * @param blooms lookup for the BLOOM_FILTER tier; returns the column's loaded split-block bloom filter when one was
     *     written
     * @param projectedCompressedBytes total compressed byte size of this row group's projected leaf column chunks; the
     *     pipeline sums it across non-eliminated row groups into {@link ExplainPlan#estimatedBytesRead()}. A group
     *     whose outcome is {@link RowGroupOutcome#ELIMINATED} contributes nothing.
     */
    public record RowGroupInputs(
            long rowCount,
            ColumnStatsLookup stats,
            SpatialBoundsSource spatialBounds,
            DictionaryLookup dictionaries,
            ColumnPageStatsLookup pageIndexes,
            BloomFilterLookup blooms,
            long projectedCompressedBytes,
            long rowPositionBase,
            List<Long> rowPositionPageFirstRows) {

        public RowGroupInputs {
            rowPositionPageFirstRows = List.copyOf(rowPositionPageFirstRows);
        }

        /**
         * Backwards-compatible four-arg constructor for callers that don't yet wire spatial bounds or a bloom-filter
         * lookup; defaults the spatial source to {@link EmptyBoundsSource#INSTANCE} (the SPATIAL tier degrades to
         * {@link PruningDecision.NotApplied}), the blooms to {@link FilterPipeline#emptyBloomLookup()}, and the
         * projected compressed bytes to zero (callers that want a byte estimate use the full constructor). The
         * row-position pruning inputs default to inert (zero base offset, no page boundaries).
         */
        public RowGroupInputs(
                long rowCount,
                ColumnStatsLookup stats,
                DictionaryLookup dictionaries,
                ColumnPageStatsLookup pageIndexes) {
            this(
                    rowCount,
                    stats,
                    EmptyBoundsSource.INSTANCE,
                    dictionaries,
                    pageIndexes,
                    emptyBloomLookup(),
                    0L,
                    0L,
                    List.of());
        }
    }

    /**
     * Per-column input to the statistics-tier filter evaluator: the column's primitive kind (so the evaluator knows how
     * to decode the raw min/max bytes) plus its {@link Statistics} record from {@code ColumnMetaData}.
     */
    public record ColumnStats(PrimitiveKind kind, Statistics statistics) {}

    /**
     * Per-column input to the COLUMN_INDEX-tier evaluator: the column's primitive kind plus its loaded
     * {@link ColumnIndex} (page min/max + null markers) and {@link OffsetIndex} (page row offsets).
     */
    public record ColumnPageStats(PrimitiveKind kind, ColumnIndex columnIndex, OffsetIndex offsetIndex) {}

    /**
     * Per-column input to the BLOOM_FILTER-tier evaluator: the column's primitive kind (drives the Parquet
     * plain-encoding hash) plus its loaded {@link SplitBlockBloomFilter}.
     */
    public record ColumnBloom(PrimitiveKind kind, SplitBlockBloomFilter bloom) {}

    /**
     * Resolves {@link ColumnStats} for a given column path within a row group. The filter pipeline implements this on
     * top of a {@code RowGroup}'s column-chunk list and the file schema; tests can supply a tiny map-based lookup.
     */
    @FunctionalInterface
    public interface ColumnStatsLookup {

        /** Returns the {@link ColumnStats} for {@code path}, or empty if the column has no statistics. */
        Optional<ColumnStats> get(ColumnPath path);
    }

    /**
     * Resolves the loaded {@link Dictionary} for a given column path within a row group. Returns empty when the column
     * isn't dictionary-encoded or the dictionary page hasn't been loaded.
     */
    @FunctionalInterface
    public interface DictionaryLookup {

        @SuppressWarnings("java:S1452") // Dictionary's element type is selected at runtime from PrimitiveKind
        Optional<Dictionary<?>> get(ColumnPath path);
    }

    /**
     * Resolves per-column {@link ColumnPageStats} (ColumnIndex + OffsetIndex) for a given column path within a row
     * group. Returns empty when the column has no index sections.
     */
    @FunctionalInterface
    public interface ColumnPageStatsLookup {

        Optional<ColumnPageStats> get(ColumnPath path);
    }

    /**
     * Resolves the loaded {@link ColumnBloom} for a given column path within a row group. Returns empty when the column
     * has no bloom filter or the bytes haven't been fetched. The lookup is expected to fetch lazily; the evaluator only
     * asks for columns the predicate touches.
     */
    @FunctionalInterface
    public interface BloomFilterLookup {

        Optional<ColumnBloom> get(ColumnPath path);
    }

    /** A {@link BloomFilterLookup} that never resolves a column - used as the default when no blooms are wired. */
    public static BloomFilterLookup emptyBloomLookup() {
        return path -> Optional.empty();
    }

    /**
     * The shared no-op dictionary lookup the production read path wires while real dictionary pruning is not yet
     * implemented. The pipeline recognizes this exact instance to report the DICTIONARY tier as inert-by-design rather
     * than as a column-level miss. Callers that genuinely wire dictionaries supply their own lookup.
     */
    private static final DictionaryLookup NO_DICTIONARY_LOOKUP = path -> Optional.empty();

    /**
     * Returns the shared no-op dictionary lookup the production read path wires until real dictionary pruning lands.
     */
    public static DictionaryLookup noDictionaryLookup() {
        return NO_DICTIONARY_LOOKUP;
    }

    /**
     * The metadata-tier toggles the pipeline honors directly: whether the STATS tier runs and whether the DICTIONARY
     * tier runs. The COLUMN_INDEX and BLOOM_FILTER toggles are honored upstream at input-build time (those tiers see an
     * empty lookup and degrade on their own), which is why they are not part of this record.
     *
     * @param useStatsFilter when false, the STATS tier is skipped and reported as {@link PruningDecision.NotApplied}
     * @param useDictionaryFilter when false, the DICTIONARY tier is skipped and reported as
     *     {@link PruningDecision.NotApplied}
     */
    public record FilterToggles(boolean useStatsFilter, boolean useDictionaryFilter) {

        /** Both tiers enabled - the default that preserves the historical always-run behavior. */
        public static final FilterToggles ALL_ENABLED = new FilterToggles(true, true);
    }

    /**
     * Runs the pipeline with all metadata tiers enabled. Equivalent to {@link #evaluate(ParquetSchema, Projection,
     * Predicate, List, FilterToggles)} with {@link FilterToggles#ALL_ENABLED}.
     */
    public static ExplainPlan evaluate(
            ParquetSchema fileSchema,
            Projection projection,
            Predicate originalPredicate,
            List<RowGroupInputs> rowGroups) {
        return evaluate(fileSchema, projection, originalPredicate, rowGroups, FilterToggles.ALL_ENABLED);
    }

    /**
     * Runs the pipeline.
     *
     * @param fileSchema the file's full schema (used for predicate validation and the explain plan)
     * @param projection projected columns (used only to record the projected schema in the plan)
     * @param originalPredicate the predicate as the caller wrote it; will be normalized before evaluation
     * @param rowGroups one {@link RowGroupInputs} per row group in the file, in file order
     * @param toggles which metadata tiers to run; a disabled tier is reported as {@link PruningDecision.NotApplied}
     *     instead of being skipped silently
     */
    public static ExplainPlan evaluate(
            ParquetSchema fileSchema,
            Projection projection,
            Predicate originalPredicate,
            List<RowGroupInputs> rowGroups,
            FilterToggles toggles) {
        Predicate normalized = PredicateNormalizer.normalizeAndValidate(originalPredicate, fileSchema);
        ParquetSchema projectedSchema = projectionOf(fileSchema, projection);
        List<RowGroupPlan> plans = new ArrayList<>(rowGroups.size());
        long rowsScanned = 0;
        long bytesRead = 0;
        for (int i = 0; i < rowGroups.size(); i++) {
            RowGroupInputs inputs = rowGroups.get(i);
            RowGroupPlan plan = evaluateRowGroup(i, inputs, normalized, toggles);
            plans.add(plan);
            rowsScanned += survivingRowsForOutcome(plan);
            bytesRead += projectedBytesForOutcome(plan, inputs);
        }
        return new ExplainPlan(
                fileSchema,
                projectedSchema,
                originalPredicate,
                normalized,
                plans,
                rowsScanned,
                bytesRead,
                Optional.empty());
    }

    /**
     * The projected compressed bytes a row group contributes to {@link ExplainPlan#estimatedBytesRead()}: its full
     * projected-chunk size when the group survives, zero when it is eliminated. This stays at whole-chunk granularity;
     * page-level refinement is not applied here even when the COLUMN_INDEX tier narrowed the surviving rows to specific
     * pages, because the estimate is meant as an upper bound on the bytes a read would fetch.
     */
    private static long projectedBytesForOutcome(RowGroupPlan plan, RowGroupInputs inputs) {
        if (plan.outcome() == RowGroupOutcome.ELIMINATED) {
            return 0L;
        }
        return inputs.projectedCompressedBytes();
    }

    private static RowGroupPlan evaluateRowGroup(
            int index, RowGroupInputs inputs, Predicate normalized, FilterToggles toggles) {
        if (normalized instanceof Predicate.RowIndexExcluded) {
            return evaluateRowPositionDeletes(index, inputs, normalized);
        }
        List<PruningDecision> tierDecisions = new ArrayList<>(5);
        Optional<RowRanges> surviving = Optional.empty();

        PruningDecision statsDecision = statsDecision(normalized, inputs, toggles);
        tierDecisions.add(statsDecision);
        if (statsDecision instanceof PruningDecision.Eliminated) {
            return new RowGroupPlan(
                    index, inputs.rowCount(), tierDecisions, RowGroupOutcome.ELIMINATED, surviving, Optional.empty());
        }
        // An empty row group has no rows to match. Letting it fall through keeps the existing behavior, where a later
        // tier eliminates it, instead of claiming MATCHED and fetching a chunk that should never be read.
        if (statsDecision instanceof PruningDecision.PassedAll && inputs.rowCount() > 0) {
            return new RowGroupPlan(
                    index, inputs.rowCount(), tierDecisions, RowGroupOutcome.MATCHED, surviving, Optional.empty());
        }

        PruningDecision spatialDecision = SpatialBoundsEvaluator.evaluate(normalized, inputs.spatialBounds(), index);
        tierDecisions.add(spatialDecision);
        if (spatialDecision instanceof PruningDecision.Eliminated) {
            return new RowGroupPlan(
                    index, inputs.rowCount(), tierDecisions, RowGroupOutcome.ELIMINATED, surviving, Optional.empty());
        }

        PruningDecision dictDecision = dictionaryDecision(normalized, inputs, toggles);
        tierDecisions.add(dictDecision);
        if (dictDecision instanceof PruningDecision.Eliminated) {
            return new RowGroupPlan(
                    index, inputs.rowCount(), tierDecisions, RowGroupOutcome.ELIMINATED, surviving, Optional.empty());
        }

        PruningDecision indexDecision =
                ColumnIndexEvaluator.evaluate(normalized, inputs.pageIndexes(), inputs.rowCount());
        tierDecisions.add(indexDecision);
        if (indexDecision instanceof PruningDecision.Eliminated) {
            return new RowGroupPlan(
                    index, inputs.rowCount(), tierDecisions, RowGroupOutcome.ELIMINATED, surviving, Optional.empty());
        }
        if (indexDecision instanceof PruningDecision.NarrowedTo n) {
            surviving = Optional.of(n.ranges());
        }

        PruningDecision bloomDecision = BloomFilterEvaluator.evaluate(normalized, inputs.blooms());
        tierDecisions.add(bloomDecision);
        if (bloomDecision instanceof PruningDecision.Eliminated) {
            return new RowGroupPlan(
                    index, inputs.rowCount(), tierDecisions, RowGroupOutcome.ELIMINATED, surviving, Optional.empty());
        }

        RowGroupOutcome outcome = surviving.isPresent() ? RowGroupOutcome.PARTIAL : RowGroupOutcome.FULL;
        return new RowGroupPlan(index, inputs.rowCount(), tierDecisions, outcome, surviving, Optional.empty());
    }

    /**
     * Evaluates a bare {@link Predicate.RowIndexExcluded} from its deleted-position range alone, with no column data
     * read. The row-group tier eliminates a fully-deleted group or passes an untouched one as MATCHED (no per-row check
     * needed); a partly-deleted group descends to the page tier, which drops fully-deleted pages and leaves the rest
     * for record-level evaluation.
     */
    private static RowGroupPlan evaluateRowPositionDeletes(int index, RowGroupInputs inputs, Predicate normalized) {
        List<PruningDecision> tierDecisions = new ArrayList<>(2);
        long base = inputs.rowPositionBase();
        long rowCount = inputs.rowCount();

        PruningDecision rowGroupDecision = RowPositionEvaluator.evaluateRowGroup(normalized, base, rowCount);
        tierDecisions.add(rowGroupDecision);
        if (rowGroupDecision instanceof PruningDecision.Eliminated) {
            return new RowGroupPlan(
                    index, rowCount, tierDecisions, RowGroupOutcome.ELIMINATED, Optional.empty(), Optional.empty());
        }
        if (rowGroupDecision instanceof PruningDecision.PassedAll && rowCount > 0) {
            return new RowGroupPlan(
                    index, rowCount, tierDecisions, RowGroupOutcome.MATCHED, Optional.empty(), Optional.empty());
        }

        PruningDecision pageDecision =
                RowPositionEvaluator.evaluatePages(normalized, base, rowCount, inputs.rowPositionPageFirstRows());
        tierDecisions.add(pageDecision);
        if (pageDecision instanceof PruningDecision.Eliminated) {
            return new RowGroupPlan(
                    index, rowCount, tierDecisions, RowGroupOutcome.ELIMINATED, Optional.empty(), Optional.empty());
        }
        if (pageDecision instanceof PruningDecision.NarrowedTo narrowed) {
            return new RowGroupPlan(
                    index,
                    rowCount,
                    tierDecisions,
                    RowGroupOutcome.PARTIAL,
                    Optional.of(narrowed.ranges()),
                    Optional.empty());
        }
        return new RowGroupPlan(
                index, rowCount, tierDecisions, RowGroupOutcome.FULL, Optional.empty(), Optional.empty());
    }

    /**
     * The STATS-tier decision, honoring {@code useStatsFilter}. When stats filtering is off the tier is reported as
     * {@link PruningDecision.NotApplied} and the row group continues to later tiers as if stats proved nothing.
     */
    private static PruningDecision statsDecision(Predicate normalized, RowGroupInputs inputs, FilterToggles toggles) {
        if (!toggles.useStatsFilter()) {
            return new PruningDecision.NotApplied(Tier.STATS, TierReasons.STATS_FILTER_DISABLED);
        }
        return StatsEvaluator.evaluate(normalized, inputs.stats(), inputs.rowCount());
    }

    /**
     * The DICTIONARY-tier decision. When dictionary filtering is off the tier reports that it is disabled. When it is
     * on but no dictionary lookup is wired into the read path, the tier reports that honestly rather than running an
     * inert evaluation whose generic "no dictionary loaded" reason would read as if a lookup had been attempted.
     */
    private static PruningDecision dictionaryDecision(
            Predicate normalized, RowGroupInputs inputs, FilterToggles toggles) {
        if (!toggles.useDictionaryFilter()) {
            return new PruningDecision.NotApplied(Tier.DICTIONARY, TierReasons.DICTIONARY_FILTER_DISABLED);
        }
        if (inputs.dictionaries() == NO_DICTIONARY_LOOKUP) {
            return new PruningDecision.NotApplied(Tier.DICTIONARY, "dictionary pruning not wired into reads");
        }
        return DictionaryEvaluator.evaluate(normalized, inputs.dictionaries());
    }

    private static long survivingRowsForOutcome(RowGroupPlan plan) {
        return switch (plan.outcome()) {
            case ELIMINATED -> 0L;
            case PARTIAL -> plan.survivingRows().map(RowRanges::totalRows).orElse(plan.rowCount());
            case FULL, MATCHED -> plan.rowCount();
        };
    }

    private static ParquetSchema projectionOf(ParquetSchema fileSchema, Projection projection) {
        return switch (projection) {
            case Projection.All _ -> fileSchema;
            case Projection.Of of -> fileSchema.project(of.physicalColumns());
        };
    }

    /** Defensive copy of an empty list as the no-row-groups input. */
    @SuppressWarnings("unused")
    public static List<RowGroupInputs> empty() {
        return Collections.emptyList();
    }
}
