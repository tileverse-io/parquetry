/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.filter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.data.read.page.Dictionary;
import io.tileverse.parquetry.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.Statistics;
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
     * @param dictionaries lookup for the DICTIONARY tier; returns the loaded dictionary page when one was written
     * @param pageIndexes lookup for the COLUMN_INDEX tier (per-page min/max + offsets)
     * @param blooms lookup for the BLOOM_FILTER tier; returns the column's loaded split-block bloom filter when one was
     *     written
     */
    public record RowGroupInputs(
            long rowCount,
            ColumnStatsLookup stats,
            DictionaryLookup dictionaries,
            ColumnPageStatsLookup pageIndexes,
            BloomFilterLookup blooms) {

        /**
         * Backwards-compatible four-arg constructor for callers that don't yet wire a bloom-filter lookup; defaults to
         * {@link FilterPipeline#emptyBloomLookup()} so the bloom tier degrades to {@link PruningDecision.NotApplied}.
         */
        public RowGroupInputs(
                long rowCount,
                ColumnStatsLookup stats,
                DictionaryLookup dictionaries,
                ColumnPageStatsLookup pageIndexes) {
            this(rowCount, stats, dictionaries, pageIndexes, emptyBloomLookup());
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
     * Runs the pipeline.
     *
     * @param fileSchema the file's full schema (used for predicate validation and the explain plan)
     * @param projection projected columns (used only to record the projected schema in the plan)
     * @param originalPredicate the predicate as the caller wrote it; will be normalized before evaluation
     * @param rowGroups one {@link RowGroupInputs} per row group in the file, in file order
     */
    public static ExplainPlan evaluate(
            ParquetSchema fileSchema,
            Projection projection,
            Predicate originalPredicate,
            List<RowGroupInputs> rowGroups) {
        Predicate normalized = PredicateNormalizer.normalizeAndValidate(originalPredicate, fileSchema);
        ParquetSchema projectedSchema = projectionOf(fileSchema, projection);
        List<RowGroupPlan> plans = new ArrayList<>(rowGroups.size());
        long rowsScanned = 0;
        for (int i = 0; i < rowGroups.size(); i++) {
            RowGroupPlan plan = evaluateRowGroup(i, rowGroups.get(i), normalized);
            plans.add(plan);
            rowsScanned += survivingRowsForOutcome(plan);
        }
        return new ExplainPlan(fileSchema, projectedSchema, originalPredicate, normalized, plans, rowsScanned, 0L);
    }

    private static RowGroupPlan evaluateRowGroup(int index, RowGroupInputs inputs, Predicate normalized) {
        List<PruningDecision> tierDecisions = new ArrayList<>(4);
        Optional<RowRanges> surviving = Optional.empty();

        PruningDecision statsDecision = StatsEvaluator.evaluate(normalized, inputs.stats(), inputs.rowCount());
        tierDecisions.add(statsDecision);
        if (statsDecision instanceof PruningDecision.Eliminated) {
            return new RowGroupPlan(index, inputs.rowCount(), tierDecisions, RowGroupOutcome.ELIMINATED, surviving);
        }

        PruningDecision dictDecision = DictionaryEvaluator.evaluate(normalized, inputs.dictionaries());
        tierDecisions.add(dictDecision);
        if (dictDecision instanceof PruningDecision.Eliminated) {
            return new RowGroupPlan(index, inputs.rowCount(), tierDecisions, RowGroupOutcome.ELIMINATED, surviving);
        }

        PruningDecision indexDecision =
                ColumnIndexEvaluator.evaluate(normalized, inputs.pageIndexes(), inputs.rowCount());
        tierDecisions.add(indexDecision);
        if (indexDecision instanceof PruningDecision.Eliminated) {
            return new RowGroupPlan(index, inputs.rowCount(), tierDecisions, RowGroupOutcome.ELIMINATED, surviving);
        }
        if (indexDecision instanceof PruningDecision.NarrowedTo n) {
            surviving = Optional.of(n.ranges());
        }

        PruningDecision bloomDecision = BloomFilterEvaluator.evaluate(normalized, inputs.blooms());
        tierDecisions.add(bloomDecision);
        if (bloomDecision instanceof PruningDecision.Eliminated) {
            return new RowGroupPlan(index, inputs.rowCount(), tierDecisions, RowGroupOutcome.ELIMINATED, surviving);
        }

        RowGroupOutcome outcome = surviving.isPresent() ? RowGroupOutcome.PARTIAL : RowGroupOutcome.FULL;
        return new RowGroupPlan(index, inputs.rowCount(), tierDecisions, outcome, surviving);
    }

    private static long survivingRowsForOutcome(RowGroupPlan plan) {
        return switch (plan.outcome()) {
            case ELIMINATED -> 0L;
            case PARTIAL -> plan.survivingRows().map(RowRanges::totalRows).orElse(plan.rowCount());
            case FULL -> plan.rowCount();
        };
    }

    private static ParquetSchema projectionOf(ParquetSchema fileSchema, Projection projection) {
        return switch (projection) {
            case Projection.All _ -> fileSchema;
            case Projection.Columns(var kept) -> fileSchema.project(kept);
        };
    }

    /** Defensive copy of an empty list as the no-row-groups input. */
    @SuppressWarnings("unused")
    public static List<RowGroupInputs> empty() {
        return Collections.emptyList();
    }
}
