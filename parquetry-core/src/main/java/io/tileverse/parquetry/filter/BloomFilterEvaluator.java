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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.parquetry.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Tier-4 (BLOOM_FILTER) evaluator. Inspects a row group's loaded bloom filters: when an equality (or IN-list) predicate
 * value is "definitely absent" from the bloom filter, the row group is eliminated.
 *
 * <p>Bloom filters answer only set-membership questions. The evaluator therefore only acts on {@code Eq} and {@code In}
 * leaves, and only when the column's value hash can be computed from the predicate-side {@link Value}. Everything else
 * (range comparisons, IS NULL, BBox, NOT) is {@link PruningDecision.NotApplied}.
 *
 * <p>Like {@link DictionaryEvaluator}, the evaluator assumes the predicate has been normalized (Not pushed to leaves,
 * Always folded, nested And/Or flattened).
 */
final class BloomFilterEvaluator {

    private static final Tier TIER = Tier.BLOOM_FILTER;

    private BloomFilterEvaluator() {}

    /** Evaluates {@code predicate} against the row group's lazily-loaded bloom filters. */
    static PruningDecision evaluate(Predicate predicate, FilterPipeline.BloomFilterLookup blooms) {
        return switch (predicate) {
            case Predicate.Always(boolean value) ->
                value
                        ? new PruningDecision.NotApplied(TIER, "predicate is ALWAYS_TRUE")
                        : new PruningDecision.Eliminated(TIER, "predicate is ALWAYS_FALSE");
            case Predicate.And(List<Predicate> children) -> evalAnd(children, blooms);
            case Predicate.Or(List<Predicate> children) -> evalOr(children, blooms);
            case Predicate.Not _ -> new PruningDecision.NotApplied(TIER, "NOT not handled at BLOOM_FILTER tier");
            case Predicate.Eq(ColumnPath col, Value v) -> evalEq(col, v, blooms);
            case Predicate.In(ColumnPath col, List<Value> values) -> evalIn(col, values, blooms);
            case Predicate.NotEq _ -> new PruningDecision.NotApplied(TIER, "NotEq not handled at BLOOM_FILTER tier");
            case Predicate.Lt _ -> new PruningDecision.NotApplied(TIER, "Lt not handled at BLOOM_FILTER tier");
            case Predicate.LtEq _ -> new PruningDecision.NotApplied(TIER, "LtEq not handled at BLOOM_FILTER tier");
            case Predicate.Gt _ -> new PruningDecision.NotApplied(TIER, "Gt not handled at BLOOM_FILTER tier");
            case Predicate.GtEq _ -> new PruningDecision.NotApplied(TIER, "GtEq not handled at BLOOM_FILTER tier");
            case Predicate.IsNull _ -> new PruningDecision.NotApplied(TIER, "IsNull not handled at BLOOM_FILTER tier");
            case Predicate.IsNotNull _ ->
                new PruningDecision.NotApplied(TIER, "IsNotNull not handled at BLOOM_FILTER tier");
            case Predicate.Spatial _ ->
                new PruningDecision.NotApplied(TIER, "spatial predicate not handled at BLOOM_FILTER tier");
            case Predicate.GeometryFilterPredicate _ ->
                new PruningDecision.NotApplied(TIER, "GeometryFilter not handled at BLOOM_FILTER tier");
        };
    }

    /**
     * Eliminates the row group when every child predicate in an AND can be independently eliminated by the bloom tier;
     * otherwise NotApplied. Standard pruning short-circuit: one eliminated child kills the conjunction.
     */
    private static PruningDecision evalAnd(List<Predicate> children, FilterPipeline.BloomFilterLookup blooms) {
        for (Predicate child : children) {
            if (evaluate(child, blooms) instanceof PruningDecision.Eliminated e) {
                return new PruningDecision.Eliminated(TIER, "AND child eliminated: " + e.reason());
            }
        }
        return new PruningDecision.NotApplied(TIER, "no AND child eliminates");
    }

    /**
     * Eliminates the row group only when every OR branch is independently eliminated by the bloom tier; otherwise
     * NotApplied. As with dictionaries, one inconclusive branch keeps the whole disjunction inconclusive.
     */
    private static PruningDecision evalOr(List<Predicate> children, FilterPipeline.BloomFilterLookup blooms) {
        for (Predicate child : children) {
            if (!(evaluate(child, blooms) instanceof PruningDecision.Eliminated)) {
                return new PruningDecision.NotApplied(TIER, "an OR child not eliminated");
            }
        }
        return new PruningDecision.Eliminated(TIER, "all OR children eliminated");
    }

    /** Single equality probe: if the bloom proves the value absent, the row group can't satisfy {@code col == v}. */
    private static PruningDecision evalEq(ColumnPath col, Value v, FilterPipeline.BloomFilterLookup blooms) {
        Optional<FilterPipeline.ColumnBloom> bloom = blooms.get(col);
        if (bloom.isEmpty()) {
            return new PruningDecision.NotApplied(TIER, "Eq " + col.dot() + ": no bloom filter loaded");
        }
        FilterPipeline.ColumnBloom cb = bloom.get();
        OptionalLong hash = hashFor(v, cb.kind());
        if (hash.isEmpty()) {
            return new PruningDecision.NotApplied(TIER, "Eq " + col.dot() + ": value/kind not hashable for bloom");
        }
        if (cb.bloom().mightContain(hash.getAsLong())) {
            return new PruningDecision.NotApplied(TIER, "Eq " + col.dot() + ": value may be present");
        }
        return new PruningDecision.Eliminated(TIER, "Eq " + col.dot() + ": value definitely absent");
    }

    /**
     * IN-list pruning: the row group is eliminated only when every list value is provably absent from the column's
     * bloom. If even one value is "maybe present", the predicate can still be satisfied and we can't conclude anything.
     */
    private static PruningDecision evalIn(ColumnPath col, List<Value> values, FilterPipeline.BloomFilterLookup blooms) {
        Optional<FilterPipeline.ColumnBloom> bloom = blooms.get(col);
        if (bloom.isEmpty()) {
            return new PruningDecision.NotApplied(TIER, "In " + col.dot() + ": no bloom filter loaded");
        }
        FilterPipeline.ColumnBloom cb = bloom.get();
        for (Value v : values) {
            OptionalLong hash = hashFor(v, cb.kind());
            if (hash.isEmpty()) {
                return new PruningDecision.NotApplied(TIER, "In " + col.dot() + ": value/kind not hashable for bloom");
            }
            if (cb.bloom().mightContain(hash.getAsLong())) {
                return new PruningDecision.NotApplied(TIER, "In " + col.dot() + ": at least one value may be present");
            }
        }
        return new PruningDecision.Eliminated(TIER, "In " + col.dot() + ": every value definitely absent");
    }

    /**
     * Maps a predicate-side {@link Value} to its bloom-filter hash, honoring Parquet's plain-encoding rule. Returns
     * empty when the column kind / value combination has no defined bloom hash (e.g. an INT96 column or a date/time
     * predicate on a non-matching column kind); the caller surfaces those as NotApplied.
     */
    @SuppressWarnings("java:S7475") // see DictionaryEvaluator: palantirJavaFormat 2.90 cannot parse bare _ in patterns
    private static OptionalLong hashFor(Value v, PrimitiveKind kind) {
        return switch (v) {
            case Value.BoolVal _ -> OptionalLong.empty();
            case Value.IntVal(int qv)
            when kind == PrimitiveKind.INT32 -> OptionalLong.of(SplitBlockBloomFilter.hashInt32(qv));
            case Value.LongVal(long qv)
            when kind == PrimitiveKind.INT64 -> OptionalLong.of(SplitBlockBloomFilter.hashInt64(qv));
            case Value.FloatVal(float qv)
            when kind == PrimitiveKind.FLOAT -> OptionalLong.of(SplitBlockBloomFilter.hashFloat(qv));
            case Value.DoubleVal(double qv)
            when kind == PrimitiveKind.DOUBLE -> OptionalLong.of(SplitBlockBloomFilter.hashDouble(qv));
            case Value.StringVal(String qv)
            when kind == PrimitiveKind.BYTE_ARRAY ->
                OptionalLong.of(SplitBlockBloomFilter.hashBytes(qv.getBytes(StandardCharsets.UTF_8)));
            case Value.BinaryVal(MemorySegment qv)
            when kind == PrimitiveKind.BYTE_ARRAY || kind == PrimitiveKind.FIXED_LEN_BYTE_ARRAY ->
                OptionalLong.of(hashSegment(qv));
            case Value.DateVal(java.time.LocalDate qv)
            when kind == PrimitiveKind.INT32 -> OptionalLong.of(SplitBlockBloomFilter.hashInt32((int) qv.toEpochDay()));
            case Value.TimestampVal(java.time.LocalDateTime qv, boolean _)
            when kind == PrimitiveKind.INT64 ->
                OptionalLong.of(SplitBlockBloomFilter.hashInt64(qv.toEpochSecond(java.time.ZoneOffset.UTC) * 1000L));
            default -> OptionalLong.empty();
        };
    }

    /** Hashes a segment's bytes by extracting them into a primitive array (no position disturbance to worry about). */
    private static long hashSegment(MemorySegment segment) {
        byte[] bytes = segment.toArray(JAVA_BYTE);
        return SplitBlockBloomFilter.hashBytes(bytes);
    }
}
