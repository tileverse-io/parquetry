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
import java.util.function.Predicate;

import io.tileverse.parquetry.data.read.page.Dictionary;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Tier-2 (DICTIONARY) evaluator. Inspects a row group's loaded dictionary pages: if no dictionary value can satisfy the
 * predicate then the row group is eliminated. The dictionary doesn't carry null counts, so the evaluator never upgrades
 * to PassedAll - it only ever returns Eliminated or NotApplied.
 *
 * <p>The evaluator assumes the predicate has already been normalized (Not pushed to leaves, Always folded, nested
 * And/Or flattened).
 */
final class DictionaryEvaluator {

    private static final Tier TIER = Tier.DICTIONARY;

    private DictionaryEvaluator() {}

    /** Evaluates {@code predicate} against the row group's loaded dictionaries. */
    public static PruningDecision evaluate(
            io.tileverse.parquetry.filter.Predicate predicate, FilterPipeline.DictionaryLookup dictionaries) {
        return switch (predicate) {
            case io.tileverse.parquetry.filter.Predicate.Always(boolean value) ->
                value
                        ? new PruningDecision.NotApplied(TIER, "predicate is ALWAYS_TRUE")
                        : new PruningDecision.Eliminated(TIER, "predicate is ALWAYS_FALSE");
            case io.tileverse.parquetry.filter.Predicate.And(List<io.tileverse.parquetry.filter.Predicate> children) ->
                evalAnd(children, dictionaries);
            case io.tileverse.parquetry.filter.Predicate.Or(List<io.tileverse.parquetry.filter.Predicate> children) ->
                evalOr(children, dictionaries);
            case io.tileverse.parquetry.filter.Predicate.Not(io.tileverse.parquetry.filter.Predicate child) ->
                evalNotLeaf(child, dictionaries);
            case io.tileverse.parquetry.filter.Predicate.Eq(ColumnPath col, Value v) ->
                evalLeaf(col, dictionaries, dv -> equals(v, dv), "Eq");
            case io.tileverse.parquetry.filter.Predicate.NotEq(ColumnPath col, Value v) ->
                evalLeaf(col, dictionaries, dv -> !equals(v, dv), "NotEq");
            case io.tileverse.parquetry.filter.Predicate.Lt(ColumnPath col, Value v) ->
                evalLeaf(col, dictionaries, dv -> compare(dv, v) < 0, "Lt");
            case io.tileverse.parquetry.filter.Predicate.LtEq(ColumnPath col, Value v) ->
                evalLeaf(col, dictionaries, dv -> compare(dv, v) <= 0, "LtEq");
            case io.tileverse.parquetry.filter.Predicate.Gt(ColumnPath col, Value v) ->
                evalLeaf(col, dictionaries, dv -> compare(dv, v) > 0, "Gt");
            case io.tileverse.parquetry.filter.Predicate.GtEq(ColumnPath col, Value v) ->
                evalLeaf(col, dictionaries, dv -> compare(dv, v) >= 0, "GtEq");
            case io.tileverse.parquetry.filter.Predicate.In(ColumnPath col, List<Value> values) ->
                evalLeaf(col, dictionaries, dv -> values.stream().anyMatch(v -> equals(v, dv)), "In");
            case io.tileverse.parquetry.filter.Predicate.IsNull _ ->
                new PruningDecision.NotApplied(TIER, "IsNull: dictionaries don't track nulls");
            case io.tileverse.parquetry.filter.Predicate.IsNotNull _ ->
                new PruningDecision.NotApplied(TIER, "IsNotNull: dictionaries don't track nulls");
            case io.tileverse.parquetry.filter.Predicate.Spatial _ ->
                new PruningDecision.NotApplied(TIER, "spatial predicate not handled at DICTIONARY tier");
        };
    }

    private static PruningDecision evalAnd(
            List<io.tileverse.parquetry.filter.Predicate> children, FilterPipeline.DictionaryLookup dicts) {
        for (io.tileverse.parquetry.filter.Predicate child : children) {
            if (evaluate(child, dicts) instanceof PruningDecision.Eliminated e) {
                return new PruningDecision.Eliminated(TIER, "AND child eliminated: " + e.reason());
            }
        }
        return new PruningDecision.NotApplied(TIER, "no AND child eliminates");
    }

    private static PruningDecision evalOr(
            List<io.tileverse.parquetry.filter.Predicate> children, FilterPipeline.DictionaryLookup dicts) {
        for (io.tileverse.parquetry.filter.Predicate child : children) {
            PruningDecision d = evaluate(child, dicts);
            if (!(d instanceof PruningDecision.Eliminated)) {
                return new PruningDecision.NotApplied(TIER, "an OR child not eliminated");
            }
        }
        return new PruningDecision.Eliminated(TIER, "all OR children eliminated");
    }

    private static PruningDecision evalNotLeaf(
            io.tileverse.parquetry.filter.Predicate child, FilterPipeline.DictionaryLookup dicts) {
        PruningDecision inner = evaluate(child, dicts);
        return inner instanceof PruningDecision.Eliminated
                ? new PruningDecision.NotApplied(TIER, "NOT of eliminated child can't be made PassedAll from dict")
                : new PruningDecision.NotApplied(TIER, "NOT of inconclusive child");
    }

    /**
     * Evaluates a leaf predicate against the column's dictionary by walking every dictionary entry. If no entry
     * satisfies {@code matches}, the row group is eliminated.
     */
    private static PruningDecision evalLeaf(
            ColumnPath col, FilterPipeline.DictionaryLookup dicts, Predicate<Object> matches, String opLabel) {
        Optional<Dictionary<?>> dictOpt = dicts.get(col);
        if (dictOpt.isEmpty()) {
            return new PruningDecision.NotApplied(TIER, opLabel + " " + col.dot() + ": no dictionary loaded");
        }
        Dictionary<?> dict = dictOpt.get();
        for (int i = 0; i < dict.size(); i++) {
            Object dv = dict.get(i);
            if (matches.test(dv)) {
                return new PruningDecision.NotApplied(
                        TIER, opLabel + " " + col.dot() + ": at least one dictionary value matches");
            }
        }
        return new PruningDecision.Eliminated(TIER, opLabel + " " + col.dot() + ": no dictionary value matches");
    }

    // S7475 (bare _ in nested record patterns) is informational only - palantirJavaFormat 2.90 cannot
    // parse the bare-underscore form Sonar suggests; see memory feedback-palantir-unnamed-pattern.
    @SuppressWarnings("java:S7475")
    private static boolean equals(Value v, Object dictValue) {
        return switch (v) {
            case Value.BoolVal(boolean qv) when dictValue instanceof Boolean dv -> qv == dv;
            case Value.IntVal(int qv) when dictValue instanceof Integer dv -> qv == dv;
            case Value.LongVal(long qv) when dictValue instanceof Long dv -> qv == dv;
            case Value.FloatVal(float qv) when dictValue instanceof Float dv -> Float.compare(qv, dv) == 0;
            case Value.DoubleVal(double qv) when dictValue instanceof Double dv -> Double.compare(qv, dv) == 0;
            case Value.StringVal(String qv)
            when dictValue instanceof MemorySegment dv ->
                compareBytes(MemorySegment.ofArray(qv.getBytes(StandardCharsets.UTF_8)), dv) == 0;
            case Value.BinaryVal(MemorySegment qv)
            when dictValue instanceof MemorySegment dv -> compareBytes(qv, dv) == 0;
            case Value.DateVal(java.time.LocalDate qv)
            when dictValue instanceof Integer dv -> (int) qv.toEpochDay() == dv;
            case Value.TimestampVal(java.time.LocalDateTime qv, boolean _)
            when dictValue instanceof Long dv -> qv.toEpochSecond(java.time.ZoneOffset.UTC) * 1000L == dv;
            default -> false;
        };
    }

    /** Returns negative/zero/positive comparing {@code dictValue} to predicate-side {@code v}. */
    @SuppressWarnings("java:S7475") // see equals(): palantirJavaFormat 2.90 cannot parse bare _ in nested patterns
    private static int compare(Object dictValue, Value v) {
        return switch (v) {
            case Value.BoolVal(boolean qv) when dictValue instanceof Boolean dv -> Boolean.compare(dv, qv);
            case Value.IntVal(int qv) when dictValue instanceof Integer dv -> Integer.compare(dv, qv);
            case Value.LongVal(long qv) when dictValue instanceof Long dv -> Long.compare(dv, qv);
            case Value.FloatVal(float qv) when dictValue instanceof Float dv -> Float.compare(dv, qv);
            case Value.DoubleVal(double qv) when dictValue instanceof Double dv -> Double.compare(dv, qv);
            case Value.StringVal(String qv)
            when dictValue instanceof MemorySegment dv ->
                compareBytes(dv, MemorySegment.ofArray(qv.getBytes(StandardCharsets.UTF_8)));
            case Value.BinaryVal(MemorySegment qv) when dictValue instanceof MemorySegment dv -> compareBytes(dv, qv);
            case Value.DateVal(java.time.LocalDate qv)
            when dictValue instanceof Integer dv -> Integer.compare(dv, (int) qv.toEpochDay());
            case Value.TimestampVal(java.time.LocalDateTime qv, boolean _)
            when dictValue instanceof Long dv -> Long.compare(dv, qv.toEpochSecond(java.time.ZoneOffset.UTC) * 1000L);
            default -> 0;
        };
    }

    /** Lexicographic unsigned byte comparison, matching Parquet's default binary ColumnOrder. */
    private static int compareBytes(MemorySegment a, MemorySegment b) {
        long aLen = a.byteSize();
        long bLen = b.byteSize();
        long common = Math.min(aLen, bLen);
        for (long i = 0; i < common; i++) {
            int diff = (a.get(JAVA_BYTE, i) & 0xff) - (b.get(JAVA_BYTE, i) & 0xff);
            if (diff != 0) {
                return diff;
            }
        }
        return Long.compare(aLen, bLen);
    }
}
