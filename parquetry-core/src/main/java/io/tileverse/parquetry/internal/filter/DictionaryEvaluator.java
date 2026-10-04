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

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.Tier;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Tier-2 (DICTIONARY) evaluator. Inspects a row group's loaded dictionary pages: if no dictionary value can satisfy the
 * predicate then the row group is eliminated. The dictionary has no null counts; the evaluator never upgrades to
 * PassedAll. It returns {@link PruningDecision.Eliminated}, {@link PruningDecision.Inconclusive} (at least one
 * dictionary value matches), or {@link PruningDecision.NotApplied} (no dictionary loaded or an ineligible predicate).
 *
 * <p>A dictionary value matches a comparison exactly as a record cell does at record level, through
 * {@link ValueComparison#holds}. The evaluator assumes the predicate has already been normalized (Not pushed to leaves,
 * Always folded, nested And/Or flattened).
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
                leafOrSkip(col, dictionaries, v, matching(ComparisonOperator.EQ, v), "Eq");
            case io.tileverse.parquetry.filter.Predicate.NotEq(ColumnPath col, Value v) ->
                leafOrSkip(col, dictionaries, v, matching(ComparisonOperator.NOT_EQ, v), "NotEq");
            case io.tileverse.parquetry.filter.Predicate.Lt(ColumnPath col, Value v) ->
                leafOrSkip(col, dictionaries, v, matching(ComparisonOperator.LT, v), "Lt");
            case io.tileverse.parquetry.filter.Predicate.LtEq(ColumnPath col, Value v) ->
                leafOrSkip(col, dictionaries, v, matching(ComparisonOperator.LT_EQ, v), "LtEq");
            case io.tileverse.parquetry.filter.Predicate.Gt(ColumnPath col, Value v) ->
                leafOrSkip(col, dictionaries, v, matching(ComparisonOperator.GT, v), "Gt");
            case io.tileverse.parquetry.filter.Predicate.GtEq(ColumnPath col, Value v) ->
                leafOrSkip(col, dictionaries, v, matching(ComparisonOperator.GT_EQ, v), "GtEq");
            case io.tileverse.parquetry.filter.Predicate.In(ColumnPath col, List<Value> values) ->
                values.stream().allMatch(DictionaryEvaluator::dictComparable)
                        ? evalLeaf(col, dictionaries, matchingAny(values), "In")
                        : new PruningDecision.NotApplied(TIER, "In " + col.dot() + ": type not dictionary-comparable");
            case io.tileverse.parquetry.filter.Predicate.IsNull _ ->
                new PruningDecision.NotApplied(TIER, "IsNull: dictionaries don't track nulls");
            case io.tileverse.parquetry.filter.Predicate.IsNotNull _ ->
                new PruningDecision.NotApplied(TIER, "IsNotNull: dictionaries don't track nulls");
            case io.tileverse.parquetry.filter.Predicate.Spatial _ ->
                new PruningDecision.NotApplied(TIER, "spatial predicate not handled at DICTIONARY tier");
            case io.tileverse.parquetry.filter.Predicate.GeometryFilterPredicate _ ->
                new PruningDecision.NotApplied(TIER, "GeometryFilter not handled at DICTIONARY tier");
            case io.tileverse.parquetry.filter.Predicate.RowIndexExcluded _ ->
                new PruningDecision.NotApplied(TIER, "row-position deletes not handled at DICTIONARY tier");
            case io.tileverse.parquetry.filter.Predicate.Quantified(
                    MatchAction match,
                    io.tileverse.parquetry.filter.Predicate leaf) -> evaluateQuantified(match, leaf, dictionaries);
        };
    }

    /**
     * An existential {@code ANY} over a repeated leaf may only eliminate the whole row group: if no dictionary value
     * matches the inner comparison then no row matches. It must never report a non-elimination as conclusive, because a
     * single-distinct-value dictionary could otherwise yield {@code PassedAll}; a row with an empty or all-null list
     * fails {@code ANY} and still needs record-level confirmation. {@code ALL} and {@code ONE} are not decidable here.
     */
    private static PruningDecision evaluateQuantified(
            MatchAction match,
            io.tileverse.parquetry.filter.Predicate leaf,
            FilterPipeline.DictionaryLookup dictionaries) {
        if (match != MatchAction.ANY) {
            return new PruningDecision.NotApplied(TIER, match + " over a repeated leaf is not pruned by dictionaries");
        }
        PruningDecision inner = evaluate(leaf, dictionaries);
        if (inner instanceof PruningDecision.Eliminated) {
            return inner;
        }
        return new PruningDecision.NotApplied(
                TIER,
                "ANY over a repeated leaf: row-group elimination only; per-row lists need record-level confirmation");
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
     * Returns true when the value type can be compared against a dictionary entry without knowing the column's logical
     * type. Timestamp, time, and decimal values encode their java.time / BigDecimal form as a unit-dependent INT64 or
     * FIXED_LEN_BYTE_ARRAY; the column unit and scale are not available here - a naive comparison would produce a wrong
     * result and could falsely eliminate the row group.
     */
    private static boolean dictComparable(Value v) {
        return !(v instanceof Value.TimestampVal || v instanceof Value.TimeVal || v instanceof Value.DecimalVal);
    }

    /**
     * Routes a leaf predicate through {@link #evalLeaf} when the value type is dictionary-comparable, or returns
     * NotApplied when it is not. This prevents a unit-unaware conversion from producing a wrong comparison result that
     * could falsely eliminate the row group.
     */
    private static PruningDecision leafOrSkip(
            ColumnPath col, FilterPipeline.DictionaryLookup dicts, Value v, Predicate<Object> matches, String op) {
        if (!dictComparable(v)) {
            return new PruningDecision.NotApplied(TIER, op + " " + col.dot() + ": type not dictionary-comparable");
        }
        return evalLeaf(col, dicts, matches, op);
    }

    /** The dictionary values for which {@code op} holds against {@code literal}, as it would for record cells. */
    private static Predicate<Object> matching(ComparisonOperator op, Value literal) {
        return dictValue -> ValueComparison.holds(op, dictValue, literal);
    }

    /** The dictionary values equal to one of {@code literals}, as record cells would be. */
    private static Predicate<Object> matchingAny(List<Value> literals) {
        return dictValue ->
                literals.stream().anyMatch(literal -> ValueComparison.holds(ComparisonOperator.EQ, dictValue, literal));
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
                return new PruningDecision.Inconclusive(
                        TIER, opLabel + " " + col.dot() + ": at least one dictionary value matches");
            }
        }
        return new PruningDecision.Eliminated(TIER, opLabel + " " + col.dot() + ": no dictionary value matches");
    }
}
