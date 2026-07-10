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

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.IntBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.schema.ColumnPath;

class DictionaryEvaluatorTest {

    @Test
    void eqValueNotInIntDictIsEliminated() {
        FilterPipeline.DictionaryLookup dicts = single("status", new Dictionary.IntDict(intBuf(1, 2, 3)));
        Predicate p = col("status").eq(99);
        assertThat(DictionaryEvaluator.evaluate(p, dicts)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void eqValueInIntDictIsInconclusive() {
        FilterPipeline.DictionaryLookup dicts = single("status", new Dictionary.IntDict(intBuf(1, 2, 3)));
        Predicate p = col("status").eq(2);
        assertThat(DictionaryEvaluator.evaluate(p, dicts)).isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void notEqAgainstSingleValueDictMatchingValueIsEliminated() {
        FilterPipeline.DictionaryLookup dicts = single("flag", new Dictionary.IntDict(intBuf(1)));
        Predicate p = col("flag").notEq(1);
        assertThat(DictionaryEvaluator.evaluate(p, dicts)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void ltAllDictValuesAboveOperandIsEliminated() {
        FilterPipeline.DictionaryLookup dicts = single("year", new Dictionary.IntDict(intBuf(2020, 2021, 2022)));
        Predicate p = col("year").lt(2020);
        assertThat(DictionaryEvaluator.evaluate(p, dicts)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void gtAllDictValuesBelowOperandIsEliminated() {
        FilterPipeline.DictionaryLookup dicts = single("year", new Dictionary.IntDict(intBuf(2010, 2011, 2012)));
        Predicate p = col("year").gt(2020);
        assertThat(DictionaryEvaluator.evaluate(p, dicts)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void inNoMatchingDictValueIsEliminated() {
        FilterPipeline.DictionaryLookup dicts = single("status", new Dictionary.IntDict(intBuf(1, 2, 3)));
        Predicate p = col("status").inInts(7, 8, 9);
        assertThat(DictionaryEvaluator.evaluate(p, dicts)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void inOneMatchingDictValueIsInconclusive() {
        FilterPipeline.DictionaryLookup dicts = single("status", new Dictionary.IntDict(intBuf(1, 2, 3)));
        Predicate p = col("status").inInts(3, 7);
        assertThat(DictionaryEvaluator.evaluate(p, dicts)).isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void isNullIsNeverEliminated() {
        FilterPipeline.DictionaryLookup dicts = single("status", new Dictionary.IntDict(intBuf(1, 2, 3)));
        Predicate p = col("status").isNull();
        assertThat(DictionaryEvaluator.evaluate(p, dicts)).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void missingDictionaryYieldsNotApplied() {
        FilterPipeline.DictionaryLookup dicts = empty();
        Predicate p = col("status").eq(2);
        assertThat(DictionaryEvaluator.evaluate(p, dicts)).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void andEliminatesIfAnyChildEliminates() {
        FilterPipeline.DictionaryLookup dicts = single("status", new Dictionary.IntDict(intBuf(1, 2, 3)));
        Predicate p = col("status").eq(2).and(col("status").eq(99));
        assertThat(DictionaryEvaluator.evaluate(p, dicts)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void orEliminatesOnlyIfAllChildrenEliminate() {
        FilterPipeline.DictionaryLookup dicts = single("status", new Dictionary.IntDict(intBuf(1, 2, 3)));
        Predicate p = col("status").eq(99).or(col("status").eq(100));
        assertThat(DictionaryEvaluator.evaluate(p, dicts)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void orWithOneNonEliminatedChildIsNotApplied() {
        FilterPipeline.DictionaryLookup dicts = single("status", new Dictionary.IntDict(intBuf(1, 2, 3)));
        Predicate p = col("status").eq(99).or(col("status").eq(2));
        assertThat(DictionaryEvaluator.evaluate(p, dicts)).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void stringEqAgainstBinaryDictMatches() {
        Dictionary.BinaryDict dict = new Dictionary.BinaryDict(List.of(
                MemorySegment.ofArray("apple".getBytes(StandardCharsets.UTF_8)),
                MemorySegment.ofArray("banana".getBytes(StandardCharsets.UTF_8))));
        FilterPipeline.DictionaryLookup dicts = single("fruit", dict);
        Predicate p = col("fruit").eq("apple");
        assertThat(DictionaryEvaluator.evaluate(p, dicts)).isInstanceOf(PruningDecision.Inconclusive.class);
    }

    @Test
    void stringEqAgainstBinaryDictNoMatchIsEliminated() {
        Dictionary.BinaryDict dict = new Dictionary.BinaryDict(List.of(
                MemorySegment.ofArray("apple".getBytes(StandardCharsets.UTF_8)),
                MemorySegment.ofArray("banana".getBytes(StandardCharsets.UTF_8))));
        FilterPipeline.DictionaryLookup dicts = single("fruit", dict);
        Predicate p = col("fruit").eq("kiwi");
        assertThat(DictionaryEvaluator.evaluate(p, dicts)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void alwaysFalseIsEliminated() {
        FilterPipeline.DictionaryLookup dicts = empty();
        assertThat(DictionaryEvaluator.evaluate(Predicate.ALWAYS_FALSE, dicts))
                .isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void alwaysTrueIsNotApplied() {
        FilterPipeline.DictionaryLookup dicts = empty();
        assertThat(DictionaryEvaluator.evaluate(Predicate.ALWAYS_TRUE, dicts))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    private static FilterPipeline.DictionaryLookup single(String name, Dictionary<?> dict) {
        Map<ColumnPath, Dictionary<?>> map = new HashMap<>();
        map.put(ColumnPath.of(name), dict);
        return path -> Optional.ofNullable(map.get(path));
    }

    private static IntBuffer intBuf(int... values) {
        return IntBuffer.wrap(values);
    }

    private static FilterPipeline.DictionaryLookup empty() {
        return _ -> Optional.empty();
    }
}
