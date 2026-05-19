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

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ColumnPath;

class RecordLevelEvaluatorTest {

    @Test
    void eqMatchingValueIsTrue() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(col("year").eq(2020), row)).isTrue();
    }

    @Test
    void eqMismatchingValueIsFalse() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(col("year").eq(2019), row)).isFalse();
    }

    @Test
    void ltAndGtChain() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(
                        col("year").gt(2019).and(col("year").lt(2025)), row))
                .isTrue();
    }

    @Test
    void orShortCircuits() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(col("year").eq(2020).or(col("year").eq(2021)), row))
                .isTrue();
    }

    @Test
    void inMatchesIfAnyValuePresent() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2021));
        assertThat(RecordLevelEvaluator.test(col("year").inInts(2020, 2021, 2022), row))
                .isTrue();
    }

    @Test
    void inFailsIfNoValueMatches() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2025));
        assertThat(RecordLevelEvaluator.test(col("year").inInts(2020, 2021, 2022), row))
                .isFalse();
    }

    @Test
    void isNullOnAbsentColumnIsTrue() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of()); // empty
        assertThat(RecordLevelEvaluator.test(col("year").isNull(), row)).isTrue();
    }

    @Test
    void isNotNullOnPresentColumnIsTrue() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(col("year").isNotNull(), row)).isTrue();
    }

    @Test
    void valueComparisonAgainstNullIsFalse() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of()); // year missing
        assertThat(RecordLevelEvaluator.test(col("year").eq(2020), row)).isFalse();
        assertThat(RecordLevelEvaluator.test(col("year").gt(2019), row)).isFalse();
    }

    @Test
    void notEqAgainstNullIsFalse() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of());
        assertThat(RecordLevelEvaluator.test(col("year").notEq(2020), row)).isFalse();
    }

    @Test
    void stringEqMatches() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("country", "AR"));
        assertThat(RecordLevelEvaluator.test(col("country").eq("AR"), row)).isTrue();
    }

    @Test
    void doubleLtMatches() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("price", 1.5));
        assertThat(RecordLevelEvaluator.test(col("price").lt(2.0), row)).isTrue();
    }

    @Test
    void alwaysTrueIsTrue() {
        assertThat(RecordLevelEvaluator.test(Predicate.ALWAYS_TRUE, row(Map.of())))
                .isTrue();
    }

    @Test
    void alwaysFalseIsFalse() {
        assertThat(RecordLevelEvaluator.test(Predicate.ALWAYS_FALSE, row(Map.of())))
                .isFalse();
    }

    @Test
    void andWithFailingChildIsFalse() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(
                        col("year").eq(2020).and(col("year").eq(2021)), row))
                .isFalse();
    }

    @Test
    void notInverts() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(col("year").eq(2020).negate(), row))
                .isFalse();
        assertThat(RecordLevelEvaluator.test(col("year").eq(2019).negate(), row))
                .isTrue();
    }

    private static RecordLevelEvaluator.RecordAccessor row(Map<String, Object> values) {
        Map<ColumnPath, Object> byPath = new HashMap<>();
        values.forEach((k, v) -> byPath.put(ColumnPath.of(k), v));
        return byPath::get;
    }
}
