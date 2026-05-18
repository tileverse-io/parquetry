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
package io.tileverse.parquetry.filter.eval;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ColumnPath;

class RecordLevelEvaluatorTest {

    @Test
    void eqMatchingValueIsTrue() {
        RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(col("year").eq(2020), row)).isTrue();
    }

    @Test
    void eqMismatchingValueIsFalse() {
        RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(col("year").eq(2019), row)).isFalse();
    }

    @Test
    void ltAndGtChain() {
        RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(
                        col("year").gt(2019).and(col("year").lt(2025)), row))
                .isTrue();
    }

    @Test
    void orShortCircuits() {
        RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(col("year").eq(2020).or(col("year").eq(2021)), row))
                .isTrue();
    }

    @Test
    void inMatchesIfAnyValuePresent() {
        RecordAccessor row = row(Map.of("year", 2021));
        assertThat(RecordLevelEvaluator.test(col("year").inInts(2020, 2021, 2022), row))
                .isTrue();
    }

    @Test
    void inFailsIfNoValueMatches() {
        RecordAccessor row = row(Map.of("year", 2025));
        assertThat(RecordLevelEvaluator.test(col("year").inInts(2020, 2021, 2022), row))
                .isFalse();
    }

    @Test
    void isNullOnAbsentColumnIsTrue() {
        RecordAccessor row = row(Map.of()); // empty
        assertThat(RecordLevelEvaluator.test(col("year").isNull(), row)).isTrue();
    }

    @Test
    void isNotNullOnPresentColumnIsTrue() {
        RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(col("year").isNotNull(), row)).isTrue();
    }

    @Test
    void valueComparisonAgainstNullIsFalse() {
        RecordAccessor row = row(Map.of()); // year missing
        assertThat(RecordLevelEvaluator.test(col("year").eq(2020), row)).isFalse();
        assertThat(RecordLevelEvaluator.test(col("year").gt(2019), row)).isFalse();
    }

    @Test
    void notEqAgainstNullIsFalse() {
        RecordAccessor row = row(Map.of());
        assertThat(RecordLevelEvaluator.test(col("year").notEq(2020), row)).isFalse();
    }

    @Test
    void stringEqMatches() {
        RecordAccessor row = row(Map.of("country", "AR"));
        assertThat(RecordLevelEvaluator.test(col("country").eq("AR"), row)).isTrue();
    }

    @Test
    void doubleLtMatches() {
        RecordAccessor row = row(Map.of("price", 1.5));
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
        RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(
                        col("year").eq(2020).and(col("year").eq(2021)), row))
                .isFalse();
    }

    @Test
    void notInverts() {
        RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(col("year").eq(2020).negate(), row))
                .isFalse();
        assertThat(RecordLevelEvaluator.test(col("year").eq(2019).negate(), row))
                .isTrue();
    }

    private static RecordAccessor row(Map<String, Object> values) {
        Map<ColumnPath, Object> byPath = new HashMap<>();
        values.forEach((k, v) -> byPath.put(ColumnPath.of(k), v));
        return byPath::get;
    }
}
