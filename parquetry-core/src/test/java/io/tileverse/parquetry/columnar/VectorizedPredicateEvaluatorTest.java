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
package io.tileverse.parquetry.columnar;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.BitSet;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;

class VectorizedPredicateEvaluatorTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath PRICE = ColumnPath.of("price");

    @Test
    void isNotNullCountsValidRowsViaPopcount() {
        ParquetRecordBatch batch = BatchFixture.intColumn(ID, new int[] {1, 0, 3, 0, 5}, "10101");
        BitSet match = VectorizedPredicateEvaluator.eval(Pred.col("id").isNotNull(), batch);
        assertThat(match.cardinality()).isEqualTo(3);
    }

    @Test
    void comparisonExcludesNulls() {
        ParquetRecordBatch batch = BatchFixture.intColumn(ID, new int[] {1, 0, 3, 0, 5}, "10101");
        BitSet match = VectorizedPredicateEvaluator.eval(Pred.col("id").gt(2), batch);
        assertThat(match.cardinality()).isEqualTo(2);
    }

    @Test
    void andIntersectsChildMasks() {
        ParquetRecordBatch batch = BatchFixture.intColumn(ID, new int[] {1, 2, 3, 4, 5}, "11111");
        BitSet match = VectorizedPredicateEvaluator.eval(
                Pred.and(Pred.col("id").gt(1), Pred.col("id").lt(5)), batch);
        assertThat(match.cardinality()).isEqualTo(3);
    }

    @Test
    void orUnionsChildMasks() {
        ParquetRecordBatch batch = BatchFixture.intColumn(ID, new int[] {1, 2, 3, 4, 5}, "11111");
        BitSet match = VectorizedPredicateEvaluator.eval(
                Pred.or(Pred.col("id").lt(2), Pred.col("id").gt(3)), batch);
        assertThat(match.cardinality()).isEqualTo(3);
    }

    @Test
    void inMatchesValueListExcludingNulls() {
        ParquetRecordBatch batch = BatchFixture.intColumn(ID, new int[] {1, 0, 3, 4, 5}, "10111");
        BitSet match = VectorizedPredicateEvaluator.eval(Pred.col("id").inInts(1, 3, 5), batch);
        assertThat(match.cardinality()).isEqualTo(3);
    }

    @Test
    void negatedComparisonIncludesNullRows() {
        ParquetRecordBatch batch = BatchFixture.intColumn(ID, new int[] {1, 0, 3, 0, 5}, "10101");
        BitSet match = VectorizedPredicateEvaluator.eval(Pred.not(Pred.col("id").gt(2)), batch);
        assertThat(match.cardinality()).isEqualTo(3);
    }

    @Test
    void nanLiteralEqualitySelectsTheNaNCellsOfAFloatColumn() {
        ParquetRecordBatch batch = BatchFixture.floatColumn(PRICE, floatsWithNaNAndBothZeros());
        assertThat(rowsMatching(Pred.col("price").eq(Double.NaN), batch)).containsExactly(1, 3);
        assertThat(rowsMatching(Pred.col("price").notEq(Double.NaN), batch)).containsExactly(0, 2, 4, 5);
    }

    @Test
    void notEqualToANumberIncludesTheNaNCells() {
        ParquetRecordBatch batch = BatchFixture.floatColumn(PRICE, floatsWithNaNAndBothZeros());
        assertThat(rowsMatching(Pred.col("price").notEq(1.0), batch)).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    void orderedComparisonsSkipTheNaNCells() {
        ParquetRecordBatch batch = BatchFixture.floatColumn(PRICE, floatsWithNaNAndBothZeros());
        assertThat(rowsMatching(Pred.col("price").gtEq(0.0), batch)).containsExactly(0, 2, 4, 5);
        assertThat(rowsMatching(Pred.col("price").lt(Double.NaN), batch)).isEmpty();
    }

    @Test
    void theTwoZerosOfADoubleColumnAreEqual() {
        ParquetRecordBatch batch = BatchFixture.doubleColumn(PRICE, new double[] {-0.0, 0.0, Double.NaN, 2.0});
        assertThat(rowsMatching(Pred.col("price").eq(-0.0), batch)).containsExactly(0, 1);
        assertThat(rowsMatching(Pred.col("price").gt(0.0), batch)).containsExactly(3);
        assertThat(rowsMatching(Pred.col("price").ltEq(0.0), batch)).containsExactly(0, 1);
    }

    @Test
    void literalOfAnotherTypeKeepsTheOutcomeOfAnUnknownPair() {
        // Validation keeps such a literal away from a floating-point column; an unknown pair compares as equal.
        ParquetRecordBatch batch = BatchFixture.doubleColumn(PRICE, new double[] {1.0, Double.NaN});
        Predicate eqInteger = new Predicate.Eq(PRICE, new Value.IntVal(1));
        Predicate ltInteger = new Predicate.Lt(PRICE, new Value.IntVal(1));
        assertThat(rowsMatching(eqInteger, batch)).containsExactly(0, 1);
        assertThat(rowsMatching(ltInteger, batch)).isEmpty();
    }

    /** Row values: 1.0, NaN, -0.0, a NaN with another payload, +0.0, +Infinity. */
    private static float[] floatsWithNaNAndBothZeros() {
        float payloadNaN = Float.intBitsToFloat(0x7FC00001);
        return new float[] {1.0f, Float.NaN, -0.0f, payloadNaN, 0.0f, Float.POSITIVE_INFINITY};
    }

    private static List<Integer> rowsMatching(Predicate predicate, ParquetRecordBatch batch) {
        return VectorizedPredicateEvaluator.eval(predicate, batch).stream()
                .boxed()
                .toList();
    }

    @Test
    void isNullCountsComplementOfValidity() {
        ParquetRecordBatch batch = BatchFixture.intColumn(ID, new int[] {1, 0, 3, 0, 5}, "10101");
        BitSet match = VectorizedPredicateEvaluator.eval(Pred.col("id").isNull(), batch);
        assertThat(match.cardinality()).isEqualTo(2);
    }
}
