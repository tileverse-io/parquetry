/*
 * Copyright (c) 2026 Multivers.io
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

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.schema.ColumnPath;

class VectorizedPredicateEvaluatorTest {

    private static final ColumnPath ID = ColumnPath.of("id");

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
    void isNullCountsComplementOfValidity() {
        ParquetRecordBatch batch = BatchFixture.intColumn(ID, new int[] {1, 0, 3, 0, 5}, "10101");
        BitSet match = VectorizedPredicateEvaluator.eval(Pred.col("id").isNull(), batch);
        assertThat(match.cardinality()).isEqualTo(2);
    }
}
