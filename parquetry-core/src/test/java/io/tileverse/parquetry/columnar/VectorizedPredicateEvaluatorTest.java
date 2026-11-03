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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.GeometryFilter;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;

class VectorizedPredicateEvaluatorTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath PRICE = ColumnPath.of("price");
    private static final ColumnPath GEOM = ColumnPath.of("geom");
    private static final int ROWS = 20;

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

    // Validation keeps an integer literal away from a floating-point column: the pair has no comparison.
    @Test
    void doubleColumnRejectsIntLiteral() {
        ParquetRecordBatch batch = BatchFixture.doubleColumn(PRICE, new double[] {1.0, Double.NaN});
        Predicate eqInteger = new Predicate.Eq(PRICE, new Value.IntVal(1));

        assertThatThrownBy(() -> VectorizedPredicateEvaluator.eval(eqInteger, batch))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("IntVal");
    }

    @Test
    void floatColumnRejectsIntLiteral() {
        ParquetRecordBatch batch = BatchFixture.floatColumn(PRICE, new float[] {1.0f, Float.NaN});
        Predicate ltInteger = new Predicate.Lt(PRICE, new Value.IntVal(1));

        assertThatThrownBy(() -> VectorizedPredicateEvaluator.eval(ltInteger, batch))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("IntVal");
    }

    @Test
    void intColumnRejectsStringLiteral() {
        ParquetRecordBatch batch = BatchFixture.intColumn(ID, new int[] {1, 2, 3}, "111");
        Predicate eqString = new Predicate.Eq(ID, new Value.StringVal("1"));

        assertThatThrownBy(() -> VectorizedPredicateEvaluator.eval(eqString, batch))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("StringVal");
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

    @Test
    void evalWithinKeepsOnlyGivenRows() {
        Predicate firstTenRows = Pred.col("id").lt(10);

        BitSet kept = VectorizedPredicateEvaluator.evalWithin(firstTenRows, rowNumbers(), rows(2, 12));

        assertThat(kept).isEqualTo(rows(2));
    }

    @Test
    void evalWithinOrKeepsOnlyGivenRows() {
        Predicate firstTenRows = Pred.or(Pred.col("id").lt(5), Pred.col("id").lt(10));

        BitSet kept = VectorizedPredicateEvaluator.evalWithin(firstTenRows, rowNumbers(), rows(2, 12));

        assertThat(kept).isEqualTo(rows(2));
    }

    @Test
    void evalWithinRestrictsPerRowTests() {
        RecordingGeometryFilter filter = new RecordingGeometryFilter();
        Predicate geometryTest = Predicate.geometryFilter(filter);

        BitSet kept = VectorizedPredicateEvaluator.evalWithin(geometryTest, rowNumberCells(), rows(2, 12));

        assertThat(filter.seenRows).containsExactly(2, 12);
        assertThat(kept).isEqualTo(rows(2, 12));
    }

    @Test
    void evalWithinAndRestrictsPerRow() {
        RecordingGeometryFilter first = new RecordingGeometryFilter();
        RecordingGeometryFilter second = new RecordingGeometryFilter();
        Predicate bothTests = Pred.and(Predicate.geometryFilter(first), Predicate.geometryFilter(second));

        VectorizedPredicateEvaluator.evalWithin(bothTests, rowNumberCells(), rows(2, 12));

        assertThat(first.seenRows).containsExactly(2, 12);
        assertThat(second.seenRows).containsExactly(2, 12);
    }

    @Test
    void evalWithinLeavesItsInputAlone() {
        BitSet given = rows(2, 12);

        VectorizedPredicateEvaluator.evalWithin(Pred.col("id").lt(10), rowNumbers(), given);

        assertThat(given).isEqualTo(rows(2, 12));
    }

    private static BitSet rows(int... rows) {
        BitSet bits = new BitSet();
        for (int row : rows) {
            bits.set(row);
        }
        return bits;
    }

    /** A batch whose {@code id} column holds the number of each row. */
    private static ParquetRecordBatch rowNumbers() {
        int[] ids = IntStream.range(0, ROWS).toArray();
        return BatchFixture.intColumn(ID, ids, "1".repeat(ROWS));
    }

    /** A batch whose {@code geom} column holds the number of each row, in one byte. */
    private static ParquetRecordBatch rowNumberCells() {
        byte[] cells = new byte[ROWS];
        for (int row = 0; row < ROWS; row++) {
            cells[row] = (byte) row;
        }
        return BatchFixture.binaryColumn(GEOM, cells);
    }

    /** Accepts each row and records the numbers of the rows reaching its test. */
    private static final class RecordingGeometryFilter implements GeometryFilter<MemorySegment> {
        private final List<Integer> seenRows = new ArrayList<>();

        @Override
        public ColumnPath column() {
            return GEOM;
        }

        @Override
        public Optional<Predicate.Spatial> pruningPredicate() {
            return Optional.empty();
        }

        @Override
        public MemorySegment decode(MemorySegment wkb) {
            return wkb;
        }

        @Override
        public boolean matches(MemorySegment geometry) {
            seenRows.add((int) geometry.get(ValueLayout.JAVA_BYTE, 0));
            return true;
        }
    }
}
