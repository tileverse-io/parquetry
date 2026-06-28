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

import java.lang.foreign.Arena;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowPositionSet;
import io.tileverse.parquetry.filter.SortedLongPositionSet;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class RowIndexExcludedEvaluatorTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    // The caller names the synthesized row-position column; the engine mandates no fixed name.
    private static final ColumnPath POS = ColumnPath.of("_pos");

    @Test
    void keepsRowsWhosePositionIsNotDeleted() {
        ParquetRecordBatch batch = batchWithPositions(new int[] {10, 11, 12, 13});
        RowPositionSet deleted = SortedLongPositionSet.of(new long[] {1, 3});
        Predicate predicate = new Predicate.RowIndexExcluded(POS, deleted);

        BitSet survivors = VectorizedPredicateEvaluator.eval(predicate, batch);

        assertThat(survivors.stream().toArray()).containsExactly(0, 2);
    }

    @Test
    void intersectsWithAnotherLeafUnderAnd() {
        ParquetRecordBatch batch = batchWithPositions(new int[] {10, 11, 12, 13});
        RowPositionSet deleted = SortedLongPositionSet.of(new long[] {1, 3});
        Predicate predicate = new Predicate.And(
                List.of(new Predicate.RowIndexExcluded(POS, deleted), new Predicate.GtEq(ID, new Value.IntVal(12))));

        BitSet survivors = VectorizedPredicateEvaluator.eval(predicate, batch);

        assertThat(survivors.stream().toArray()).containsExactly(2);
    }

    private static ParquetRecordBatch batchWithPositions(int[] ids) {
        int rows = ids.length;
        BitSet allValid = new BitSet(rows);
        allValid.set(0, rows);
        Validity validity = Validity.of(allValid, rows);
        LongVector positions = LongVector.rowPositions(0L, Selection.ALL, rows);
        Map<ColumnPath, ColumnVector> columns = Map.of(ID, IntVector.materialized(ids, validity), POS, positions);
        return new DefaultParquetRecordBatch(schema(), columns, rows, Arena.ofConfined());
    }

    private static ParquetSchema schema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive pos = new SchemaNode.Primitive(
                POS.name(), Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(id, pos), Optional.empty(), -1));
    }
}
