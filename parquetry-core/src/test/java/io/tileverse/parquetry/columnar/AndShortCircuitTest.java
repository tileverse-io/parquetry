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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.GeometryFilter;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * An {@code AND} of a cheap columnar predicate and an expensive per-row geometry gate must run the gate only over the
 * rows the cheap predicate keeps, not the whole batch. Otherwise an {@code attribute AND intersects(...)} builds a
 * geometry for every row regardless of the attribute filter. A counting {@link GeometryFilter} records how many rows
 * reached the gate.
 */
class AndShortCircuitTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath GEOM = ColumnPath.of("geom");

    @Test
    void andRunsTheGeometryGateOnlyOnSurvivorsOfTheCheaperPredicate() {
        ParquetRecordBatch batch = idAndGeomBatch(new int[] {1, 2, 1, 2, 1}); // three id==1 rows
        CountingGeometryFilter filter = new CountingGeometryFilter();
        Predicate predicate = Pred.and(Pred.col("id").eq(1), new Predicate.GeometryFilterPredicate(filter));

        BitSet match = VectorizedPredicateEvaluator.eval(predicate, batch);

        assertThat(match.cardinality()).isEqualTo(3); // the gate accepts every row it sees
        assertThat(filter.gateCalls)
                .as("gate ran only on the id==1 survivors, not all five rows")
                .isEqualTo(3);
    }

    @Test
    void standaloneGeometryGateScansEveryValidRow() {
        ParquetRecordBatch batch = idAndGeomBatch(new int[] {1, 2, 1, 2, 1});
        CountingGeometryFilter filter = new CountingGeometryFilter();

        VectorizedPredicateEvaluator.eval(new Predicate.GeometryFilterPredicate(filter), batch);

        assertThat(filter.gateCalls)
                .as("with no cheaper predicate to narrow it, the gate scans all valid rows")
                .isEqualTo(5);
    }

    /** Counts the rows that reach {@link #gate} and accepts each one. The WKB bytes are never parsed. */
    private static final class CountingGeometryFilter implements GeometryFilter<Object> {
        private int gateCalls;

        @Override
        public ColumnPath column() {
            return GEOM;
        }

        @Override
        public Optional<Predicate.Spatial> pruningPredicate() {
            return Optional.empty();
        }

        @Override
        public Object decode(MemorySegment wkb) {
            return wkb;
        }

        @Override
        public boolean matches(Object geometry) {
            return true;
        }

        @Override
        public Optional<Object> gate(MemorySegment wkb) {
            gateCalls++;
            return Optional.of(wkb);
        }
    }

    /** A batch with an all-valid {@code id} int column and a one-byte-per-row {@code geom} binary column. */
    private static ParquetRecordBatch idAndGeomBatch(int[] ids) {
        int rows = ids.length;
        byte[] geomBytes = new byte[rows];
        int[] offsets = new int[rows + 1];
        for (int i = 0; i < rows; i++) {
            offsets[i + 1] = i + 1;
        }
        MemorySegment backing = MemorySegment.ofArray(geomBytes).asReadOnly();
        BinaryVector geom = BinaryVector.of(backing, IntSequence.of(offsets), Validity.allValid(rows));
        IntVector id = IntVector.materialized(ids, Validity.allValid(rows));

        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ID, id);
        columns.put(GEOM, geom);
        return new DefaultParquetRecordBatch(schema(), columns, rows, Arena.ofConfined());
    }

    private static ParquetSchema schema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive geom = new SchemaNode.Primitive(
                "geom", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, geom), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
