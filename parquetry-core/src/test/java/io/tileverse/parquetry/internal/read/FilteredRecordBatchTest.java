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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.FilteredRecordBatch;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class FilteredRecordBatchTest {

    private static final ColumnPath A = ColumnPath.of("a");
    private static final ColumnPath B = ColumnPath.of("b");

    @Test
    void filteredViewKeepsOnlyTheSetRowsAndTheOutputColumns() {
        ParquetRecordBatch source = intBatch(Map.of(A, new int[] {10, 11, 12, 13}, B, new int[] {20, 21, 22, 23}));
        BitSet keep = new BitSet();
        keep.set(1);
        keep.set(3);

        ParquetRecordBatch result = FilteredRecordBatch.filtered(source, keep, schemaOf(A));
        try {
            assertThat(result.rowCount()).isEqualTo(2);
            assertThat(result.columns().keySet()).containsExactly(A);
            IntVector survivingA = (IntVector) result.columns().get(A);
            assertThat(survivingA.getInt(0)).isEqualTo(11);
            assertThat(survivingA.getInt(1)).isEqualTo(13);
        } finally {
            result.close();
        }
    }

    @Test
    void projectDropsNonOutputColumnsButKeepsEveryRow() {
        ParquetRecordBatch source = intBatch(Map.of(A, new int[] {10, 11, 12}, B, new int[] {20, 21, 22}));

        ParquetRecordBatch result = FilteredRecordBatch.narrowed(source, schemaOf(A));
        try {
            assertThat(result.rowCount()).isEqualTo(3);
            assertThat(result.columns().keySet()).containsExactly(A);
        } finally {
            result.close();
        }
    }

    @Test
    void projectReturnsTheSourceUnchangedWhenAlreadyOutputShaped() {
        ParquetRecordBatch source = intBatch(Map.of(A, new int[] {10, 11, 12}));

        ParquetRecordBatch result = FilteredRecordBatch.narrowed(source, schemaOf(A));
        try {
            assertThat(result).isSameAs(source);
        } finally {
            result.close();
        }
    }

    @Test
    void filteredViewKeepsAllRowsWhenEveryBitIsSet() {
        ParquetRecordBatch source = intBatch(Map.of(A, new int[] {10, 11, 12, 13}));
        BitSet keep = new BitSet();
        keep.set(0, 4);

        ParquetRecordBatch result = FilteredRecordBatch.filtered(source, keep, schemaOf(A));
        try {
            assertThat(result.rowCount()).isEqualTo(4);
            IntVector survivingA = (IntVector) result.columns().get(A);
            assertThat(survivingA.getInt(0)).isEqualTo(10);
            assertThat(survivingA.getInt(1)).isEqualTo(11);
            assertThat(survivingA.getInt(2)).isEqualTo(12);
            assertThat(survivingA.getInt(3)).isEqualTo(13);
        } finally {
            result.close();
        }
    }

    @Test
    void closingTheSurvivorClosesTheSource() {
        ParquetRecordBatch source = intBatch(Map.of(A, new int[] {10, 11, 12, 13}, B, new int[] {20, 21, 22, 23}));
        FlagCloseable sourceClosed = new FlagCloseable();
        source.registerBuffer(sourceClosed);

        BitSet keep = new BitSet();
        keep.set(1);
        keep.set(3);
        ParquetRecordBatch survivor = FilteredRecordBatch.filtered(source, keep, schemaOf(A));

        survivor.close();

        assertThat(sourceClosed.closed).isTrue();
    }

    private static final class FlagCloseable implements AutoCloseable {
        private boolean closed;

        @Override
        public void close() {
            closed = true;
        }
    }

    private static ParquetRecordBatch intBatch(Map<ColumnPath, int[]> columnValues) {
        int rowCount = columnValues.values().iterator().next().length;
        Map<ColumnPath, ColumnVector> columns = LinkedHashMap.newLinkedHashMap(columnValues.size());
        for (Map.Entry<ColumnPath, int[]> entry : columnValues.entrySet()) {
            columns.put(entry.getKey(), IntVector.materialized(entry.getValue(), Validity.allValid(rowCount)));
        }
        return DefaultParquetRecordBatch.ofHeap(
                schemaOf(columnValues.keySet().toArray(ColumnPath[]::new)), columns, rowCount);
    }

    private static ParquetSchema schemaOf(ColumnPath... leaves) {
        List<SchemaNode> children = new ArrayList<>(leaves.length);
        for (ColumnPath leaf : leaves) {
            children.add(new SchemaNode.Primitive(
                    leaf.dot(), Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1));
        }
        return new ParquetSchema(new SchemaNode.Group("root", Repetition.REQUIRED, children, Optional.empty(), -1));
    }
}
