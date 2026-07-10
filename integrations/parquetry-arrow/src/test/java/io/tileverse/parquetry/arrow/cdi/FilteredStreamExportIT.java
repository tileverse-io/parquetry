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
package io.tileverse.parquetry.arrow.cdi;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.FilteredRecordBatch;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Proves the C Data stream densifies filtered batches, skips empty batches (a consumer never holds a zero-length
 * array), and releases every export: {@code outstandingExports()} settles back to zero after the drain.
 */
class FilteredStreamExportIT {

    private static final ColumnPath ID = ColumnPath.of("id");

    @Test
    void filteredAndEmptyBatchesDensifyAndSkip() throws Exception {
        ParquetSchema schema = idSchema();
        BitSet keep = new BitSet();
        keep.set(1);
        keep.set(3);
        ParquetRecordBatch filtered =
                FilteredRecordBatch.filtered(idBatch(schema, new long[] {7, 8, 9, 10}), keep, schema);
        ParquetRecordBatch empty = idBatch(schema, new long[0]);
        ParquetRecordBatch tail = idBatch(schema, new long[] {99});

        List<Long> ids = new ArrayList<>();
        try (RootAllocator allocator = new RootAllocator();
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
            ArrowCDataExporter.export(
                    schema, Optional.empty(), Stream.of(filtered, empty, tail), stream.memoryAddress());
            try (ArrowReader reader = Data.importArrayStream(allocator, stream)) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    assertThat(root.getRowCount()).isGreaterThan(0);
                    BigIntVector idVector = (BigIntVector) root.getVector("id");
                    for (int row = 0; row < root.getRowCount(); row++) {
                        ids.add(idVector.get(row));
                    }
                }
            }
        }

        assertThat(ids).containsExactly(8L, 10L, 99L);
        assertThat(ArrowCDataExporter.outstandingExports()).isZero();
    }

    private static ParquetSchema idSchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), 0);
        return new ParquetSchema(new SchemaNode.Group("root", Repetition.REQUIRED, List.of(id), Optional.empty(), -1));
    }

    private static ParquetRecordBatch idBatch(ParquetSchema schema, long[] values) {
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ID, LongVector.materialized(values, Validity.allValid(values.length)));
        return new DefaultParquetRecordBatch(schema, columns, values.length, Arena.ofShared());
    }
}
