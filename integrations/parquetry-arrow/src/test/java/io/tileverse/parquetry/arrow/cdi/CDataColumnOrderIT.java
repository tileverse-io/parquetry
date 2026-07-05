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
package io.tileverse.parquetry.arrow.cdi;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.util.ArrayList;
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
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Pins the C Data stream's column pairing to schema declaration order. The exported {@code ArrowSchema} lists the
 * batch's columns in declaration order and the exported arrays must line up with it positionally; a codec that reorders
 * struct children (the root struct wraps every batch's columns) hands the consumer each column's data under a different
 * column's name. The anti-alphabetical schema here ({@code n} before {@code a}) fails against any alphabetical
 * reordering.
 */
class CDataColumnOrderIT {

    private static final ColumnPath N = ColumnPath.of("n");
    private static final ColumnPath A = ColumnPath.of("a");

    @Test
    void columnsExportInSchemaOrderNotAlphabetical() throws Exception {
        ParquetSchema schema = nBeforeASchema();
        ParquetRecordBatch batch = twoColumnBatch(schema, new long[] {1, 2, 3}, new long[] {10, 20, 30});

        List<String> fieldNames = new ArrayList<>();
        List<Long> nValues = new ArrayList<>();
        List<Long> aValues = new ArrayList<>();
        try (RootAllocator allocator = new RootAllocator();
                ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
            ArrowCDataExporter.export(schema, Optional.empty(), Stream.of(batch), stream.memoryAddress());
            try (ArrowReader reader = Data.importArrayStream(allocator, stream)) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    if (fieldNames.isEmpty()) {
                        root.getSchema().getFields().forEach(field -> fieldNames.add(field.getName()));
                    }
                    BigIntVector nVector = (BigIntVector) root.getVector("n");
                    BigIntVector aVector = (BigIntVector) root.getVector("a");
                    for (int row = 0; row < root.getRowCount(); row++) {
                        nValues.add(nVector.get(row));
                        aValues.add(aVector.get(row));
                    }
                }
            }
        }

        assertThat(fieldNames).containsExactly("n", "a");
        assertThat(nValues).containsExactly(1L, 2L, 3L);
        assertThat(aValues).containsExactly(10L, 20L, 30L);
    }

    private static ParquetSchema nBeforeASchema() {
        SchemaNode.Primitive n = int64Column("n", 0);
        SchemaNode.Primitive a = int64Column("a", 1);
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(n, a), Optional.empty(), -1));
    }

    private static SchemaNode.Primitive int64Column(String name, int fieldId) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), fieldId);
    }

    private static ParquetRecordBatch twoColumnBatch(ParquetSchema schema, long[] nValues, long[] aValues) {
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(N, LongVector.materialized(nValues, Validity.allValid(nValues.length)));
        columns.put(A, LongVector.materialized(aValues, Validity.allValid(aValues.length)));
        return new DefaultParquetRecordBatch(schema, columns, nValues.length, Arena.ofShared());
    }
}
