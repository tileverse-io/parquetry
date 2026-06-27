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
package io.tileverse.parquetry.arrow.ipc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.UnsupportedFeatureException;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class ArrowIpcWriterTest {

    private static ParquetSchema schema(SchemaNode.Primitive... leaves) {
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(leaves), Optional.empty(), -1));
    }

    @Test
    void roundTripsLongColumnThroughCanonicalReader() throws Exception {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), 0);
        ParquetSchema schema = schema(id);
        BitSet validBits = new BitSet();
        validBits.set(0, 3);
        Validity validity = Validity.of(validBits, 3);
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("id"), LongVector.materialized(new long[] {7, 8, 9}, validity));
        ParquetRecordBatch batch = new DefaultParquetRecordBatch(schema, columns, 3, Arena.ofShared());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ArrowIpcWriter.write(schema, Optional.empty(), Stream.of(batch), out);

        try (RootAllocator allocator = new RootAllocator();
                ArrowStreamReader reader =
                        new ArrowStreamReader(new ByteArrayInputStream(out.toByteArray()), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getRowCount()).isEqualTo(3);
            BigIntVector vector = (BigIntVector) root.getVector("id");
            assertThat(vector.get(0)).isEqualTo(7L);
            assertThat(vector.get(2)).isEqualTo(9L);
            assertThat(reader.loadNextBatch()).isFalse();
        }
    }

    @Test
    void rejectsUnsupportedColumnBeforeWritingAnyBytes() {
        SchemaNode.Primitive decimal = new SchemaNode.Primitive(
                "amount",
                Repetition.OPTIONAL,
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                OptionalInt.of(17),
                Optional.of(new LogicalType.Decimal(2, 40)),
                0);
        ParquetSchema schema = schema(decimal);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        Stream<ParquetRecordBatch> empty = Stream.empty();
        assertThatThrownBy(() -> ArrowIpcWriter.write(schema, Optional.empty(), empty, out))
                .isInstanceOf(UnsupportedFeatureException.class)
                .hasMessageContaining("DECIMAL");
        assertThat(out.size()).isZero();
    }
}
