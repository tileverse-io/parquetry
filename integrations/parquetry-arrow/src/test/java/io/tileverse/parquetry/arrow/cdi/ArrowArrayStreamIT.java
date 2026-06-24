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
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Drives a multi-batch {@code ArrowArrayStream} through arrow-java's {@code Data.importArrayStream}, draining every
 * batch, to confirm the lazy pull yields every row across batches and that releasing the reader closes the underlying
 * batch source and returns every borrowed buffer to the pool.
 */
class ArrowArrayStreamIT {

    @Test
    void drainsEveryBatchAndClosesTheSourceOnRelease() throws Exception {
        SegmentPool pool = SegmentPool.create();
        ParquetSchema schema = idNameSchema();
        AtomicBoolean sourceClosed = new AtomicBoolean(false);
        Stream<ParquetRecordBatch> batches = Stream.of(batch(schema, 1L, "a", 2L, "b"), batch(schema, 3L, "c", 4L, "d"))
                .onClose(() -> sourceClosed.set(true));

        List<Long> ids = new ArrayList<>();
        List<String> names = new ArrayList<>();
        try (Arena arena = Arena.ofShared();
                RootAllocator allocator = new RootAllocator()) {
            MemorySegment streamSegment = arena.allocate(CDataLayouts.ARROW_ARRAY_STREAM);
            ArrowArrayStreamExporter.export(schema, Optional.empty(), batches, pool, streamSegment);

            ArrowArrayStream cStream = ArrowArrayStream.wrap(streamSegment.address());
            try (ArrowReader reader = Data.importArrayStream(allocator, cStream)) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    BigIntVector idVector = (BigIntVector) root.getVector("id");
                    VarCharVector nameVector = (VarCharVector) root.getVector("name");
                    for (int row = 0; row < root.getRowCount(); row++) {
                        ids.add(idVector.get(row));
                        names.add(new String(nameVector.get(row), StandardCharsets.UTF_8));
                    }
                }
            }
            assertThat(ids).containsExactly(1L, 2L, 3L, 4L);
            assertThat(names).containsExactly("a", "b", "c", "d");
            assertThat(sourceClosed).isTrue();
            assertThat(pool.stats().outstandingBorrows()).isZero();
        }
        pool.close();
    }

    private static ParquetRecordBatch batch(ParquetSchema schema, long id0, String name0, long id1, String name1) {
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("id"), LongVector.materialized(new long[] {id0, id1}, Validity.allValid(2)));
        columns.put(ColumnPath.of("name"), utf8(name0, name1));
        return new DefaultParquetRecordBatch(schema, columns, 2, Arena.ofShared());
    }

    private static BinaryVector utf8(String... values) {
        MemorySegment[] segments = new MemorySegment[values.length];
        for (int i = 0; i < values.length; i++) {
            segments[i] = MemorySegment.ofArray(values[i].getBytes(StandardCharsets.UTF_8));
        }
        return BinaryVector.materialized(segments, Validity.allValid(values.length));
    }

    private static ParquetSchema idNameSchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), 0);
        SchemaNode.Primitive name = new SchemaNode.Primitive(
                "name",
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                1);
        return new ParquetSchema(
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(id, name), Optional.empty(), -1));
    }
}
