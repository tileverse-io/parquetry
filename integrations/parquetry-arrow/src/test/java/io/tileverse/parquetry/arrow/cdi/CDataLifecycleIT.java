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
import java.lang.foreign.MemorySegment;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Drains many small batches through the C Data stream and confirms the lifecycle contract: every borrowed buffer
 * returns to the pool by the end (no leak across many {@code get_next} cycles), and releasing the stream closes the
 * underlying batch source.
 */
class CDataLifecycleIT {

    private static final int BATCHES = 50;

    @Test
    void everyBatchReturnsBuffersAndReleaseClosesTheSource() throws Exception {
        SegmentPool pool = SegmentPool.create();
        ParquetSchema schema = idSchema();
        AtomicBoolean sourceClosed = new AtomicBoolean(false);
        Stream<ParquetRecordBatch> batches = IntStream.range(0, BATCHES)
                .mapToObj(batch -> smallBatch(schema, batch))
                .onClose(() -> sourceClosed.set(true));

        long outstandingBefore = ArrowCDataExporter.outstandingExports();
        long rows = 0;
        try (Arena arena = Arena.ofShared();
                RootAllocator allocator = new RootAllocator()) {
            MemorySegment streamSegment = arena.allocate(CDataLayouts.ARROW_ARRAY_STREAM);
            ArrowCDataExporter.export(schema, Optional.empty(), batches, pool, streamSegment);
            ArrowArrayStream cStream = ArrowArrayStream.wrap(streamSegment.address());

            try (ArrowReader reader = Data.importArrayStream(allocator, cStream)) {
                while (reader.loadNextBatch()) {
                    rows += reader.getVectorSchemaRoot().getRowCount();
                }
            }
        }

        assertThat(rows).isEqualTo(BATCHES);
        assertThat(sourceClosed).isTrue();
        assertThat(pool.stats().outstandingBorrows()).isZero();
        assertThat(ArrowCDataExporter.outstandingExports()).isEqualTo(outstandingBefore);
        pool.close();
    }

    private static ParquetRecordBatch smallBatch(ParquetSchema schema, int batch) {
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("id"), LongVector.materialized(new long[] {batch}, Validity.allValid(1)));
        return new DefaultParquetRecordBatch(schema, columns, 1, Arena.ofShared());
    }

    private static ParquetSchema idSchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), 0);
        return new ParquetSchema(new SchemaNode.Group("root", Repetition.REQUIRED, List.of(id), Optional.empty(), -1));
    }
}
