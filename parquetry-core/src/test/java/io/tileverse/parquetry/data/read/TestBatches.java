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
package io.tileverse.parquetry.data.read;

import java.lang.foreign.Arena;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.batch.DefaultParquetRecordBatch;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/** Shared batch fixtures for the batch-source tests. */
final class TestBatches {

    private TestBatches() {}

    /** An empty batch over the given arena; its {@code approximateHeapBytes()} is zero. */
    static ParquetRecordBatch emptyBatch(Arena arena) {
        return new DefaultParquetRecordBatch(singleIntColumnSchema(), Map.of(), 0, arena);
    }

    /**
     * A batch whose {@code approximateHeapBytes()} is at least {@code targetBytes}, sized through its
     * {@code IntVector}. Lets a test pick a heap footprint that interacts with a {@link DecodeBudget} of a known
     * capacity.
     */
    static ParquetRecordBatch intBatchOfAtLeastBytes(long targetBytes) {
        int count = (int) Math.max(1L, (targetBytes / Integer.BYTES) + 1L);
        return intBatch(count);
    }

    /**
     * A batch holding one {@code IntVector} of {@code count} values, hence a non-zero {@code approximateHeapBytes()}.
     */
    static ParquetRecordBatch intBatch(int count) {
        Arena arena = Arena.ofShared();
        int[] values = new int[count];
        for (int i = 0; i < count; i++) {
            values[i] = i;
        }
        BitSet validity = new BitSet(count);
        validity.set(0, count);
        IntVector vector = IntVector.materialized(values, validity);
        Map<ColumnPath, io.tileverse.parquetry.batch.ColumnVector> columns = Map.of(ColumnPath.of("value"), vector);
        return new DefaultParquetRecordBatch(singleIntColumnSchema(), columns, count, arena);
    }

    static ParquetSchema singleIntColumnSchema() {
        return new ParquetSchema(new SchemaNode.Group(
                "root",
                Repetition.REQUIRED,
                List.of(new SchemaNode.Primitive(
                        "value", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1)),
                Optional.empty(),
                -1));
    }
}
