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
package io.tileverse.parquetry.record;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.materializer.RowAccessor;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Adapter that exposes one row of a {@link ParquetRecordBatch} through the {@link RowAccessor} surface that custom
 * {@link io.tileverse.parquetry.materializer.Materializer materializers} consume.
 *
 * <p>Calls dispatch to {@link BatchBackedRecords}, the same helper {@link BatchBackedParquetRecord} uses, so leaf
 * values, list / map cells, and struct sub-records all flow through one decoding path.
 *
 * <p>The view is short-lived: it holds the batch and the row index by reference, never copying. Once the producing
 * batch is closed, results from {@link #get(ColumnPath)} that exposed {@code MemorySegment}s become invalid; callers
 * who need values past batch close must copy them out (e.g. via {@code segment.toArray(JAVA_BYTE)}).
 */
public final class BatchRowAccessor implements RowAccessor {

    private final ParquetRecordBatch batch;
    private final int rowIndex;

    /** Wraps row {@code rowIndex} of {@code batch} as a {@link RowAccessor}. */
    public BatchRowAccessor(@NonNull ParquetRecordBatch batch, int rowIndex) {
        int rowCount = batch.rowCount();
        if (rowIndex < 0 || rowIndex >= rowCount) {
            throw new IndexOutOfBoundsException("rowIndex %d out of bounds [0, %d)".formatted(rowIndex, rowCount));
        }
        this.batch = batch;
        this.rowIndex = rowIndex;
    }

    @Override
    public Object get(ColumnPath path) {
        return BatchBackedRecords.get(batch.columns(), rowIndex, path, batch.projectedSchema());
    }

    /**
     * Returns {@code true} when the column at {@code path} carries a struct group whose validity bit for this row is
     * clear. Returns {@code false} for any non-struct column: leaf nullness lives in the vector's validity mask and is
     * surfaced through {@link #get(ColumnPath)} returning {@code null}, not through this method.
     */
    @Override
    public boolean isGroupNull(ColumnPath path) {
        ColumnVector vec = batch.columns().get(path);
        if (vec instanceof StructVector struct) {
            return !struct.validity().get(rowIndex);
        }
        return false;
    }

    /**
     * Returns a snapshot of {@code (path -> value)} for every projected leaf column on this row.
     *
     * <p>Each entry's value is whatever {@link #get(ColumnPath)} would return for that path: a boxed primitive, a
     * {@code MemorySegment} for a binary leaf, {@code null} for a null leaf. The map iterates in the projected schema's
     * leaf order so downstream materializers see deterministic column order.
     */
    @Override
    public Map<ColumnPath, Object> values() {
        ParquetSchema schema = batch.projectedSchema();
        List<ColumnPath> leafColumns = schema.leafColumns();
        Map<ColumnPath, Object> snapshot = HashMap.newHashMap(leafColumns.size());
        leafColumns.forEach(leaf -> snapshot.put(leaf, get(leaf)));
        return snapshot;
    }
}
