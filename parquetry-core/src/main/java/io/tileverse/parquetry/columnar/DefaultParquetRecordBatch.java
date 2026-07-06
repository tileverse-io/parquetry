/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.record.DefaultParquetRecord;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.record.RowColumns;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

public final class DefaultParquetRecordBatch implements ParquetRecordBatch {

    private final ParquetSchema projectedSchema;
    private final Map<ColumnPath, ColumnVector> columns;
    private final RowColumns rowColumns;
    private final int rowCount;
    private final Arena arena;
    private final List<AutoCloseable> ownedBuffers = new ArrayList<>();
    private boolean closed;
    private Runnable releaseAction;

    /**
     * Builds a heap-backed batch around vectors that own their bytes outright. Used when a batch is rebuilt from a
     * detached representation rather than decoded from a row group: the vectors already hold heap-owned values, and the
     * batch only needs a token Arena to satisfy the close contract. Vectors remain accessible after {@link #close()}.
     */
    public static DefaultParquetRecordBatch ofHeap(
            @NonNull ParquetSchema projectedSchema, @NonNull Map<ColumnPath, ColumnVector> columns, int rowCount) {
        return new DefaultParquetRecordBatch(projectedSchema, columns, rowCount, Arena.ofShared());
    }

    public DefaultParquetRecordBatch(
            @NonNull ParquetSchema projectedSchema,
            @NonNull Map<ColumnPath, ColumnVector> columns,
            int rowCount,
            @NonNull Arena arena) {

        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must be >= 0, got " + rowCount);
        }

        this.projectedSchema = projectedSchema;
        this.columns = Map.copyOf(columns);
        this.rowColumns = RowColumns.of(projectedSchema, projectedSchema.root(), this.columns);
        this.rowCount = rowCount;
        this.arena = arena;
    }

    @Override
    public ParquetSchema projectedSchema() {
        return projectedSchema;
    }

    @Override
    public int rowCount() {
        return rowCount;
    }

    @Override
    public Map<ColumnPath, ColumnVector> columns() {
        return columns;
    }

    @Override
    public ParquetRecord materialize(int rowIndex) {
        if (rowIndex < 0 || rowIndex >= rowCount) {
            throw new IndexOutOfBoundsException("rowIndex " + rowIndex + " out of bounds [0, " + rowCount + ")");
        }
        return new DefaultParquetRecord(rowColumns, rowIndex);
    }

    @Override
    public ParquetRecordBatch slice(int from, int count) {
        return FilteredRecordBatch.of(this, Selection.range(from, count), projectedSchema);
    }

    @Override
    public long approximateHeapBytes() {
        long total = 0L;
        for (ColumnVector vector : columns.values()) {
            total += vector.approximateHeapBytes();
        }
        return total;
    }

    @Override
    public void attachReleaseAction(Runnable releaseAction) {
        this.releaseAction = releaseAction;
    }

    @Override
    public void registerBuffer(AutoCloseable buffer) {
        ownedBuffers.add(buffer);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        closeOwnedBuffers();
        arena.close();
        if (releaseAction != null) {
            releaseAction.run();
        }
    }

    private void closeOwnedBuffers() {
        for (int i = ownedBuffers.size() - 1; i >= 0; i--) {
            try {
                ownedBuffers.get(i).close();
            } catch (Exception _) {
                // best-effort release; one failure must not strand the others
            }
        }
    }
}
