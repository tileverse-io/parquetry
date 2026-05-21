/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.batch;

import java.lang.foreign.Arena;
import java.util.Map;

import io.tileverse.parquetry.record.BatchBackedParquetRecord;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

public final class DefaultParquetRecordBatch implements ParquetRecordBatch {

    private final ParquetSchema projectedSchema;
    private final Map<ColumnPath, ColumnVector> columns;
    private final int rowCount;
    private final Arena arena;
    private boolean closed;

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
        return new BatchBackedParquetRecord(this, rowIndex);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        arena.close();
    }
}
