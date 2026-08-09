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
package io.tileverse.parquetry.columnar;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * A {@link ParquetRecordBatch} view that adds writer-derived columns (the GeoParquet {@code bbox} covering struct) to a
 * wrapped batch's column map. The write path reads only {@link #columns()} and {@link #rowCount()}; those two present
 * the augmented view, while every other member delegates to the wrapped batch, which keeps owning its buffers.
 *
 * <p>This lives in the {@code columnar} package because {@link ParquetRecordBatch} is a sealed interface: an unnamed
 * module requires every permitted subtype in the interface's own package.
 */
public final class AugmentedRecordBatch implements ParquetRecordBatch {

    private final ParquetRecordBatch delegate;
    private final ParquetSchema projectedSchema;
    private final Map<ColumnPath, ColumnVector> columns;

    private AugmentedRecordBatch(
            ParquetRecordBatch delegate, ParquetSchema projectedSchema, Map<ColumnPath, ColumnVector> columns) {
        this.delegate = delegate;
        this.projectedSchema = projectedSchema;
        this.columns = Collections.unmodifiableMap(new LinkedHashMap<>(columns));
    }

    /**
     * A view of {@code delegate} presenting {@code projectedSchema} and {@code columns}, the wrapped batch's columns
     * plus the writer-derived ones. Insertion order is preserved, keeping the added columns after the existing ones.
     */
    public static AugmentedRecordBatch of(
            @NonNull ParquetRecordBatch delegate,
            @NonNull ParquetSchema projectedSchema,
            @NonNull Map<ColumnPath, ColumnVector> columns) {
        return new AugmentedRecordBatch(delegate, projectedSchema, columns);
    }

    @Override
    public ParquetSchema projectedSchema() {
        return projectedSchema;
    }

    @Override
    public int rowCount() {
        return delegate.rowCount();
    }

    @Override
    public Map<ColumnPath, ColumnVector> columns() {
        return columns;
    }

    @Override
    public ParquetRecord materialize(int rowIndex) {
        return delegate.materialize(rowIndex);
    }

    @Override
    public ParquetRecordBatch slice(int from, int count) {
        return delegate.slice(from, count);
    }

    @Override
    public long approximateHeapBytes() {
        return delegate.approximateHeapBytes();
    }

    @Override
    public void attachReleaseAction(Runnable releaseAction) {
        delegate.attachReleaseAction(releaseAction);
    }

    @Override
    public void registerBuffer(AutoCloseable buffer) {
        delegate.registerBuffer(buffer);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
