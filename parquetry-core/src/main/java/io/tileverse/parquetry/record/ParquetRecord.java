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

import java.util.List;

import io.tileverse.parquetry.materializer.RowAccessor;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * A single row of a Parquet result set, addressed by {@link ColumnPath}.
 *
 * <p>The sealed hierarchy has two production implementations: {@link DefaultParquetRecord}, which adapts the
 * column-keyed {@code RowAccessor} produced by the Dremel assembler, and {@link BatchBackedParquetRecord} (with its
 * {@link BatchBackedSubRecord} sub-record view), which adapts one row of a
 * {@link io.tileverse.parquetry.batch.ParquetRecordBatch}. Callers that want a different in-memory shape register a
 * custom {@code Materializer<T>} on the read pipeline rather than implementing this interface directly.
 *
 * <p>Typed accessors fail fast with {@link IllegalArgumentException} when the requested Java type does not match the
 * column's physical {@code PrimitiveKind}. This is treated as a programming error: predicates and projections are
 * validated against the schema at read setup, so a type mismatch at access time means the caller picked the wrong
 * accessor for the column.
 */
public sealed interface ParquetRecord permits DefaultParquetRecord, BatchBackedParquetRecord, BatchBackedSubRecord {

    /** The projected schema this record was assembled against (not the full file schema). */
    ParquetSchema schema();

    /**
     * Returns the raw boxed value at {@code col}: a boxed primitive, a read-only
     * {@link java.lang.foreign.MemorySegment} for binary/INT96, or {@code null} when the leaf was null or absent from
     * the projection.
     */
    Object get(ColumnPath col);

    boolean getBoolean(ColumnPath col);

    int getInt(ColumnPath col);

    long getLong(ColumnPath col);

    float getFloat(ColumnPath col);

    double getDouble(ColumnPath col);

    /** Materializes a fresh {@code byte[]} for the binary column at {@code col} on every call. */
    byte[] getBinary(ColumnPath col);

    /** UTF-8 decodes the binary column at {@code col}. */
    String getString(ColumnPath col);

    /**
     * Returns {@code true} when the value at {@code col} is null, either because the leaf itself was null or because an
     * ancestor group was null in the source row.
     */
    boolean isNull(ColumnPath col);

    /**
     * Returns the repeated values at {@code col} as a list of sub-records.
     *
     * <p>Supported by {@link BatchBackedParquetRecord}. The {@link DefaultParquetRecord} adapter over the legacy
     * row-API path does not yet implement this and throws {@link UnsupportedOperationException}.
     */
    List<ParquetRecord> getList(ColumnPath col);

    /**
     * Returns the struct at {@code col} as a sub-record.
     *
     * <p>Supported by {@link BatchBackedParquetRecord}. The {@link DefaultParquetRecord} adapter over the legacy
     * row-API path does not yet implement this and throws {@link UnsupportedOperationException}.
     */
    ParquetRecord getStruct(ColumnPath col);

    /**
     * Builds the canonical {@link ParquetRecord} view over an assembled {@link RowAccessor}. Used by built-in
     * materializers; user code typically obtains records through {@code ParquetDataset.read}.
     */
    static ParquetRecord of(ParquetSchema projectedSchema, RowAccessor row) {
        return new DefaultParquetRecord(projectedSchema, row);
    }
}
