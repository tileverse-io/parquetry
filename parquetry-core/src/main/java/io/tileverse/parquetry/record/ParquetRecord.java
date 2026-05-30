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
 * <p>The sealed hierarchy has one implementation, {@link DefaultParquetRecord}, which adapts a column-keyed
 * {@link RowAccessor}. The row stream and the columnar batch both reach it through a {@code RowAccessor}, and nested
 * struct cells are the same type as top-level rows. Callers that want a different in-memory shape register a custom
 * {@code Materializer<T>} on the read pipeline rather than implementing this interface directly.
 *
 * <p>Typed accessors fail fast with {@link io.tileverse.parquetry.schema.ParquetSchemaException} when the requested
 * Java type does not match the column's physical {@code PrimitiveKind}. This is treated as a programming error:
 * predicates and projections are validated against the schema at read setup, and a type mismatch at access time means
 * the caller picked the wrong accessor for the column.
 */
public sealed interface ParquetRecord permits DefaultParquetRecord {

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
     * Returns the repeated values at {@code col} as a list of sub-records, or {@code null} when the row's value at
     * {@code col} is null. Throws {@link io.tileverse.parquetry.schema.ParquetSchemaException} when {@code col} is not
     * a list column.
     */
    List<ParquetRecord> getList(ColumnPath col);

    /**
     * Returns the struct at {@code col} as a sub-record, or {@code null} when the struct value at {@code col} is null.
     * Throws {@link io.tileverse.parquetry.schema.ParquetSchemaException} when {@code col} is not a struct column.
     */
    ParquetRecord getStruct(ColumnPath col);

    /**
     * Returns a self-contained copy of this record that owns its data and stays valid after the producing batch closes.
     *
     * <p>Records obtained from the read path are lazy views over a columnar batch: binary leaves are
     * {@link java.lang.foreign.MemorySegment} slices over per-page buffers, and nested cells reference the same batch.
     * The views are valid only during streaming iteration. A consumer that retains a record past the batch (a cache, a
     * buffered or sorted feature collection) must detach it first; the detached record copies every value out.
     *
     * <p>Detaching is idempotent: detaching an already-detached record yields an equivalent record. The detached record
     * shares the same immutable {@link #schema()} reference.
     */
    ParquetRecord detach();

    /**
     * Builds the canonical {@link ParquetRecord} view over an assembled {@link RowAccessor}. Used by built-in
     * materializers; user code typically obtains records through {@code ParquetDataset.read}.
     */
    static ParquetRecord of(ParquetSchema projectedSchema, RowAccessor row) {
        return new DefaultParquetRecord(projectedSchema, row);
    }
}
