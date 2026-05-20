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
package io.tileverse.parquetry.record;

import java.util.List;

import io.tileverse.parquetry.materializer.RowAccessor;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * A single row of a Parquet result set, addressed by {@link ColumnPath}.
 *
 * <p>The sealed hierarchy intentionally exposes only one implementation: {@link DefaultParquetRecord}. Callers that
 * want a different in-memory shape register a custom {@code Materializer<T>} on the read pipeline rather than
 * implementing this interface directly.
 *
 * <p>Typed accessors fail fast with {@link IllegalArgumentException} when the requested Java type does not match the
 * column's physical {@code PrimitiveKind}. This is treated as a programming error: predicates and projections are
 * validated against the schema at read setup, so a type mismatch at access time means the caller picked the wrong
 * accessor for the column.
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
     * Returns the raw WKB bytes for a GeoParquet 2.0 {@code Geometry} column.
     *
     * <p>Currently this is a thin alias over {@link #getBinary(ColumnPath)}; logical-type detection that picks the
     * right accessor automatically lands with the GeoParquet integration in a future release.
     */
    byte[] getGeometryBytes(ColumnPath col);

    /**
     * Returns the raw WKB bytes for a GeoParquet 2.0 {@code Geography} column.
     *
     * <p>Currently this is a thin alias over {@link #getBinary(ColumnPath)}; logical-type detection that picks the
     * right accessor automatically lands with the GeoParquet integration in a future release.
     */
    byte[] getGeographyBytes(ColumnPath col);

    /**
     * Returns {@code true} when the value at {@code col} is null, either because the leaf itself was null or because an
     * ancestor group was null in the source row.
     */
    boolean isNull(ColumnPath col);

    /**
     * Returns the repeated values at {@code col} as a list of sub-records.
     *
     * <p>Not yet implemented: the Dremel assembler currently emits flat rows only; nested list materialization is a
     * follow-up.
     */
    List<ParquetRecord> getList(ColumnPath col);

    /**
     * Returns the struct at {@code col} as a sub-record.
     *
     * <p>Not yet implemented: the Dremel assembler currently emits flat rows only; nested struct materialization is a
     * follow-up.
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
