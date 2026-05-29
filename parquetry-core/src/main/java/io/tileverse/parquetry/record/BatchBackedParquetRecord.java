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
import java.util.Map;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;

import lombok.NonNull;

/**
 * Row-API view over one row of a {@link ParquetRecordBatch}. Accessor calls dispatch on the column's
 * {@link ColumnVector} subtype: primitives unbox their value, binaries materialize a fresh {@code byte[]}, list and map
 * cells materialize through the per-row helpers, and nested struct cells return a {@link BatchBackedSubRecord}.
 *
 * <p>Instances are short-lived: one per row, owned by the iterator. The batch and row index are stored by reference;
 * the record never copies them.
 *
 * <p>Typed primitive accessors require the leaf to be non-null. Callers must guard with {@link #isNull(ColumnPath)};
 * otherwise primitive returns throw {@link NullPointerException} (the underlying vector's {@code get(row)} dereferences
 * the dense primitive array regardless of validity). Only {@link #get(ColumnPath)} returns {@code null} for null
 * leaves.
 *
 * <p>Memory-segment views returned by {@link #get(ColumnPath)} for binary cells remain valid only as long as the
 * producing batch (and its underlying page Arenas) does. {@link #getBinary(ColumnPath)} and its UTF-8 / WKB siblings
 * copy the bytes out and are safe to retain past batch close.
 */
public final class BatchBackedParquetRecord implements ParquetRecord {

    private final ParquetRecordBatch batch;
    private final int rowIndex;

    /** Wraps row {@code rowIndex} of {@code batch} as a {@link ParquetRecord}. */
    public BatchBackedParquetRecord(@NonNull ParquetRecordBatch batch, int rowIndex) {
        if (rowIndex < 0 || rowIndex >= batch.rowCount()) {
            throw new IndexOutOfBoundsException(
                    "rowIndex " + rowIndex + " out of bounds [0, " + batch.rowCount() + ")");
        }
        this.batch = batch;
        this.rowIndex = rowIndex;
    }

    @Override
    public ParquetSchema schema() {
        return batch.projectedSchema();
    }

    @Override
    public Object get(ColumnPath col) {
        return BatchBackedRecords.get(batch.columns(), rowIndex, col, batch.projectedSchema());
    }

    @Override
    public boolean getBoolean(ColumnPath col) {
        return BatchBackedRecords.getBoolean(batch.columns(), rowIndex, col, batch.projectedSchema());
    }

    @Override
    public int getInt(ColumnPath col) {
        return BatchBackedRecords.getInt(batch.columns(), rowIndex, col, batch.projectedSchema());
    }

    @Override
    public long getLong(ColumnPath col) {
        return BatchBackedRecords.getLong(batch.columns(), rowIndex, col, batch.projectedSchema());
    }

    @Override
    public float getFloat(ColumnPath col) {
        return BatchBackedRecords.getFloat(batch.columns(), rowIndex, col, batch.projectedSchema());
    }

    @Override
    public double getDouble(ColumnPath col) {
        return BatchBackedRecords.getDouble(batch.columns(), rowIndex, col, batch.projectedSchema());
    }

    @Override
    public byte[] getBinary(ColumnPath col) {
        return BatchBackedRecords.getBinary(batch.columns(), rowIndex, col, batch.projectedSchema());
    }

    @Override
    public String getString(ColumnPath col) {
        return BatchBackedRecords.getString(batch.columns(), rowIndex, col, batch.projectedSchema());
    }

    @Override
    public boolean isNull(ColumnPath col) {
        return BatchBackedRecords.isNull(batch.columns(), rowIndex, col);
    }

    /**
     * Returns the list cell at {@code col} as a {@code List<ParquetRecord>}.
     *
     * <p>The cast is unchecked: a {@code ListVector} whose child is a primitive materializes to a list of boxed
     * primitives, not {@link ParquetRecord} sub-records. Callers that consume primitive lists should read through
     * {@link #get(ColumnPath)} instead, which returns the raw {@code List<?>}.
     */
    // null is intentional - distinguishes a null row from an empty list per the spec's empty-vs-null contract
    @Override
    @SuppressWarnings({"unchecked", "java:S1168"})
    public List<ParquetRecord> getList(ColumnPath col) {
        Object value = get(col);
        if (value == null) {
            return null;
        }
        if (!(value instanceof List<?> list)) {
            throw new ParquetSchemaException("Column " + col.dot() + " is not a list column");
        }
        return (List<ParquetRecord>) list;
    }

    @Override
    public ParquetRecord getStruct(ColumnPath col) {
        Map<ColumnPath, ColumnVector> columns = batch.columns();
        ColumnVector vec = columns.get(col);
        if (vec == null) {
            throw new ParquetSchemaException("Column " + col.dot() + " is not present in this record");
        }
        if (!(vec instanceof StructVector struct)) {
            throw new ParquetSchemaException("Column " + col.dot() + " is not a struct column");
        }
        if (!struct.validity().get(rowIndex)) {
            return null;
        }
        return new BatchBackedSubRecord(struct, rowIndex, batch.projectedSchema());
    }
}
