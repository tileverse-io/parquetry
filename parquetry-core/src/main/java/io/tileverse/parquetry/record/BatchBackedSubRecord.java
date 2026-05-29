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
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;

import lombok.NonNull;

/**
 * {@link ParquetRecord} view over one row of a {@link StructVector}. Returned by
 * {@link BatchBackedParquetRecord#getStruct(ColumnPath)} and by the {@code get(col)} dispatch when the column is a
 * struct. Children are addressed by their absolute {@link ColumnPath} - the same paths the producing batch projected.
 *
 * <p>The enclosing batch's projected schema is shared with the sub-record so typed accessors can resolve the primitive
 * kind of nested leaves.
 */
public final class BatchBackedSubRecord implements ParquetRecord {

    private final StructVector struct;
    private final int rowIndex;
    private final ParquetSchema schema;

    BatchBackedSubRecord(@NonNull StructVector struct, int rowIndex, @NonNull ParquetSchema schema) {
        if (rowIndex < 0 || rowIndex >= struct.size()) {
            throw new IndexOutOfBoundsException("rowIndex " + rowIndex + " out of bounds [0, " + struct.size() + ")");
        }
        this.struct = struct;
        this.schema = schema;
        this.rowIndex = rowIndex;
    }

    @Override
    public ParquetSchema schema() {
        return schema;
    }

    @Override
    public Object get(ColumnPath col) {
        return BatchBackedRecords.get(struct.children(), rowIndex, col, schema);
    }

    @Override
    public boolean getBoolean(ColumnPath col) {
        return BatchBackedRecords.getBoolean(struct.children(), rowIndex, col, schema);
    }

    @Override
    public int getInt(ColumnPath col) {
        return BatchBackedRecords.getInt(struct.children(), rowIndex, col, schema);
    }

    @Override
    public long getLong(ColumnPath col) {
        return BatchBackedRecords.getLong(struct.children(), rowIndex, col, schema);
    }

    @Override
    public float getFloat(ColumnPath col) {
        return BatchBackedRecords.getFloat(struct.children(), rowIndex, col, schema);
    }

    @Override
    public double getDouble(ColumnPath col) {
        return BatchBackedRecords.getDouble(struct.children(), rowIndex, col, schema);
    }

    @Override
    public byte[] getBinary(ColumnPath col) {
        return BatchBackedRecords.getBinary(struct.children(), rowIndex, col, schema);
    }

    @Override
    public String getString(ColumnPath col) {
        return BatchBackedRecords.getString(struct.children(), rowIndex, col, schema);
    }

    @Override
    public boolean isNull(ColumnPath col) {
        return BatchBackedRecords.isNull(struct.children(), rowIndex, col);
    }

    /**
     * Returns the list cell at {@code col} as a {@code List<ParquetRecord>}.
     *
     * <p>See {@link BatchBackedParquetRecord#getList(ColumnPath)} for the unchecked-cast caveat on primitive lists.
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
        Map<ColumnPath, ColumnVector> children = struct.children();
        ColumnVector vec = children.get(col);
        if (vec == null) {
            throw new ParquetSchemaException("Column " + col.dot() + " is not present in this record");
        }
        if (!(vec instanceof StructVector inner)) {
            throw new ParquetSchemaException("Column " + col.dot() + " is not a struct column");
        }
        if (!inner.validity().get(rowIndex)) {
            return null;
        }
        return new BatchBackedSubRecord(inner, rowIndex, schema);
    }
}
