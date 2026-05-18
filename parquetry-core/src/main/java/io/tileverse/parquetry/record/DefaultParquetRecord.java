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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.assembly.RowAccessor;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Schema;

/**
 * Lazy, per-row {@link ParquetRecord} that adapts the column-keyed {@link RowAccessor} produced by the Dremel assembler
 * into the typed accessor surface used by readers.
 *
 * <p>Instances are short-lived: one per row, owned by the iterator. Both the schema and row accessor are stored by
 * reference; the record never copies them.
 *
 * <p>Binary values arrive from {@link RowAccessor} as read-only {@link ByteBuffer}s. {@link #getBinary(ColumnPath)} and
 * its UTF-8 / WKB siblings materialize a fresh {@code byte[]} every time so callers can mutate the returned array
 * without affecting the underlying page buffer.
 *
 * <p>Typed primitive and binary accessors require that the leaf value be non-null. Callers must guard with
 * {@link #isNull(ColumnPath)} first; otherwise typed accessors throw {@link NullPointerException} (auto-unboxing for
 * primitive returns, or dereference of the underlying {@link ByteBuffer} for binary returns). Only
 * {@link #get(ColumnPath)} returns {@code null} for null leaves.
 */
public final class DefaultParquetRecord implements ParquetRecord {

    private final Schema schema;
    private final RowAccessor row;

    /** Wraps the given row, exposing it through the {@link ParquetRecord} accessor surface. */
    public DefaultParquetRecord(Schema schema, RowAccessor row) {
        this.schema = schema;
        this.row = row;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Object get(ColumnPath col) {
        return row.get(col);
    }

    @Override
    public boolean getBoolean(ColumnPath col) {
        requireKind(col, PrimitiveKind.BOOLEAN, "getBoolean");
        return (Boolean) row.get(col);
    }

    @Override
    public int getInt(ColumnPath col) {
        requireKind(col, PrimitiveKind.INT32, "getInt");
        return (Integer) row.get(col);
    }

    @Override
    public long getLong(ColumnPath col) {
        requireKind(col, PrimitiveKind.INT64, "getLong");
        return (Long) row.get(col);
    }

    @Override
    public float getFloat(ColumnPath col) {
        requireKind(col, PrimitiveKind.FLOAT, "getFloat");
        return (Float) row.get(col);
    }

    @Override
    public double getDouble(ColumnPath col) {
        requireKind(col, PrimitiveKind.DOUBLE, "getDouble");
        return (Double) row.get(col);
    }

    @Override
    public byte[] getBinary(ColumnPath col) {
        requireBinaryKind(col, "getBinary");
        return copyBytes(col);
    }

    @Override
    public String getString(ColumnPath col) {
        requireBinaryKind(col, "getString");
        return new String(copyBytes(col), StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getGeometryBytes(ColumnPath col) {
        requireBinaryKind(col, "getGeometryBytes");
        return copyBytes(col);
    }

    @Override
    public byte[] getGeographyBytes(ColumnPath col) {
        requireBinaryKind(col, "getGeographyBytes");
        return copyBytes(col);
    }

    @Override
    public boolean isNull(ColumnPath col) {
        Object value = row.get(col);
        return value == null || row.isGroupNull(col);
    }

    @Override
    public List<ParquetRecord> getList(ColumnPath col) {
        throw nestedNotYetSupported(col);
    }

    @Override
    public ParquetRecord getStruct(ColumnPath col) {
        throw nestedNotYetSupported(col);
    }

    private byte[] copyBytes(ColumnPath col) {
        ByteBuffer source = (ByteBuffer) row.get(col);
        ByteBuffer view = source.duplicate();
        byte[] out = new byte[view.remaining()];
        view.get(out);
        return out;
    }

    private void requireKind(ColumnPath col, PrimitiveKind expected, String accessor) {
        PrimitiveKind actual = lookupKind(col, accessor);
        if (actual != expected) {
            throw mismatch(col, actual, accessor);
        }
    }

    private void requireBinaryKind(ColumnPath col, String accessor) {
        PrimitiveKind actual = lookupKind(col, accessor);
        if (actual != PrimitiveKind.BYTE_ARRAY && actual != PrimitiveKind.FIXED_LEN_BYTE_ARRAY) {
            throw mismatch(col, actual, accessor);
        }
    }

    private PrimitiveKind lookupKind(ColumnPath col, String accessor) {
        Optional<Field> field = schema.find(col);
        if (field.isEmpty()) {
            throw new IllegalArgumentException(
                    "Column " + col.dot() + " is not present in the projected schema (accessor " + accessor + ")");
        }
        if (!(field.get() instanceof Field.Primitive primitive)) {
            throw new IllegalArgumentException(
                    "Column " + col.dot() + " is a group column; accessor " + accessor + " requires a primitive leaf");
        }
        return primitive.kind();
    }

    private static IllegalArgumentException mismatch(ColumnPath col, PrimitiveKind actual, String accessor) {
        return new IllegalArgumentException("Column " + col.dot() + " is " + actual + "; requested " + accessor);
    }

    private static UnsupportedOperationException nestedNotYetSupported(ColumnPath col) {
        return new UnsupportedOperationException(
                "repeated/nested columns land with RowGroupReader (column: " + col.dot() + ")");
    }
}
