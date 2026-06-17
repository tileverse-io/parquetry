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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.tileverse.parquetry.data.UuidConverter;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;

/**
 * The owned {@link ParquetRecord}: a self-contained snapshot produced by {@link DefaultParquetRecord#detach()}. Each
 * column holds a detached value (a boxed primitive, an owned read-only {@link MemorySegment} for binary, a nested
 * detached record/list/map, or {@code null}). It stays valid after the producing batch closes and is safe to retain and
 * share across threads.
 */
public final class DetachedParquetRecord implements ParquetRecord {

    private final ParquetSchema schema;
    private final ColumnPath[] paths;
    private final Object[] values;
    private final Map<ColumnPath, Integer> indexByPath;

    DetachedParquetRecord(ParquetSchema schema, ColumnPath[] paths, Object[] values) {
        this.schema = schema;
        this.paths = paths;
        this.values = values;
        Map<ColumnPath, Integer> index = HashMap.newHashMap(paths.length);
        for (int i = 0; i < paths.length; i++) {
            index.put(paths[i], i);
        }
        this.indexByPath = index;
    }

    @Override
    public ParquetSchema schema() {
        return schema;
    }

    @Override
    public int columnCount() {
        return paths.length;
    }

    @Override
    public ColumnPath columnPath(int col) {
        return paths[col];
    }

    @Override
    public boolean isNull(int col) {
        return values[col] == null;
    }

    @Override
    public boolean getBoolean(int col) {
        return (Boolean) values[col];
    }

    @Override
    public int getInt(int col) {
        return (Integer) values[col];
    }

    @Override
    public long getLong(int col) {
        return (Long) values[col];
    }

    @Override
    public float getFloat(int col) {
        return (Float) values[col];
    }

    @Override
    public double getDouble(int col) {
        return (Double) values[col];
    }

    @Override
    public String getString(int col) {
        MemorySegment segment = binaryAt(col);
        return segment == null ? null : new String(segment.toArray(JAVA_BYTE), StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBinary(int col) {
        MemorySegment segment = binaryAt(col);
        return segment == null ? null : segment.toArray(JAVA_BYTE);
    }

    @Override
    public UUID getUuid(int col) {
        MemorySegment segment = binaryAt(col);
        if (segment == null) {
            return null;
        }
        if (segment.byteSize() != UuidConverter.BYTES) {
            throw new ParquetSchemaException("Column " + paths[col].dot() + " is not a 16-byte fixed-length binary"
                    + " column; requested getUuid");
        }
        return UuidConverter.fromSegment(segment, 0L);
    }

    @Override
    public <R> R readBinary(int col, BinaryView<R> view) {
        MemorySegment segment = binaryAt(col);
        return segment == null ? null : view.read(segment, 0L, segment.byteSize());
    }

    private MemorySegment binaryAt(int col) {
        return (MemorySegment) values[col];
    }

    @Override
    public ParquetRecord readStruct(int col) {
        return (ParquetRecord) values[col];
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<ParquetRecord> readList(int col) {
        return (List<ParquetRecord>) values[col];
    }

    @Override
    public Map<?, ?> readMap(int col) {
        return (Map<?, ?>) values[col];
    }

    @Override
    public Object get(int col) {
        return values[col];
    }

    @Override
    public boolean isNull(ColumnPath col) {
        int index = indexOf(col);
        return index < 0 || isNull(index);
    }

    @Override
    public boolean getBoolean(ColumnPath col) {
        return getBoolean(requireIndex(col, "getBoolean"));
    }

    @Override
    public int getInt(ColumnPath col) {
        return getInt(requireIndex(col, "getInt"));
    }

    @Override
    public long getLong(ColumnPath col) {
        return getLong(requireIndex(col, "getLong"));
    }

    @Override
    public float getFloat(ColumnPath col) {
        return getFloat(requireIndex(col, "getFloat"));
    }

    @Override
    public double getDouble(ColumnPath col) {
        return getDouble(requireIndex(col, "getDouble"));
    }

    @Override
    public String getString(ColumnPath col) {
        return getString(requireIndex(col, "getString"));
    }

    @Override
    public byte[] getBinary(ColumnPath col) {
        return getBinary(requireIndex(col, "getBinary"));
    }

    @Override
    public UUID getUuid(ColumnPath col) {
        return getUuid(requireIndex(col, "getUuid"));
    }

    @Override
    public <R> R readBinary(ColumnPath col, BinaryView<R> view) {
        return readBinary(requireIndex(col, "readBinary"), view);
    }

    @Override
    public ParquetRecord readStruct(ColumnPath col) {
        int index = indexOf(col);
        return index < 0 ? null : readStruct(index);
    }

    @Override
    public List<ParquetRecord> readList(ColumnPath col) {
        int index = indexOf(col);
        return index < 0 ? null : readList(index);
    }

    @Override
    public Map<?, ?> readMap(ColumnPath col) {
        int index = indexOf(col);
        return index < 0 ? null : readMap(index);
    }

    @Override
    public Object get(ColumnPath col) {
        int index = indexOf(col);
        return index < 0 ? null : get(index);
    }

    @Override
    public List<Object> multiValue(ColumnPath leafPath) {
        return MultiValues.flatten(this, leafPath);
    }

    @Override
    public int multiValueSize(ColumnPath leafPath) {
        return MultiValues.flattenSize(this, leafPath);
    }

    @Override
    public ParquetRecord detach() {
        return this;
    }

    private int indexOf(ColumnPath col) {
        Integer index = indexByPath.get(col);
        return index == null ? -1 : index;
    }

    private int requireIndex(ColumnPath col, String accessor) {
        int index = indexOf(col);
        if (index < 0) {
            throw new ParquetSchemaException(
                    "Column " + col.dot() + " is not present in the projected schema (accessor " + accessor + ")");
        }
        return index;
    }
}
