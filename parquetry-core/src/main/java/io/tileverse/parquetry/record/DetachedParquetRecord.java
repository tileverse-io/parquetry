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
package io.tileverse.parquetry.record;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.schema.UuidConverter;

/**
 * The owned {@link ParquetRecord}: a self-contained snapshot produced by {@link DefaultParquetRecord#detach()}. Each
 * column holds a detached value (a boxed primitive, an owned read-only {@link MemorySegment} for binary, a nested
 * detached record/list/map, or {@code null}). It stays valid after the producing batch closes and is safe to retain and
 * share across threads.
 *
 * <p>Multi-part column paths (e.g. {@code ColumnPath.of("s", "f")}) are resolved by walking into nested struct
 * sub-records segment by segment, mirroring the navigation that {@link DefaultParquetRecord} performs on live batches.
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

    // --- ColumnPath-based accessors: all delegate to locateNested for path navigation ---

    @Override
    public boolean isNull(ColumnPath col) {
        Located located = locateNested(col);
        return located == null || located.nested().isNull(located.index());
    }

    @Override
    public boolean getBoolean(ColumnPath col) {
        Located located = requireLocated(col, "getBoolean");
        return located.nested().getBoolean(located.index());
    }

    @Override
    public int getInt(ColumnPath col) {
        Located located = requireLocated(col, "getInt");
        return located.nested().getInt(located.index());
    }

    @Override
    public long getLong(ColumnPath col) {
        Located located = requireLocated(col, "getLong");
        return located.nested().getLong(located.index());
    }

    @Override
    public float getFloat(ColumnPath col) {
        Located located = requireLocated(col, "getFloat");
        return located.nested().getFloat(located.index());
    }

    @Override
    public double getDouble(ColumnPath col) {
        Located located = requireLocated(col, "getDouble");
        return located.nested().getDouble(located.index());
    }

    @Override
    public String getString(ColumnPath col) {
        Located located = requireLocated(col, "getString");
        return located.nested().getString(located.index());
    }

    @Override
    public byte[] getBinary(ColumnPath col) {
        Located located = requireLocated(col, "getBinary");
        return located.nested().getBinary(located.index());
    }

    @Override
    public UUID getUuid(ColumnPath col) {
        Located located = requireLocated(col, "getUuid");
        return located.nested().getUuid(located.index());
    }

    @Override
    public <R> R readBinary(ColumnPath col, BinaryView<R> view) {
        Located located = requireLocated(col, "readBinary");
        return located.nested().readBinary(located.index(), view);
    }

    @Override
    public ParquetRecord readStruct(ColumnPath col) {
        Located located = locateNested(col);
        return located == null ? null : located.nested().readStruct(located.index());
    }

    @Override
    public List<ParquetRecord> readList(ColumnPath col) {
        Located located = locateNested(col);
        return located == null ? null : located.nested().readList(located.index());
    }

    @Override
    public Map<?, ?> readMap(ColumnPath col) {
        Located located = locateNested(col);
        return located == null ? null : located.nested().readMap(located.index());
    }

    @Override
    public Object get(ColumnPath col) {
        Located located = locateNested(col);
        return located == null ? null : located.nested().get(located.index());
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

    /**
     * Locates a possibly-nested path by walking struct sub-records one segment at a time.
     *
     * <p>Returns the (record, colIndex) pair where the leaf resides. Returns {@code null} when the path is absent at
     * any level or when an intermediate struct cell is null.
     */
    private Located locateNested(ColumnPath col) {
        Integer directIndex = indexByPath.get(col);
        if (directIndex != null) {
            return new Located(this, directIndex);
        }
        if (col.numParts() <= 1) {
            return null;
        }
        String dotted = col.dot();
        DetachedParquetRecord level = this;
        int segmentStart = 0;
        while (true) {
            int separator = dotted.indexOf('.', segmentStart);
            int segmentEnd = separator < 0 ? dotted.length() : separator;
            ColumnPath segment = ColumnPath.of(dotted.substring(segmentStart, segmentEnd));
            Integer segmentIndex = level.indexByPath.get(segment);
            if (segmentIndex == null) {
                return null;
            }
            if (separator < 0) {
                return new Located(level, segmentIndex);
            }
            Object child = level.values[segmentIndex];
            if (!(child instanceof DetachedParquetRecord nestedRecord)) {
                return null;
            }
            level = nestedRecord;
            segmentStart = segmentEnd + 1;
        }
    }

    private Located requireLocated(ColumnPath col, String accessor) {
        Located located = locateNested(col);
        if (located == null) {
            throw new ParquetSchemaException("Column " + col.dot()
                    + " is not present in the projected schema or is unreachable through a null struct ancestor (accessor "
                    + accessor + ")");
        }
        return located;
    }

    private record Located(DetachedParquetRecord nested, int index) {}
}
