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

import java.util.HashMap;
import java.util.Map;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.materializer.RowAccessor;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Adapter that exposes one row of a {@link StructVector} through the {@link RowAccessor} API that custom
 * {@link io.tileverse.parquetry.materializer.Materializer materializers} consume.
 *
 * <p>Calls dispatch to {@link BatchBackedRecords}, the same helper used by {@link BatchRowAccessor}; leaf values, list
 * / map cells, and nested struct sub-records all flow through one decoding path.
 *
 * <p>The accessor is short-lived: it holds the struct and row index by reference, never copying. Once the producing
 * batch is closed, any {@code MemorySegment}s obtained from {@link #get(ColumnPath)} become invalid; callers who need
 * values past that point must copy them out (e.g. via {@code segment.toArray(JAVA_BYTE)}).
 */
public final class StructRowAccessor implements RowAccessor {

    private final StructVector struct;
    private final int rowIndex;
    private final ParquetSchema schema;

    /**
     * Wraps row {@code rowIndex} of {@code struct} as a {@link RowAccessor}.
     *
     * @param struct the struct vector to read from
     * @param rowIndex the row to expose; must be in {@code [0, struct.size())}
     * @param schema the projected schema used to resolve typed accessors and nested struct sub-records; may be
     *     {@code null} only when this struct has no nested-struct child and no typed accessor is used (a {@code null}
     *     schema passed to a nested-struct {@link #get} call will propagate to {@link BatchBackedRecords} and cause a
     *     NullPointerException there)
     * @throws IndexOutOfBoundsException when {@code rowIndex} is outside {@code [0, struct.size())}
     */
    public StructRowAccessor(@NonNull StructVector struct, int rowIndex, ParquetSchema schema) {
        int size = struct.size();
        if (rowIndex < 0 || rowIndex >= size) {
            throw new IndexOutOfBoundsException("rowIndex %d out of bounds [0, %d)".formatted(rowIndex, size));
        }
        this.struct = struct;
        this.rowIndex = rowIndex;
        this.schema = schema;
    }

    @Override
    public Object get(ColumnPath path) {
        return BatchBackedRecords.get(struct.children(), rowIndex, path, schema);
    }

    /**
     * Returns {@code true} when the child at {@code path} is a struct group whose validity bit for this row is clear.
     * Returns {@code false} for any non-struct child: leaf nullness is reflected by {@link #get(ColumnPath)} returning
     * {@code null}, not through this method.
     */
    @Override
    public boolean isGroupNull(ColumnPath path) {
        ColumnVector vec = struct.children().get(path);
        if (vec instanceof StructVector inner) {
            return !inner.validity().get(rowIndex);
        }
        return false;
    }

    /**
     * Returns a snapshot of {@code (path -> value)} for every child path in this struct row.
     *
     * <p>Each entry's value is whatever {@link #get(ColumnPath)} returns for that path: a boxed primitive, a
     * {@code MemorySegment} for a binary leaf, {@code null} for a null leaf.
     */
    @Override
    public Map<ColumnPath, Object> values() {
        Map<ColumnPath, Object> snapshot = HashMap.newHashMap(struct.children().size());
        struct.children().keySet().forEach(child -> snapshot.put(child, get(child)));
        return Map.copyOf(snapshot);
    }
}
