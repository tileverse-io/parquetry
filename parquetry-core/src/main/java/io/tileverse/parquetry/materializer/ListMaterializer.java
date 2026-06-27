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
package io.tileverse.parquetry.materializer;

import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.LevelListVector;
import io.tileverse.parquetry.batch.LevelMapVector;
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.batch.MapVector;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.record.DefaultParquetRecord;
import io.tileverse.parquetry.record.RowColumns;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Builds a Java {@link List} from a row slice of a {@link ListVector}. Used by the row API view when a consumer reads a
 * list cell via {@code record.get(path)} or {@code record.readList(path)}.
 *
 * <p>Null vs empty: a row is null iff {@code vec.validity().isValid(rowIndex) == false}; an empty list iff
 * {@code offsets[i] == offsets[i+1]} AND validity is set.
 */
public final class ListMaterializer {

    private ListMaterializer() {}

    /**
     * Builds a Java {@link List} from row {@code rowIndex} of {@code vec}'s offset slice. Returns null when the row's
     * list is null per the vector's validity bitmap; an empty list materializes to {@link List#of()}.
     *
     * <p>The result is an immutable, lazy random-access view over the row's slice of the child vector: nothing is
     * copied up front, and repeated {@code get(i)} re-materializes the element. The view stays valid while the owning
     * batch is open; a caller retaining the list past the batch needs an explicit copy (or
     * {@link io.tileverse.parquetry.record.ParquetRecord#detach()} on the enclosing record).
     *
     * <p>The {@code schema} is threaded through to struct element materialization. Pass {@code null} only when the list
     * is known to contain no struct elements; a null schema on a struct element will propagate and cause a
     * NullPointerException in any nested-struct accessor call.
     *
     * <p>Null is intentional - distinguishes a null row from an empty list per the spec's empty-vs-null contract.
     */
    @SuppressWarnings({"java:S1452", "java:S1168"})
    public static List<?> materializeAt(ListVector vec, int rowIndex, ParquetSchema schema) {
        return materializeAt(vec, rowIndex, schema, null);
    }

    /**
     * Builds the list view sharing a batch-scoped {@code elementColumns} layout for struct elements. The caller
     * resolves the list's struct-element layout once per batch (see {@link RowColumns#listStructColumns(int)}) and
     * passes it in, which spares the per-cell layout rebuild the lazy fallback would pay. Pass {@code null} when no
     * shared layout is available (e.g. a nested list reached recursively); the view then builds the layout lazily on
     * first access.
     */
    @SuppressWarnings({"java:S1452", "java:S1168"})
    public static List<?> materializeAt(ListVector vec, int rowIndex, ParquetSchema schema, RowColumns elementColumns) {
        if (vec.isNull(rowIndex)) {
            return null;
        }
        int start = vec.rowOffsetStart(rowIndex);
        int end = vec.rowOffsetEnd(rowIndex);
        if (start == end) {
            return List.of();
        }
        ColumnVector child = vec.child();
        return new LazyListView(child, start, end, schema, elementColumns);
    }

    /**
     * Extracts the value at position {@code i} from {@code child}, dispatching by vector type.
     *
     * <p>A null element yields {@code null}. Leaf vectors delegate to {@link ColumnVector#get(int)}, which consults the
     * validity bitmap and returns a boxed primitive (or {@code null}) for primitive kinds, and the
     * {@link java.lang.foreign.MemorySegment} (or {@code null}) for binary and INT96 kinds.
     *
     * <p>The nested {@link ListVector}, {@link MapVector}, and {@link StructVector} arms are dispatched explicitly
     * because they need the {@code schema} context that {@code get} does not accept. A null {@link StructVector}
     * element yields {@code null}; a non-null element materializes to a {@link DefaultParquetRecord} over the struct's
     * {@link RowColumns}.
     *
     * <p>The list path resolves struct elements through {@link LazyListView}'s shared {@link RowColumns} instead of
     * this method; the map path keeps the per-element resolution here.
     */
    static Object valueAt(ColumnVector child, int i, ParquetSchema schema) {
        return switch (child) {
            case ListVector nested -> materializeAt(nested, i, schema);
            case MapVector nested -> MapMaterializer.materializeAt(nested, i, schema);
            case LevelListVector nested -> LevelListMaterializer.materializeAt(nested, i);
            case LevelMapVector nested -> LevelMapMaterializer.materializeAt(nested, i);
            case StructVector struct ->
                struct.isValid(i) ? new DefaultParquetRecord(RowColumns.ofStruct(schema, struct), i) : null;
            default -> child.get(i);
        };
    }

    /**
     * An immutable, random-access {@link List} view over one row's slice of a {@link ListVector}. Elements materialize
     * on demand in {@link #get(int)}; nothing is copied up front. The view is valid while the owning batch is open -
     * the same lifetime the struct elements inside the previously copied lists already had. Callers needing a detached
     * copy use an explicit copy loop or {@code List.copyOf} (which rejects null elements).
     *
     * <p>Struct elements share one {@link RowColumns}: a batch-scoped layout passed in by the caller, or, when none is
     * available, one built lazily on first access. Either way it replaces the per-element rebuild the eager path paid.
     * Each {@code get(int)} on a struct element returns a fresh record instance with identity equality; do not rely on
     * element identity or {@code contains}/{@code indexOf} over struct lists.
     */
    static final class LazyListView extends AbstractList<Object> implements RandomAccess {

        private final ColumnVector child;
        private final int start;
        private final int size;
        private final ParquetSchema schema;
        private RowColumns structElementColumns;

        LazyListView(ColumnVector child, int start, int end, ParquetSchema schema, RowColumns elementColumns) {
            this.child = child;
            this.start = start;
            this.size = end - start;
            this.schema = schema;
            this.structElementColumns = elementColumns;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public Object get(int index) {
            Objects.checkIndex(index, size);
            int position = start + index;
            if (child instanceof StructVector struct) {
                return struct.isValid(position) ? new DefaultParquetRecord(structColumns(struct), position) : null;
            }
            return valueAt(child, position, schema);
        }

        private RowColumns structColumns(StructVector struct) {
            if (structElementColumns == null) {
                structElementColumns = RowColumns.ofStruct(schema, struct);
            }
            return structElementColumns;
        }
    }
}
