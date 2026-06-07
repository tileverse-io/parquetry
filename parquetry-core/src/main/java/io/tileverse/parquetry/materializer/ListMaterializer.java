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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.tileverse.parquetry.batch.ColumnVector;
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
     * <p>The {@code schema} is threaded through to struct element materialization. Pass {@code null} only when the list
     * is known to contain no struct elements; a null schema on a struct element will propagate and cause a
     * NullPointerException in any nested-struct accessor call.
     *
     * <p>Null is intentional - distinguishes a null row from an empty list per the spec's empty-vs-null contract.
     */
    @SuppressWarnings({"java:S1452", "java:S1168"})
    public static List<?> materializeAt(ListVector vec, int rowIndex, ParquetSchema schema) {
        if (vec.isNull(rowIndex)) {
            return null;
        }
        int start = vec.rowOffsetStart(rowIndex);
        int end = vec.rowOffsetEnd(rowIndex);
        if (start == end) {
            return List.of();
        }
        ColumnVector child = vec.child();
        List<Object> result = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            result.add(valueAt(child, i, schema));
        }
        // An element may be null (a null nested list or a null primitive element); List.copyOf would reject it.
        return Collections.unmodifiableList(result);
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
     */
    static Object valueAt(ColumnVector child, int i, ParquetSchema schema) {
        return switch (child) {
            case ListVector nested -> materializeAt(nested, i, schema);
            case MapVector nested -> MapMaterializer.materializeAt(nested, i, schema);
            case StructVector struct ->
                struct.isValid(i) ? new DefaultParquetRecord(RowColumns.ofStruct(schema, struct), i) : null;
            default -> child.get(i);
        };
    }
}
