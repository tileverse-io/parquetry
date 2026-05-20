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
package io.tileverse.parquetry.materializer;

import java.util.ArrayList;
import java.util.List;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.FixedLenBinaryVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.Int96Vector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.MapVector;
import io.tileverse.parquetry.batch.StructVector;

/**
 * Builds a Java {@link List} from a row slice of a {@link ListVector}. Used by the row API view when a consumer reads a
 * list cell via {@code record.get(path)} or {@code record.getList(path)}.
 *
 * <p>Null vs empty: a row is null iff {@code vec.validity().get(rowIndex) == false}; an empty list iff
 * {@code offsets[i] == offsets[i+1]} AND validity is set.
 */
public final class ListMaterializer {

    private ListMaterializer() {}

    /**
     * Builds a Java {@link List} from row {@code rowIndex} of {@code vec}'s offset slice. Returns null when the row's
     * list is null per the vector's validity bitmap; an empty list materializes to {@link List#of()}.
     *
     * <p>Null is intentional - distinguishes a null row from an empty list per the spec's empty-vs-null contract.
     */
    // wildcard intentional - list elements are heterogeneous per ColumnVector subtype;
    // null is intentional - distinguishes a null row from an empty list per the spec's empty-vs-null contract
    @SuppressWarnings({"java:S1452", "java:S1168"})
    public static List<?> materializeAt(ListVector vec, int rowIndex) {
        if (!vec.validity().get(rowIndex)) {
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
            result.add(valueAt(child, i));
        }
        return List.copyOf(result);
    }

    /**
     * Extracts the value at position {@code i} from {@code child}, dispatching by vector type.
     *
     * <p>{@link StructVector} returns null here because sub-record access comes through the row API view, not through a
     * flat materializer. Lists-of-structs are deferred to that layer.
     */
    static Object valueAt(ColumnVector child, int i) {
        return switch (child) {
            case IntVector v -> v.get(i);
            case LongVector v -> v.get(i);
            case FloatVector v -> v.get(i);
            case DoubleVector v -> v.get(i);
            case BooleanVector v -> v.get(i);
            case BinaryVector v -> v.get(i);
            case FixedLenBinaryVector v -> v.get(i);
            case Int96Vector v -> v.get(i);
            case ListVector nested -> materializeAt(nested, i);
            case MapVector nested -> MapMaterializer.materializeAt(nested, i);
            case StructVector _ -> null;
        };
    }
}
