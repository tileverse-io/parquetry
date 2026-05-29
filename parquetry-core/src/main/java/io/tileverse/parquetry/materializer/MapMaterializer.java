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

import java.util.LinkedHashMap;
import java.util.Map;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.MapVector;

/**
 * Builds a Java {@link Map} (insertion-ordered) from a row slice of a {@link MapVector}. Used by the row API view when
 * a consumer reads a map cell via {@code record.get(path)} or {@code record.getMap(path)}.
 *
 * <p>Null vs empty: a row is null iff {@code vec.validity().get(rowIndex) == false}; an empty map iff {@code offsets[i]
 * == offsets[i+1]} AND validity is set.
 *
 * <p>Key and value extraction reuses {@link ListMaterializer#valueAt(ColumnVector, int)} to avoid duplicating the
 * dispatch over {@link ColumnVector} subtypes.
 */
public final class MapMaterializer {

    private MapMaterializer() {}

    /**
     * Builds a Java {@link Map} from row {@code rowIndex} of {@code vec}'s offset slice. Returns null when the row's
     * map is null per the vector's validity bitmap; an empty map materializes to {@link Map#of()}.
     *
     * <p>Insertion order is preserved (Parquet's {@code key_value} record order) via {@link LinkedHashMap}.
     *
     * <p>Null is intentional - distinguishes a null row from an empty map per the spec's empty-vs-null contract.
     */
    // wildcard intentional - keys and values are heterogeneous per ColumnVector subtype
    @SuppressWarnings({"java:S1452", "java:S1168"})
    public static Map<?, ?> materializeAt(MapVector vec, int rowIndex) {
        if (!vec.validity().get(rowIndex)) {
            return null;
        }
        int start = vec.rowOffsetStart(rowIndex);
        int end = vec.rowOffsetEnd(rowIndex);
        if (start == end) {
            return Map.of();
        }
        ColumnVector keys = vec.keys();
        ColumnVector values = vec.values();
        LinkedHashMap<Object, Object> result = LinkedHashMap.newLinkedHashMap(end - start);
        for (int i = start; i < end; i++) {
            result.put(ListMaterializer.valueAt(keys, i), ListMaterializer.valueAt(values, i));
        }
        return result;
    }
}
