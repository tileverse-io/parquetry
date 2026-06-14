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
package io.tileverse.parquetry.geotools;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.tileverse.parquetry.geotools.NestedType.Field;
import io.tileverse.parquetry.geotools.NestedType.StructType;

/**
 * A read-only {@link Map} view over one translated Parquet {@code STRUCT} value: its {@link StructType} and a
 * positional values array parallel to the struct's field list.
 *
 * <p>Extending {@link AbstractMap} makes every mutator throw {@link UnsupportedOperationException}, giving the
 * read-only contract for free. Presenting as a {@code Map} lets a generic JSON encoder (GeoServer's GeoJSON/Jackson
 * path) serialize the struct as an object without knowing about parquetry types.
 *
 * <p>The {@link StructType} is interned across rows; only the values array is per-instance. The values are already
 * translated to owned, batch-independent forms ({@link Struct}, {@link TypedList}, {@link TypedMap}, or owned scalars),
 * never live {@code ParquetRecord} views.
 */
public final class Struct extends AbstractMap<String, Object> {

    private final StructType type;
    private final Object[] values;

    public Struct(StructType type, Object[] values) {
        this.type = type;
        this.values = values;
    }

    /** The struct's nested type, shared across rows. */
    public StructType type() {
        return type;
    }

    @Override
    public Object get(Object key) {
        int index = indexOf(key);
        if (index < 0) {
            return null;
        }
        return values[index];
    }

    @Override
    public boolean containsKey(Object key) {
        return indexOf(key) >= 0;
    }

    @Override
    public int size() {
        return type.fields().size();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        List<Field> fields = type.fields();
        Map<String, Object> view = new LinkedHashMap<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            view.put(fields.get(i).name(), values[i]);
        }
        return Collections.unmodifiableSet(view.entrySet());
    }

    private int indexOf(Object key) {
        List<Field> fields = type.fields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(key)) {
                return i;
            }
        }
        return -1;
    }
}
