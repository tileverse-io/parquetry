/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package org.geotools.data.nested;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Set;

/**
 * A read-only {@link Map} view over one translated Parquet {@code MAP} value: its key and value {@link NestedType}s and
 * an already-translated backing map whose keys and values are {@link Struct}, {@link TypedList}, {@link TypedMap}, or
 * owned scalars (never live {@code ParquetRecord} views).
 *
 * <p>Extending {@link AbstractMap} makes every mutator throw {@link UnsupportedOperationException}, giving the
 * read-only contract for free. Presenting as a {@code Map} lets a generic JSON encoder serialize the value as an
 * object.
 */
// S2160: the Map contract requires equality by entries only; the inherited AbstractMap.equals is correct.
@SuppressWarnings("java:S2160")
public final class TypedMap extends AbstractMap<Object, Object> {

    private final NestedType keyType;
    private final NestedType valueType;
    private final Map<Object, Object> backing;

    public TypedMap(NestedType keyType, NestedType valueType, Map<Object, Object> backing) {
        this.keyType = keyType;
        this.valueType = valueType;
        this.backing = backing;
    }

    /** The nested type of this map's keys. */
    public NestedType keyType() {
        return keyType;
    }

    /** The nested type of this map's values. */
    public NestedType valueType() {
        return valueType;
    }

    @Override
    public Set<Entry<Object, Object>> entrySet() {
        return backing.entrySet();
    }
}
