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
package org.geotools.data.nested;

import java.util.AbstractList;
import java.util.List;

/**
 * A read-only {@link List} view over one translated Parquet {@code LIST} value: its element {@link NestedType} and an
 * already-translated backing list whose elements are {@link Struct}, {@link TypedList}, {@link TypedMap}, or owned
 * scalars (never live {@code ParquetRecord} views).
 *
 * <p>Extending {@link AbstractList} makes every mutator throw {@link UnsupportedOperationException}, giving the
 * read-only contract for free. Presenting as a {@code List} lets a generic JSON encoder serialize the value as an
 * array.
 */
public final class TypedList extends AbstractList<Object> {

    private final NestedType elementType;
    private final List<Object> backing;

    public TypedList(NestedType elementType, List<Object> backing) {
        this.elementType = elementType;
        this.backing = backing;
    }

    /** The nested type of this list's elements. */
    public NestedType elementType() {
        return elementType;
    }

    @Override
    public Object get(int index) {
        return backing.get(index);
    }

    @Override
    public int size() {
        return backing.size();
    }
}
