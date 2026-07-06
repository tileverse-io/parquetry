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
package io.tileverse.parquetry.geotools.data;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.geotools.data.nested.NestedType;
import org.geotools.data.nested.NestedType.Field;
import org.geotools.data.nested.NestedType.ListType;
import org.geotools.data.nested.NestedType.MapType;
import org.geotools.data.nested.NestedType.ScalarType;
import org.geotools.data.nested.NestedType.StructType;
import org.geotools.data.nested.NestedType.VariantType;
import org.geotools.data.nested.Struct;
import org.geotools.data.nested.TypedList;
import org.geotools.data.nested.TypedMap;

import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Translates a nested value read from a live {@link ParquetRecord} into an owned, batch-independent form.
 *
 * <p>The reader hands back live views: a struct column is a {@code ParquetRecord} sub-view, a list column is a
 * {@code List<?>} of scalars or sub-records, a map column is a {@code Map<?,?>}. Those views are valid only while the
 * producing batch is open, and parquetry recycles batch buffers. This translator copies each value into a
 * {@link Struct}, {@link TypedList}, {@link TypedMap}, or an owned scalar, retaining no {@code ParquetRecord}. The
 * result stays readable after the source batch closes, which lets a GeoTools feature keep nested attributes past the
 * scan.
 */
public final class NestedValues {

    private NestedValues() {}

    /**
     * Translates {@code value} (a value read from a {@link ParquetRecord}) of nested type {@code type} into an owned
     * form.
     */
    public static Object translate(Object value, NestedType type) {
        if (value == null) {
            return null;
        }
        return switch (type) {
            case StructType st -> translateStruct(value, st);
            case ListType lt -> translateList(value, lt);
            case MapType mt -> translateMap(value, mt);
            case ScalarType sc -> translateScalar(value, sc);
            case VariantType _ -> value;
        };
    }

    private static Object translateStruct(Object value, StructType type) {
        if (!(value instanceof ParquetRecord parquetRecord)) {
            return null;
        }
        List<Field> fields = type.fields();
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            Object fieldValue = parquetRecord.get(ColumnPath.of(field.name()));
            values[i] = translate(fieldValue, field.type());
        }
        return new Struct(type, values);
    }

    private static Object translateList(Object value, ListType type) {
        List<?> source = (List<?>) value;
        List<Object> translated = new ArrayList<>(source.size());
        for (Object element : source) {
            translated.add(translate(element, type.element()));
        }
        return new TypedList(type.element(), Collections.unmodifiableList(translated));
    }

    private static Object translateMap(Object value, MapType type) {
        Map<?, ?> source = (Map<?, ?>) value;
        Map<Object, Object> translated = LinkedHashMap.newLinkedHashMap(source.size());
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            Object key = translate(entry.getKey(), type.key());
            Object mapped = translate(entry.getValue(), type.value());
            translated.put(key, mapped);
        }
        return new TypedMap(type.key(), type.value(), Collections.unmodifiableMap(translated));
    }

    /**
     * Copies a scalar leaf into an owned value. A binary leaf comes back from {@link ParquetRecord#get(ColumnPath)} as
     * a read-only {@link MemorySegment}; this decodes it by the scalar's binding (UTF-8 {@code String} or a fresh
     * {@code byte[]}). Boxed primitives are already immutable and pass through; a {@code byte[]} is defensively cloned.
     */
    private static Object translateScalar(Object value, ScalarType type) {
        if (value instanceof MemorySegment segment) {
            return decodeSegment(segment, type.binding());
        }
        if (value instanceof byte[] bytes) {
            return bytes.clone();
        }
        return value;
    }

    private static Object decodeSegment(MemorySegment segment, Class<?> binding) {
        byte[] bytes = segment.toArray(JAVA_BYTE);
        if (binding == String.class) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return bytes;
    }
}
