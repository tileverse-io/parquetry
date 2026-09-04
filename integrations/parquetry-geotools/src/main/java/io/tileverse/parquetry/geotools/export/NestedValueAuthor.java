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
package io.tileverse.parquetry.geotools.export;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.geotools.data.nested.NestedType;

import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Authors a present list, map, or struct attribute value onto a {@link ParquetRecordBatchBuilder}, following the same
 * three-level LIST/MAP encoding and struct group shape {@link NestedSchemaNodes} builds the schema for.
 *
 * <p>The builder's scope verbs address a field by {@link ColumnPath} differently depending on where authoring is
 * happening: at the top level, or inside an open {@code beginStruct} scope, a path is absolute from the schema root (a
 * struct field's path repeats its enclosing struct's own name); inside an open {@code beginList} element or
 * {@code beginMap} entry, a path is relative to that container's repeated wrapper node ({@code element} for a list,
 * {@code key_value} for a map) and no {@code beginStruct} is used for a struct element or entry value - the relative
 * path alone reaches its fields. This class keeps those two authoring modes in separate method families:
 * {@code authorXxx} for the absolute mode, {@code authorRelativeXxx} for the mode inside a list element or map entry.
 *
 * <p>A container nested inside a container authors through the innermost open scope. A list or map that is DIRECTLY the
 * element or the entry value opens scope-anchored, with no path, through {@code addList()}/{@code addMap()}; a list or
 * map reached through an element's or entry value's struct fields opens with a wrapper-relative
 * {@code beginList}/{@code beginMap}. Either way the freshly opened scope authors its content exactly like a top-level
 * one - the element and entry verbs anchor at the innermost scope - and the recursion is uniform at every depth.
 */
final class NestedValueAuthor {

    private static final String LIST_ELEMENT_NAME = "element";
    private static final String MAP_ENTRY_NAME = "key_value";
    private static final String MAP_KEY_NAME = "key";
    private static final String MAP_VALUE_NAME = "value";

    private NestedValueAuthor() {}

    /**
     * Authors {@code value}, a present container of {@code type}, onto {@code builder} at the absolute {@code path}.
     */
    static void author(ParquetRecordBatchBuilder builder, ColumnPath path, NestedType type, Object value) {
        switch (type) {
            case NestedType.ListType list -> authorList(builder, path, list.element(), (List<?>) value);
            case NestedType.MapType map -> authorMap(builder, path, map.key(), map.value(), (Map<?, ?>) value);
            case NestedType.StructType struct -> authorStruct(builder, path, struct.fields(), (Map<?, ?>) value);
            case NestedType.ScalarType scalar -> throw new IllegalArgumentException("not a container: " + scalar);
            case NestedType.VariantType _ ->
                throw new IllegalArgumentException("Variant attributes are not writable: " + path.dot());
        }
    }

    // --- absolute mode: top level, or inside an open beginStruct scope ---

    private static void authorList(
            ParquetRecordBatchBuilder builder, ColumnPath path, NestedType elementType, List<?> values) {
        builder.beginList(path);
        authorElements(builder, path, elementType, values);
        builder.endList();
    }

    private static void authorMap(
            ParquetRecordBatchBuilder builder,
            ColumnPath path,
            NestedType keyType,
            NestedType valueType,
            Map<?, ?> entries) {
        builder.beginMap(path);
        authorEntries(builder, path, keyType, valueType, entries);
        builder.endMap();
    }

    private static void authorStruct(
            ParquetRecordBatchBuilder builder, ColumnPath path, List<NestedType.Field> fields, Map<?, ?> value) {
        builder.beginStruct(path);
        for (NestedType.Field field : fields) {
            ColumnPath childPath = ColumnPath.of(withPart(partsOf(path), field.name()));
            Object fieldValue = value == null ? null : value.get(field.name());
            authorAbsoluteField(builder, childPath, field.type(), fieldValue);
        }
        builder.endStruct();
    }

    /** Authors one struct field reached through an absolute path: a scalar leaf, a nested struct, or a container. */
    private static void authorAbsoluteField(
            ParquetRecordBatchBuilder builder, ColumnPath path, NestedType fieldType, Object fieldValue) {
        if (fieldValue == null) {
            builder.setNull(path);
            return;
        }
        switch (fieldType) {
            case NestedType.ScalarType scalar -> setScalar(builder, path, scalar.binding(), fieldValue);
            case NestedType.StructType struct -> authorStruct(builder, path, struct.fields(), (Map<?, ?>) fieldValue);
            case NestedType.ListType list -> authorList(builder, path, list.element(), (List<?>) fieldValue);
            case NestedType.MapType map -> authorMap(builder, path, map.key(), map.value(), (Map<?, ?>) fieldValue);
            case NestedType.VariantType _ ->
                throw new IllegalArgumentException("Variant attributes are not writable: " + path.dot());
        }
    }

    /** Authors every element of the innermost open list scope; the element verbs anchor there at every depth. */
    private static void authorElements(
            ParquetRecordBatchBuilder builder, ColumnPath attributePath, NestedType elementType, List<?> values) {
        for (Object element : values) {
            authorListElement(builder, attributePath, elementType, element);
        }
    }

    /**
     * A list element: a null slot, a scalar value, a struct authored through the element's relative path, or a nested
     * container opened with {@code addList()}/{@code addMap()}. {@code attributePath} names the enclosing attribute in
     * error messages only.
     */
    private static void authorListElement(
            ParquetRecordBatchBuilder builder, ColumnPath attributePath, NestedType elementType, Object element) {
        if (element == null) {
            builder.addNull();
            return;
        }
        switch (elementType) {
            case NestedType.ScalarType scalar -> addScalarElement(builder, scalar.binding(), element);
            case NestedType.StructType struct -> {
                builder.addElement();
                authorRelativeStructFields(builder, List.of(LIST_ELEMENT_NAME), struct.fields(), (Map<?, ?>) element);
                builder.endElement();
            }
            case NestedType.ListType list -> {
                builder.addList();
                authorElements(builder, attributePath, list.element(), (List<?>) element);
                builder.endList();
            }
            case NestedType.MapType map -> {
                builder.addMap();
                authorEntries(builder, attributePath, map.key(), map.value(), (Map<?, ?>) element);
                builder.endMap();
            }
            case NestedType.VariantType _ ->
                throw new IllegalArgumentException("Variant attributes are not writable: " + attributePath.dot());
        }
    }

    /**
     * Authors every entry of the innermost open map scope: the key through its wrapper-relative path, then the value.
     */
    private static void authorEntries(
            ParquetRecordBatchBuilder builder,
            ColumnPath attributePath,
            NestedType keyType,
            NestedType valueType,
            Map<?, ?> entries) {
        for (Map.Entry<?, ?> entry : entries.entrySet()) {
            builder.putEntry();
            authorMapKey(builder, attributePath, keyType, entry.getKey());
            authorEntryValue(builder, attributePath, valueType, entry.getValue());
            builder.endEntry();
        }
    }

    /**
     * The open entry's value: a null or a scalar set through the entry's relative path, a struct authored through that
     * same path prefix, or a nested container opened with {@code addList()}/{@code addMap()}.
     */
    private static void authorEntryValue(
            ParquetRecordBatchBuilder builder, ColumnPath attributePath, NestedType valueType, Object value) {
        ColumnPath valuePath = ColumnPath.of(MAP_ENTRY_NAME, MAP_VALUE_NAME);
        if (value == null) {
            builder.setNull(valuePath);
            return;
        }
        switch (valueType) {
            case NestedType.ScalarType scalar -> setScalar(builder, valuePath, scalar.binding(), value);
            case NestedType.StructType struct ->
                authorRelativeStructFields(
                        builder, List.of(MAP_ENTRY_NAME, MAP_VALUE_NAME), struct.fields(), (Map<?, ?>) value);
            case NestedType.ListType list -> {
                builder.addList();
                authorElements(builder, attributePath, list.element(), (List<?>) value);
                builder.endList();
            }
            case NestedType.MapType map -> {
                builder.addMap();
                authorEntries(builder, attributePath, map.key(), map.value(), (Map<?, ?>) value);
                builder.endMap();
            }
            case NestedType.VariantType _ ->
                throw new IllegalArgumentException("Variant attributes are not writable: " + attributePath.dot());
        }
    }

    private static void authorMapKey(
            ParquetRecordBatchBuilder builder, ColumnPath attributePath, NestedType keyType, Object key) {
        if (key == null) {
            throw new IllegalArgumentException("map key must not be null: " + attributePath.dot());
        }
        if (!(keyType instanceof NestedType.ScalarType scalar)) {
            throw new IllegalArgumentException("map key must be a scalar type, not " + keyType);
        }
        ColumnPath keyPath = ColumnPath.of(MAP_ENTRY_NAME, MAP_KEY_NAME);
        setScalar(builder, keyPath, scalar.binding(), key);
    }

    // --- relative mode: inside an open beginList element or beginMap entry, no beginStruct used ---

    private static void authorRelativeStructFields(
            ParquetRecordBatchBuilder builder,
            List<String> basePathParts,
            List<NestedType.Field> fields,
            Map<?, ?> value) {
        for (NestedType.Field field : fields) {
            List<String> childPathParts = withPart(basePathParts, field.name());
            Object fieldValue = value == null ? null : value.get(field.name());
            authorRelativeField(builder, childPathParts, field.type(), fieldValue);
        }
    }

    /**
     * Authors one field reached through a path relative to the active list element or map entry: a scalar leaf, a
     * nested struct (recursing with the same relative path prefix, without opening a new scope), or a list or map field
     * opened with a wrapper-relative {@code beginList}/{@code beginMap} and authored through its own scope.
     */
    private static void authorRelativeField(
            ParquetRecordBatchBuilder builder, List<String> pathParts, NestedType fieldType, Object fieldValue) {
        ColumnPath path = ColumnPath.of(pathParts);
        if (fieldValue == null) {
            builder.setNull(path);
            return;
        }
        switch (fieldType) {
            case NestedType.ScalarType scalar -> setScalar(builder, path, scalar.binding(), fieldValue);
            case NestedType.StructType struct ->
                authorRelativeStructFields(builder, pathParts, struct.fields(), (Map<?, ?>) fieldValue);
            case NestedType.ListType list -> {
                builder.beginList(path);
                authorElements(builder, path, list.element(), (List<?>) fieldValue);
                builder.endList();
            }
            case NestedType.MapType map -> {
                builder.beginMap(path);
                authorEntries(builder, path, map.key(), map.value(), (Map<?, ?>) fieldValue);
                builder.endMap();
            }
            case NestedType.VariantType _ ->
                throw new IllegalArgumentException("Variant attributes are not writable: " + path.dot());
        }
    }

    private static List<String> withPart(List<String> parts, String extra) {
        List<String> combined = new ArrayList<>(parts.size() + 1);
        combined.addAll(parts);
        combined.add(extra);
        return combined;
    }

    /**
     * The name segments of {@code path}, read out one by one rather than split on {@code '.'}, which a segment may
     * contain.
     */
    private static List<String> partsOf(ColumnPath path) {
        List<String> parts = new ArrayList<>(path.numParts());
        for (int i = 0; i < path.numParts(); i++) {
            parts.add(path.part(i));
        }
        return parts;
    }

    // --- scalar dispatch, shared by both modes ---

    private static void addScalarElement(ParquetRecordBatchBuilder builder, Class<?> binding, Object value) {
        if (binding == String.class) {
            builder.addString((String) value);
        } else if (binding == Integer.class) {
            builder.addInt(((Number) value).intValue());
        } else if (binding == Long.class) {
            builder.addLong(((Number) value).longValue());
        } else if (binding == Float.class) {
            builder.addFloat(((Number) value).floatValue());
        } else if (binding == Double.class) {
            builder.addDouble(((Number) value).doubleValue());
        } else if (binding == Boolean.class) {
            builder.addBoolean((Boolean) value);
        } else if (binding == byte[].class) {
            builder.addBinary(MemorySegment.ofArray((byte[]) value));
        } else {
            throw new IllegalArgumentException("unsupported nested scalar binding: " + binding.getName());
        }
    }

    private static void setScalar(ParquetRecordBatchBuilder builder, ColumnPath path, Class<?> binding, Object value) {
        if (binding == String.class) {
            builder.setString(path, (String) value);
        } else if (binding == Integer.class) {
            builder.setInt(path, ((Number) value).intValue());
        } else if (binding == Long.class) {
            builder.setLong(path, ((Number) value).longValue());
        } else if (binding == Float.class) {
            builder.setFloat(path, ((Number) value).floatValue());
        } else if (binding == Double.class) {
            builder.setDouble(path, ((Number) value).doubleValue());
        } else if (binding == Boolean.class) {
            builder.setBoolean(path, (Boolean) value);
        } else if (binding == byte[].class) {
            builder.setBinary(path, MemorySegment.ofArray((byte[]) value));
        } else {
            throw new IllegalArgumentException("unsupported nested scalar binding: " + binding.getName());
        }
    }
}
