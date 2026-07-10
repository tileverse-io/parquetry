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
package io.tileverse.parquetry.conformance;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Bridges the parquetry record model to the dependency-free canonical tree the conformance corpus compares against the
 * parquet-java oracle. Dispatch is on the materialized runtime value; the schema node supplies struct field names and
 * the list-element / map-key-value node when a nested element is itself a record. The list/map node resolution mirrors
 * {@code GroupKind.of} and {@code DremelAssembler.elementNode}.
 */
final class CanonicalRow {

    private CanonicalRow() {}

    static Map<String, Object> fromParquetry(ParquetRecord row, ParquetSchema schema) {
        return canonicalizeStruct(row, schema.root());
    }

    static boolean deepEquals(Object a, Object b) {
        if (a == null || b == null) {
            return a == b;
        }
        if (a instanceof Map<?, ?> ma && b instanceof Map<?, ?> mb) {
            return mapsEqual(ma, mb);
        }
        if (a instanceof List<?> la && b instanceof List<?> lb) {
            return listsEqual(la, lb);
        }
        if (a instanceof ByteBuffer ba && b instanceof ByteBuffer bb) {
            return ba.equals(bb);
        }
        if (a instanceof Float fa && b instanceof Float fb) {
            return Float.compare(fa, fb) == 0;
        }
        if (a instanceof Double da && b instanceof Double db) {
            return Double.compare(da, db) == 0;
        }
        return Objects.equals(a, b);
    }

    private static boolean mapsEqual(Map<?, ?> a, Map<?, ?> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (Map.Entry<?, ?> entry : a.entrySet()) {
            if (!b.containsKey(entry.getKey())) {
                return false;
            }
            if (!deepEquals(entry.getValue(), b.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    private static boolean listsEqual(List<?> a, List<?> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!deepEquals(a.get(i), b.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static Map<String, Object> canonicalizeStruct(ParquetRecord structRecord, SchemaNode.Group group) {
        Map<String, Object> result =
                LinkedHashMap.newLinkedHashMap(group.children().size());
        for (SchemaNode child : group.children()) {
            Object raw = structRecord.get(ColumnPath.of(child.name()));
            result.put(child.name(), canonicalizeValue(raw, child));
        }
        return result;
    }

    private static Object canonicalizeValue(Object raw, SchemaNode node) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof MemorySegment segment) {
            return ByteBuffer.wrap(segment.toArray(JAVA_BYTE));
        }
        if (raw instanceof ParquetRecord nested) {
            return canonicalizeStruct(nested, (SchemaNode.Group) node);
        }
        if (raw instanceof Map<?, ?> map) {
            return canonicalizeMap(map, node);
        }
        if (raw instanceof List<?> list) {
            return canonicalizeList(list, node);
        }
        return raw;
    }

    private static Object canonicalizeMap(Map<?, ?> map, SchemaNode node) {
        SchemaNode.Group keyValue =
                (SchemaNode.Group) ((SchemaNode.Group) node).children().get(0);
        SchemaNode keyNode = keyValue.children().get(0);
        SchemaNode valueNode = keyValue.children().get(1);
        Map<Object, Object> result = LinkedHashMap.newLinkedHashMap(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            result.put(canonicalizeValue(entry.getKey(), keyNode), canonicalizeValue(entry.getValue(), valueNode));
        }
        return result;
    }

    private static Object canonicalizeList(List<?> list, SchemaNode node) {
        SchemaNode elementNode = elementNode(node);
        List<Object> result = new ArrayList<>(list.size());
        for (Object element : list) {
            result.add(canonicalizeValue(element, elementNode));
        }
        return result;
    }

    // Standard LIST: the element is the wrapper group's lone child (three-level) or the repeated primitive
    // (two-level). A legacy repeated node with no LIST annotation is its own element: a repeated primitive yields
    // scalars, a repeated group yields struct rows over all its children.
    private static SchemaNode elementNode(SchemaNode node) {
        if (node instanceof SchemaNode.Group group && isListAnnotated(group)) {
            SchemaNode repeated = group.children().get(0);
            if (repeated instanceof SchemaNode.Group wrapper) {
                return wrapper.children().get(0);
            }
            return repeated;
        }
        return node;
    }

    private static boolean isListAnnotated(SchemaNode.Group group) {
        return group.logicalType()
                .filter(LogicalType.ListType.class::isInstance)
                .isPresent();
    }
}
