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
package io.tileverse.parquetry.iceberg;

import java.util.HashMap;
import java.util.Map;

import tools.jackson.databind.JsonNode;

/**
 * The nested topology of a table's current schema: every struct/list/map node keyed by Iceberg field id, with the id of
 * its enclosing nested node and its local name. Top-level primitive fields are not nested and are absent; a top-level
 * nested column has parent id {@link #ABSENT_PARENT}.
 *
 * <p>The reader reconciles only top-level primitives by field id; nested members are read by physical name. This
 * description is the table half of a fail-loud check that a data file's nested field ids agree with the table by name,
 * catching a nested rename (id stable, name changed) or an id reassignment (name reused for a new id) that would
 * otherwise read nulls where data exists or a wrong value. It holds just enough topology for that check: no types, no
 * Parquet projection.
 */
final class IcebergNestedSchema {

    /** Returned by {@link #parentOf}/{@link #nameOf}/{@link #idAt} when the queried node is not in the schema. */
    static final int ABSENT = Integer.MIN_VALUE;

    /** The parent id of a top-level nested column (its enclosing node is the table root, which has no field id). */
    static final int ABSENT_PARENT = 0;

    private final Map<Integer, NestedNode> byId;
    private final Map<Slot, Integer> idBySlot;

    private record NestedNode(int parentId, String localName) {}

    private record Slot(int parentId, String localName) {}

    private IcebergNestedSchema(Map<Integer, NestedNode> byId) {
        this.byId = Map.copyOf(byId);
        Map<Slot, Integer> idBySlot = new HashMap<>();
        for (Map.Entry<Integer, NestedNode> entry : byId.entrySet()) {
            NestedNode node = entry.getValue();
            idBySlot.put(new Slot(node.parentId(), node.localName()), entry.getKey());
        }
        this.idBySlot = Map.copyOf(idBySlot);
    }

    /** Parses the current schema's {@code fields} array, recording only nested nodes. */
    static IcebergNestedSchema of(JsonNode fields) {
        Map<Integer, NestedNode> byId = new HashMap<>();
        if (fields != null && fields.isArray()) {
            for (JsonNode field : fields) {
                addTopLevelField(field, byId);
            }
        }
        return new IcebergNestedSchema(byId);
    }

    boolean isEmpty() {
        return byId.isEmpty();
    }

    int parentOf(int fieldId) {
        NestedNode node = byId.get(fieldId);
        return node == null ? ABSENT : node.parentId();
    }

    String nameOf(int fieldId) {
        NestedNode node = byId.get(fieldId);
        return node == null ? null : node.localName();
    }

    /** The field id at the given parent-and-name slot, or {@link #ABSENT} when the table has no such slot. */
    int idAt(int parentId, String localName) {
        Integer id = idBySlot.get(new Slot(parentId, localName));
        return id == null ? ABSENT : id;
    }

    /** Whether the schema holds a node with this id. */
    boolean hasId(int fieldId) {
        return byId.containsKey(fieldId);
    }

    /** Whether the schema holds a node at this parent-and-name slot. */
    boolean hasSlot(int parentId, String localName) {
        return idBySlot.containsKey(new Slot(parentId, localName));
    }

    /** The dotted path to a node, chasing parent ids to the top-level column (e.g. {@code bbox.xmin}). */
    String path(int fieldId) {
        NestedNode node = byId.get(fieldId);
        if (node == null) {
            return null;
        }
        if (node.parentId() == ABSENT_PARENT) {
            return node.localName();
        }
        return path(node.parentId()) + "." + node.localName();
    }

    private static void addTopLevelField(JsonNode field, Map<Integer, NestedNode> byId) {
        JsonNode type = field.get("type");
        if (type == null || !type.isObject()) {
            return;
        }
        int id = requiredId(field, "a nested column");
        String name = requiredName(field, "a nested column");
        byId.put(id, new NestedNode(ABSENT_PARENT, name));
        addChildren(id, type, byId);
    }

    private static void addChildren(int parentId, JsonNode type, Map<Integer, NestedNode> byId) {
        JsonNode kindNode = type.get("type");
        if (kindNode == null || !kindNode.isString()) {
            throw new IcebergFormatException("malformed nested schema: nested type has no kind");
        }
        switch (kindNode.stringValue()) {
            case "struct" -> addStructMembers(parentId, type, byId);
            case "list" -> addMember(parentId, "element", type.get("element-id"), type.get("element"), byId);
            case "map" -> {
                addMember(parentId, "key", type.get("key-id"), type.get("key"), byId);
                addMember(parentId, "value", type.get("value-id"), type.get("value"), byId);
            }
            default -> {
                // A nested type object with an unrecognized kind has no comparable child ids.
            }
        }
    }

    private static void addStructMembers(int parentId, JsonNode type, Map<Integer, NestedNode> byId) {
        JsonNode members = type.get("fields");
        if (members == null || !members.isArray()) {
            return;
        }
        for (JsonNode member : members) {
            int id = requiredId(member, "a struct member");
            String name = requiredName(member, "a struct member");
            byId.put(id, new NestedNode(parentId, name));
            JsonNode memberType = member.get("type");
            if (memberType != null && memberType.isObject()) {
                addChildren(id, memberType, byId);
            }
        }
    }

    private static void addMember(
            int parentId, String token, JsonNode idNode, JsonNode childType, Map<Integer, NestedNode> byId) {
        if (idNode == null || !idNode.isIntegralNumber()) {
            throw new IcebergFormatException("malformed nested schema: missing integer id for a " + token + " member");
        }
        int id = idNode.intValue();
        byId.put(id, new NestedNode(parentId, token));
        if (childType != null && childType.isObject()) {
            addChildren(id, childType, byId);
        }
    }

    private static int requiredId(JsonNode node, String context) {
        JsonNode id = node.get("id");
        if (id == null || !id.isIntegralNumber()) {
            throw new IcebergFormatException("malformed nested schema: missing integer id for " + context);
        }
        return id.intValue();
    }

    private static String requiredName(JsonNode node, String context) {
        JsonNode name = node.get("name");
        if (name == null || !name.isString()) {
            throw new IcebergFormatException("malformed nested schema: missing name for " + context);
        }
        return name.stringValue();
    }
}
