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
package io.tileverse.parquetry.iceberg;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The parsed subset of an Iceberg {@code metadata.json} this reader needs: format version, table location, the pinned
 * snapshot id, its manifest-list location, whether the table is partitioned, and the primitive fields of the current
 * schema (used to map a manifest bound's field id to a column name and type). Complex types (struct/list/map) are not
 * pruned in this cut and are omitted from {@link #fields()}.
 */
final class IcebergTableMetadata {

    private final int formatVersion;
    private final String tableLocation;
    private final long currentSnapshotId;
    private final long currentSnapshotTimestampMs;
    private final String manifestListLocation;
    private final boolean partitioned;
    private final List<IcebergField> fields;

    private IcebergTableMetadata(
            int formatVersion,
            String tableLocation,
            long currentSnapshotId,
            long currentSnapshotTimestampMs,
            String manifestListLocation,
            boolean partitioned,
            List<IcebergField> fields) {
        this.formatVersion = formatVersion;
        this.tableLocation = tableLocation;
        this.currentSnapshotId = currentSnapshotId;
        this.currentSnapshotTimestampMs = currentSnapshotTimestampMs;
        this.manifestListLocation = manifestListLocation;
        this.partitioned = partitioned;
        this.fields = List.copyOf(fields);
    }

    public int formatVersion() {
        return formatVersion;
    }

    public String tableLocation() {
        return tableLocation;
    }

    public long currentSnapshotId() {
        return currentSnapshotId;
    }

    public long currentSnapshotTimestampMs() {
        return currentSnapshotTimestampMs;
    }

    public String manifestListLocation() {
        return manifestListLocation;
    }

    public boolean isPartitioned() {
        return partitioned;
    }

    /** The primitive fields of the current schema, in declaration order; empty when the schema has none. */
    public List<IcebergField> fields() {
        return fields;
    }

    /** Parses {@code metadata.json} content, pinning the current snapshot. */
    public static IcebergTableMetadata read(String json) {
        return read(json, IcebergOptions.defaults());
    }

    /** Parses {@code metadata.json}, pinning {@code options.snapshotId()} when present, else the current snapshot. */
    public static IcebergTableMetadata read(String json, IcebergOptions options) {
        JsonNode root;
        try {
            root = JsonMapper.shared().readTree(json);
        } catch (JacksonException e) {
            throw new IcebergFormatException("malformed metadata.json", e);
        }
        int formatVersion = requiredInt(root, "format-version");
        String tableLocation = requiredString(root, "location");
        long snapshotId = options.snapshotId().orElseGet(() -> requiredLong(root, "current-snapshot-id"));
        JsonNode snapshot = snapshotNode(root, snapshotId);
        long timestampMs = requiredLong(snapshot, "timestamp-ms");
        String manifestList = requiredString(snapshot, "manifest-list");
        boolean partitioned = anyPartitionSpecHasFields(root);
        List<IcebergField> fields = currentSchemaFields(root);
        return new IcebergTableMetadata(
                formatVersion, tableLocation, snapshotId, timestampMs, manifestList, partitioned, fields);
    }

    private static List<IcebergField> currentSchemaFields(JsonNode root) {
        JsonNode schema = currentSchemaNode(root);
        if (schema == null) {
            return List.of();
        }
        JsonNode fieldNodes = schema.get("fields");
        if (fieldNodes == null || !fieldNodes.isArray()) {
            return List.of();
        }
        List<IcebergField> fields = new ArrayList<>();
        for (JsonNode fieldNode : fieldNodes) {
            primitiveField(fieldNode).ifPresent(fields::add);
        }
        return fields;
    }

    private static JsonNode currentSchemaNode(JsonNode root) {
        JsonNode schemas = root.get("schemas");
        if (schemas != null && schemas.isArray()) {
            return schemaById(schemas, root.get("current-schema-id"));
        }
        JsonNode schema = root.get("schema");
        if (schema != null && schema.isObject()) {
            return schema;
        }
        return null;
    }

    private static JsonNode schemaById(JsonNode schemas, JsonNode currentSchemaId) {
        if (schemas.isEmpty()) {
            return null;
        }
        if (currentSchemaId == null || !currentSchemaId.isIntegralNumber()) {
            return schemas.get(0);
        }
        int wanted = currentSchemaId.intValue();
        for (JsonNode schema : schemas) {
            JsonNode id = schema.get("schema-id");
            if (id != null && id.isIntegralNumber() && id.intValue() == wanted) {
                return schema;
            }
        }
        return schemas.get(0);
    }

    private static Optional<IcebergField> primitiveField(JsonNode fieldNode) {
        if (fieldNode == null || !fieldNode.isObject()) {
            throw new IcebergFormatException("schema field is not an object");
        }
        JsonNode type = fieldNode.get("type");
        if (type == null || !type.isString()) {
            return Optional.empty();
        }
        int id = requiredInt(fieldNode, "id");
        String name = requiredString(fieldNode, "name");
        boolean required = optionalBoolean(fieldNode, "required");
        return Optional.of(new IcebergField(id, name, type.stringValue(), required));
    }

    private static boolean optionalBoolean(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull() || !value.isBoolean()) {
            return false;
        }
        return value.booleanValue();
    }

    private static JsonNode snapshotNode(JsonNode root, long snapshotId) {
        JsonNode snapshots = root.get("snapshots");
        if (snapshots == null || !snapshots.isArray()) {
            throw new IcebergFormatException("metadata.json has no snapshots array");
        }
        for (JsonNode snapshot : snapshots) {
            if (requiredLong(snapshot, "snapshot-id") == snapshotId) {
                return snapshot;
            }
        }
        throw new IcebergFormatException("no snapshot with id " + snapshotId);
    }

    private static boolean anyPartitionSpecHasFields(JsonNode root) {
        JsonNode specs = root.get("partition-specs");
        if (specs == null || !specs.isArray()) {
            return false;
        }
        for (JsonNode spec : specs) {
            JsonNode fields = spec.get("fields");
            if (fields != null && fields.isArray() && !fields.isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private static String requiredString(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull() || !value.isString()) {
            throw new IcebergFormatException("missing or non-string field: " + field);
        }
        return value.stringValue();
    }

    private static int requiredInt(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull() || !value.isIntegralNumber()) {
            throw new IcebergFormatException("missing or non-integer field: " + field);
        }
        return value.intValue();
    }

    private static long requiredLong(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull() || !value.isIntegralNumber()) {
            throw new IcebergFormatException("missing or non-integer field: " + field);
        }
        return value.longValue();
    }
}
