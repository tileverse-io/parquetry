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
package io.tileverse.parquetry.iceberg;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.filter.Value;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * The parsed subset of an Iceberg {@code metadata.json} this reader needs: format version, table location, the pinned
 * snapshot id, its manifest-list location, the table's partition model, the primitive fields of the current schema
 * (used to map a manifest bound's field id to a column name and type), and the name-mapping table property that
 * resolves id-less data files. Complex types (struct/list/map) are not pruned in this cut and are omitted from
 * {@link #fields()}; their nested topology is retained separately in {@link #nestedSchema()} for a fail-loud check.
 */
final class IcebergTableMetadata {

    private static final int PARTITION_FIELD_ID_BASE = 1000;
    private static final String NAME_MAPPING_PROPERTY = "schema.name-mapping.default";

    private final int formatVersion;
    private final String tableLocation;
    private final long currentSnapshotId;
    private final long currentSnapshotTimestampMs;
    private final String manifestListLocation;
    private final IcebergPartitionSpec partitionSpec;
    private final List<IcebergField> fields;
    private final Optional<IcebergNameMapping> nameMapping;
    private final IcebergNestedSchema nestedSchema;

    // The parsed metadata document hands the reader its cohesive per-table state in one place; these are
    // one-per-concern final fields, not a long argument list worth bundling into a parameter object.
    @SuppressWarnings("java:S107")
    private IcebergTableMetadata(
            int formatVersion,
            String tableLocation,
            long currentSnapshotId,
            long currentSnapshotTimestampMs,
            String manifestListLocation,
            IcebergPartitionSpec partitionSpec,
            List<IcebergField> fields,
            Optional<IcebergNameMapping> nameMapping,
            IcebergNestedSchema nestedSchema) {
        this.formatVersion = formatVersion;
        this.tableLocation = tableLocation;
        this.currentSnapshotId = currentSnapshotId;
        this.currentSnapshotTimestampMs = currentSnapshotTimestampMs;
        this.manifestListLocation = manifestListLocation;
        this.partitionSpec = partitionSpec;
        this.fields = List.copyOf(fields);
        this.nameMapping = nameMapping;
        this.nestedSchema = nestedSchema;
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
        return !partitionSpec.isEmpty();
    }

    /** The table's partition model, merged across every partition spec it has used. */
    public IcebergPartitionSpec partitionSpec() {
        return partitionSpec;
    }

    /** The primitive fields of the current schema, in declaration order; empty when the schema has none. */
    public List<IcebergField> fields() {
        return fields;
    }

    /** The table's name-mapping document for id-less data files, when the metadata declares one. */
    public Optional<IcebergNameMapping> nameMapping() {
        return nameMapping;
    }

    /** The nested (struct/list/map) topology of the current schema, empty when the table is flat. */
    public IcebergNestedSchema nestedSchema() {
        return nestedSchema;
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
        List<IcebergField> fields = currentSchemaFields(root);
        IcebergNestedSchema nestedSchema = IcebergNestedSchema.of(currentSchemaFieldNodes(root));
        IcebergPartitionSpec partitionSpec = IcebergPartitionSpec.of(partitionFields(root), fields);
        Optional<IcebergNameMapping> nameMapping = parseNameMapping(root);
        return new IcebergTableMetadata(
                formatVersion,
                tableLocation,
                snapshotId,
                timestampMs,
                manifestList,
                partitionSpec,
                fields,
                nameMapping,
                nestedSchema);
    }

    private static List<IcebergField> currentSchemaFields(JsonNode root) {
        JsonNode fieldNodes = currentSchemaFieldNodes(root);
        if (fieldNodes == null || !fieldNodes.isArray()) {
            return List.of();
        }
        List<IcebergField> fields = new ArrayList<>();
        for (JsonNode fieldNode : fieldNodes) {
            primitiveField(fieldNode).ifPresent(fields::add);
        }
        return fields;
    }

    private static JsonNode currentSchemaFieldNodes(JsonNode root) {
        JsonNode schema = currentSchemaNode(root);
        if (schema == null) {
            return null;
        }
        return schema.get("fields");
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
        String typeName = type.stringValue();
        if (IcebergGeometryType.isGeometryToken(typeName)) {
            IcebergGeometryType geometryType = IcebergGeometryType.parse(typeName);
            String baseKind = geometryType.baseKind();
            Optional<Value> geoInitialDefault = initialDefault(fieldNode, baseKind, name);
            return Optional.of(new IcebergField(
                    id, name, baseKind, required, geoInitialDefault, geometryType.crs(), geometryType.algorithm()));
        }
        Optional<Value> initialDefault = initialDefault(fieldNode, typeName, name);
        return Optional.of(new IcebergField(id, name, typeName, required, initialDefault));
    }

    /**
     * Reads a field's {@code initial-default} from Iceberg's JSON single-value serialization. A {@code date} default is
     * an ISO {@code YYYY-MM-DD} string (not epoch days); numeric and boolean defaults are JSON scalars. An unsupported
     * type fails fast rather than dropping the default and reading the column as null.
     */
    private static Optional<Value> initialDefault(JsonNode fieldNode, String type, String name) {
        JsonNode defaultNode = fieldNode.get("initial-default");
        if (defaultNode == null || defaultNode.isNull()) {
            return Optional.empty();
        }
        return Optional.of(decodeInitialDefault(type, defaultNode, name));
    }

    private static Value decodeInitialDefault(String type, JsonNode node, String name) {
        return switch (type) {
            case "int" -> new Value.IntVal(node.intValue());
            case "long" -> new Value.LongVal(node.longValue());
            case "float" -> new Value.FloatVal((float) node.doubleValue());
            case "double" -> new Value.DoubleVal(node.doubleValue());
            case "boolean" -> new Value.BoolVal(node.booleanValue());
            case "date" -> new Value.DateVal(parseDate(node.stringValue(), name));
            case "string" -> new Value.StringVal(node.stringValue());
            default ->
                throw new IcebergFormatException(
                        "cannot read initial-default for field %s of unsupported type %s".formatted(name, type));
        };
    }

    private static LocalDate parseDate(String text, String name) {
        try {
            return LocalDate.parse(text);
        } catch (DateTimeParseException e) {
            throw new IcebergFormatException(
                    "initial-default for date field %s is not an ISO date: %s".formatted(name, text), e);
        }
    }

    private static boolean optionalBoolean(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || value.isNull() || !value.isBoolean()) {
            return false;
        }
        return value.booleanValue();
    }

    private static Optional<IcebergNameMapping> parseNameMapping(JsonNode root) {
        JsonNode properties = root.get("properties");
        if (properties == null || !properties.isObject()) {
            return Optional.empty();
        }
        JsonNode mapping = properties.get(NAME_MAPPING_PROPERTY);
        if (mapping == null || mapping.isNull()) {
            return Optional.empty();
        }
        if (!mapping.isString()) {
            throw new IcebergFormatException("table property " + NAME_MAPPING_PROPERTY + " is not a string");
        }
        return Optional.of(IcebergNameMapping.fromJson(mapping.stringValue()));
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

    private static List<IcebergPartitionSpec.PartitionField> partitionFields(JsonNode root) {
        List<IcebergPartitionSpec.PartitionField> fields = new ArrayList<>();
        JsonNode specs = root.get("partition-specs");
        if (specs != null && specs.isArray()) {
            for (JsonNode spec : specs) {
                addPartitionFields(spec.get("fields"), fields);
            }
            return fields;
        }
        // A format-version-1 table may expose only the singular default spec under `partition-spec`.
        addPartitionFields(root.get("partition-spec"), fields);
        return fields;
    }

    private static void addPartitionFields(JsonNode specFields, List<IcebergPartitionSpec.PartitionField> into) {
        if (specFields == null || !specFields.isArray()) {
            return;
        }
        int position = 0;
        for (JsonNode field : specFields) {
            into.add(new IcebergPartitionSpec.PartitionField(
                    partitionFieldId(field, position),
                    requiredInt(field, "source-id"),
                    requiredString(field, "name"),
                    requiredString(field, "transform")));
            position++;
        }
    }

    /** The partition field id, defaulting to Iceberg's base (1000) plus position when a v1 spec omits it. */
    private static int partitionFieldId(JsonNode field, int position) {
        JsonNode id = field.get("field-id");
        if (id != null && id.isIntegralNumber()) {
            return id.intValue();
        }
        return PARTITION_FIELD_ID_BASE + position;
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
