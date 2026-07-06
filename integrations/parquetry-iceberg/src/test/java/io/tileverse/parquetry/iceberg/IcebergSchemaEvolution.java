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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

import io.tileverse.parquetry.filter.Value;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * Writes a new metadata version that evolves a corpus Iceberg table's current schema in place, over the same snapshot,
 * manifests, and data files.
 *
 * <p>An evolved table is a legitimate Iceberg evolution: the data files keep their original footer field ids, while the
 * table's current schema renames, adds, drops, reorders, or promotes columns by field id. This helper reads the
 * original {@code v1.metadata.json}, appends an evolved schema with a fresh schema id, repoints
 * {@code current-schema-id} at it, and writes the result as {@code v<version>.metadata.json}. Everything else,
 * including {@code current-snapshot-id}, the snapshots array, and the manifest-list location, stays byte-identical
 * (apart from the optional {@code schema.name-mapping.default} property rewrite the 3-arg overload performs), hence the
 * evolved table reads over the original data files. The metadata resolver then picks the highest version, presenting
 * the evolved schema to {@link IcebergTableCatalog#openLocal}.
 */
final class IcebergSchemaEvolution {

    private static final int EVOLVED_SCHEMA_ID = 1;
    private static final int EVOLVED_METADATA_VERSION = 2;

    private IcebergSchemaEvolution() {}

    /**
     * Writes {@code v2.metadata.json} into {@code tableDir/metadata}, evolving the current schema to {@code evolved}.
     * Returns {@code tableDir} for chaining into {@link IcebergTableCatalog#openLocal}.
     */
    static Path evolveCurrentSchema(Path tableDir, List<IcebergField> evolved) {
        return evolve(tableDir, evolved, root -> {});
    }

    /**
     * Evolves the current schema and rewrites the {@code schema.name-mapping.default} property in the same metadata
     * version, mirroring how Iceberg maintains the mapping on schema commits. A null {@code nameMappingJson} removes
     * the property, exercising the implicit schema-derived mapping.
     */
    static Path evolveCurrentSchema(Path tableDir, List<IcebergField> evolved, String nameMappingJson) {
        return evolve(tableDir, evolved, root -> putNameMapping(root, nameMappingJson));
    }

    private static Path evolve(Path tableDir, List<IcebergField> evolved, Consumer<ObjectNode> metadataEdit) {
        Path metadataDir = tableDir.resolve("metadata");
        ObjectNode root = readMetadata(metadataDir.resolve("v1.metadata.json"));
        appendEvolvedSchema(root, evolved);
        root.put("current-schema-id", EVOLVED_SCHEMA_ID);
        metadataEdit.accept(root);
        writeMetadata(metadataDir.resolve("v" + EVOLVED_METADATA_VERSION + ".metadata.json"), root);
        return tableDir;
    }

    private static void putNameMapping(ObjectNode root, String nameMappingJson) {
        JsonNode existing = root.get("properties");
        ObjectNode properties = existing instanceof ObjectNode node ? node : root.putObject("properties");
        if (nameMappingJson == null) {
            properties.remove("schema.name-mapping.default");
        } else {
            properties.put("schema.name-mapping.default", nameMappingJson);
        }
    }

    private static void appendEvolvedSchema(ObjectNode root, List<IcebergField> evolved) {
        ArrayNode schemas = (ArrayNode) root.get("schemas");
        ObjectNode evolvedSchema = JsonMapper.shared().createObjectNode();
        evolvedSchema.put("schema-id", EVOLVED_SCHEMA_ID);
        evolvedSchema.put("type", "struct");
        evolvedSchema.set("fields", fieldsNode(evolved));
        schemas.add(evolvedSchema);
    }

    private static ArrayNode fieldsNode(List<IcebergField> evolved) {
        ArrayNode fields = JsonMapper.shared().createArrayNode();
        for (IcebergField field : evolved) {
            fields.add(fieldNode(field));
        }
        return fields;
    }

    private static ObjectNode fieldNode(IcebergField field) {
        ObjectNode node = JsonMapper.shared().createObjectNode();
        node.put("id", field.fieldId());
        node.put("name", field.name());
        node.put("required", field.required());
        node.put("type", field.type());
        field.initialDefault().ifPresent(value -> putInitialDefault(node, value));
        return node;
    }

    /** Serializes a default value back to Iceberg's JSON single-value form (a date renders as an ISO string). */
    private static void putInitialDefault(ObjectNode node, Value value) {
        switch (value) {
            case Value.IntVal v -> node.put("initial-default", v.value());
            case Value.LongVal v -> node.put("initial-default", v.value());
            case Value.FloatVal v -> node.put("initial-default", v.value());
            case Value.DoubleVal v -> node.put("initial-default", v.value());
            case Value.BoolVal v -> node.put("initial-default", v.value());
            case Value.DateVal v -> node.put("initial-default", v.value().toString());
            case Value.StringVal v -> node.put("initial-default", v.value());
            default -> throw new IllegalArgumentException("unsupported default value: " + value);
        }
    }

    private static ObjectNode readMetadata(Path metadataJson) {
        try {
            String json = Files.readString(metadataJson, StandardCharsets.UTF_8);
            JsonNode root = JsonMapper.shared().readTree(json);
            return (ObjectNode) root;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read " + metadataJson, e);
        }
    }

    private static void writeMetadata(Path destination, ObjectNode root) {
        try {
            String json = JsonMapper.shared().writeValueAsString(root);
            Files.writeString(destination, json, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot write " + destination, e);
        }
    }
}
