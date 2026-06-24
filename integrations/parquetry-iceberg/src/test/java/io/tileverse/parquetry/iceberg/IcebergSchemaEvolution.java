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
 * including {@code current-snapshot-id}, the snapshots array, and the manifest-list location, stays byte-identical,
 * hence the evolved table reads over the original data files. The metadata resolver then picks the highest version,
 * presenting the evolved schema to {@link IcebergCatalog#openLocal}.
 */
final class IcebergSchemaEvolution {

    private static final int EVOLVED_SCHEMA_ID = 1;
    private static final int EVOLVED_METADATA_VERSION = 2;

    private IcebergSchemaEvolution() {}

    /**
     * Writes {@code v2.metadata.json} into {@code tableDir/metadata}, evolving the current schema to {@code evolved}.
     * Returns {@code tableDir} for chaining into {@link IcebergCatalog#openLocal}.
     */
    static Path evolveCurrentSchema(Path tableDir, List<IcebergField> evolved) {
        Path metadataDir = tableDir.resolve("metadata");
        ObjectNode root = readMetadata(metadataDir.resolve("v1.metadata.json"));
        appendEvolvedSchema(root, evolved);
        root.put("current-schema-id", EVOLVED_SCHEMA_ID);
        writeMetadata(metadataDir.resolve("v" + EVOLVED_METADATA_VERSION + ".metadata.json"), root);
        return tableDir;
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
        return node;
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
