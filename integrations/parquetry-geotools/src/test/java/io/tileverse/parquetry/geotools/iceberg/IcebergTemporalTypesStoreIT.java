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
package io.tileverse.parquetry.geotools.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import org.geotools.api.data.DataStore;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.testkit.TestCorpus;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * Proves the GeoTools store presents temporal columns as java.time bindings. The v3_minimal testbed table is evolved in
 * place to add a date, a zone-less timestamp, and a UTC-adjusted timestamp column; the store's feature type must bind
 * them to LocalDate, LocalDateTime, and Instant. getSchema is metadata-only, and no value fixture is needed.
 */
class IcebergTemporalTypesStoreIT {

    @TempDir
    Path tempDir;

    @Test
    void temporalColumnsBindToJavaTimeThroughTheStore() throws IOException {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve("corpus"));
        Path table = root.resolve("v3_minimal");
        addColumnsToCurrentSchema(
                table,
                List.of(
                        columnField(50, "d", "date"),
                        columnField(51, "ts", "timestamp"),
                        columnField(52, "tstz", "timestamptz")));

        DataStore store = new IcebergDataStoreFactory()
                .createDataStore(Map.of("iceberg", table.toUri().toString()));
        try {
            SimpleFeatureType schema = store.getSchema("v3_minimal");
            assertThat(schema.getDescriptor("d").getType().getBinding()).isEqualTo(LocalDate.class);
            assertThat(schema.getDescriptor("ts").getType().getBinding()).isEqualTo(LocalDateTime.class);
            assertThat(schema.getDescriptor("tstz").getType().getBinding()).isEqualTo(Instant.class);
        } finally {
            store.dispose();
        }
    }

    private static void addColumnsToCurrentSchema(Path table, List<ObjectNode> newFields) throws IOException {
        Path metadataJson = table.resolve("metadata/v1.metadata.json");
        ObjectNode root = (ObjectNode) JsonMapper.shared().readTree(Files.readString(metadataJson));
        int currentSchemaId = root.get("current-schema-id").intValue();
        boolean appended = false;
        for (JsonNode schemaNode : root.get("schemas")) {
            ObjectNode schema = (ObjectNode) schemaNode;
            if (schema.get("schema-id").intValue() == currentSchemaId) {
                ArrayNode fields = (ArrayNode) schema.get("fields");
                newFields.forEach(fields::add);
                appended = true;
            }
        }
        if (!appended) {
            throw new IllegalStateException("no current schema found to extend in " + metadataJson);
        }
        Files.writeString(metadataJson, JsonMapper.shared().writeValueAsString(root));
    }

    private static ObjectNode columnField(int id, String name, String type) {
        ObjectNode field = JsonMapper.shared().createObjectNode();
        field.put("id", id);
        field.put("name", name);
        field.put("required", false);
        field.put("type", type);
        return field;
    }
}
