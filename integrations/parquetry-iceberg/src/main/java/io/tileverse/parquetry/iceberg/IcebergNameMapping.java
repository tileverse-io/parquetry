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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Maps a data file's physical column names to Iceberg field ids, for files written without embedded field ids.
 *
 * <p>Iceberg's {@code schema.name-mapping.default} table property is the spec's mapping document for such files
 * (migrated {@code add_files} tables, external writers): a JSON array of entries, each an optional {@code field-id}
 * plus a {@code names} list. A rename accumulates the old name as an alias, hence any name in the list resolves to the
 * entry's field id. When a table has no mapping document, the implicit mapping derived from the current schema's names
 * ({@link #fromSchema}) resolves the common unevolved case.
 *
 * <p>Scope matches reconciliation: top-level entries only. A nested {@code fields} array is tolerated and ignored until
 * nested field-id reconciliation lands. An entry without a {@code field-id} maps its names to nothing; a later entry
 * wins a duplicate name.
 */
final class IcebergNameMapping {

    private final Map<String, Integer> idByName;

    private IcebergNameMapping(Map<String, Integer> idByName) {
        this.idByName = Map.copyOf(idByName);
    }

    /** A mapping that resolves nothing; every id-less column stays unresolved. */
    static IcebergNameMapping empty() {
        return new IcebergNameMapping(Map.of());
    }

    /** Parses the {@code schema.name-mapping.default} property value, a JSON array held in a string. */
    static IcebergNameMapping fromJson(String json) {
        JsonNode root;
        try {
            root = JsonMapper.shared().readTree(json);
        } catch (JacksonException e) {
            throw new IcebergFormatException("malformed schema.name-mapping.default: not valid JSON", e);
        }
        if (!root.isArray()) {
            throw new IcebergFormatException("malformed schema.name-mapping.default: not a JSON array");
        }
        Map<String, Integer> idByName = new HashMap<>();
        for (JsonNode entry : root) {
            addEntry(entry, idByName);
        }
        return new IcebergNameMapping(idByName);
    }

    /** The implicit mapping of a table without the property: each field id keyed by its current name. */
    static IcebergNameMapping fromSchema(List<IcebergField> fields) {
        Map<String, Integer> idByName = new HashMap<>();
        for (IcebergField field : fields) {
            idByName.put(field.name(), field.fieldId());
        }
        return new IcebergNameMapping(idByName);
    }

    /** The field id mapped to a physical column name, empty when no entry names it. */
    OptionalInt idFor(String physicalName) {
        Integer id = idByName.get(physicalName);
        return id == null ? OptionalInt.empty() : OptionalInt.of(id);
    }

    private static void addEntry(JsonNode entry, Map<String, Integer> idByName) {
        if (!entry.isObject()) {
            throw new IcebergFormatException("malformed schema.name-mapping.default: entry is not an object");
        }
        List<String> names = namesOf(entry);
        JsonNode fieldId = entry.get("field-id");
        if (fieldId == null || fieldId.isNull()) {
            return;
        }
        if (!fieldId.isIntegralNumber()) {
            throw new IcebergFormatException("malformed schema.name-mapping.default: field-id is not an integer");
        }
        for (String name : names) {
            idByName.put(name, fieldId.intValue());
        }
    }

    private static List<String> namesOf(JsonNode entry) {
        JsonNode names = entry.get("names");
        if (names == null || !names.isArray()) {
            throw new IcebergFormatException("malformed schema.name-mapping.default: entry has no names array");
        }
        List<String> result = new ArrayList<>();
        for (JsonNode name : names) {
            if (!name.isString()) {
                throw new IcebergFormatException("malformed schema.name-mapping.default: a name is not a string");
            }
            result.add(name.stringValue());
        }
        return result;
    }
}
