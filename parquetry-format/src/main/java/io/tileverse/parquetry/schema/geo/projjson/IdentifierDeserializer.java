/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.schema.geo.projjson;

import java.util.Optional;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.deser.std.StdDeserializer;

/**
 * Jackson deserializer for {@link Identifier}. Handles the PROJJSON quirk where {@code code} may be a JSON integer or a
 * JSON string by coercing integers to their decimal string form. All other fields are plain strings.
 */
final class IdentifierDeserializer extends StdDeserializer<Identifier> {

    IdentifierDeserializer() {
        super(Identifier.class);
    }

    @Override
    public Identifier deserialize(JsonParser p, DeserializationContext ctxt) throws JacksonException {
        JsonNode root = p.objectReadContext().readTree(p);
        String authority = textOrNull(root, "authority");
        String code = codeAsString(root.get("code"));
        Optional<String> version = optionalText(root, "version");
        Optional<String> uri = optionalText(root, "uri");
        return new Identifier(authority, code, version, uri);
    }

    private static String codeAsString(JsonNode codeNode) {
        if (codeNode == null || codeNode.isNull()) {
            return null;
        }
        if (codeNode.isNumber()) {
            return codeNode.asString();
        }
        return codeNode.asString();
    }

    private static String textOrNull(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        return node.asString();
    }

    private static Optional<String> optionalText(JsonNode root, String field) {
        return Optional.ofNullable(textOrNull(root, field));
    }
}
