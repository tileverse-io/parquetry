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
package io.tileverse.parquetry.schema.geo.geoparquet;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.deser.std.StdDeserializer;

/**
 * Polymorphic deserializer for {@link GeoParquetMetadata}. Dispatches on the {@code "version"} JSON field to one of the
 * concrete record permits. Unknown versions fall through to {@link GeoParquetMetadata.V2} (newest known shape;
 * forward-compatibility direction of the GeoParquet spec is additive).
 */
final class GeoParquetMetadataDeserializer extends StdDeserializer<GeoParquetMetadata> {

    GeoParquetMetadataDeserializer() {
        super(GeoParquetMetadata.class);
    }

    @Override
    public GeoParquetMetadata deserialize(JsonParser p, DeserializationContext ctxt) throws JacksonException {
        ObjectReadContext rc = p.objectReadContext();
        JsonNode root = rc.readTree(p);
        String version = readVersion(root);
        Class<? extends GeoParquetMetadata> target = targetFor(version);
        JsonParser treeParser = rc.treeAsTokens(root);
        treeParser.nextToken();
        return rc.readValue(treeParser, target);
    }

    private static String readVersion(JsonNode root) {
        JsonNode versionNode = root.get("version");
        if (versionNode == null || versionNode.isNull()) {
            return "";
        }
        return versionNode.asString();
    }

    private static Class<? extends GeoParquetMetadata> targetFor(String version) {
        return switch (version) {
            case "1.0.0" -> GeoParquetMetadata.V1_0.class;
            case "1.1.0", "1.1.0+p1" -> GeoParquetMetadata.V1_1.class;
            default -> GeoParquetMetadata.V2.class;
        };
    }
}
