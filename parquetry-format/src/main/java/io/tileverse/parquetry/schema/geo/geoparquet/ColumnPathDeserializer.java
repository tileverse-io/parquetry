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
package io.tileverse.parquetry.schema.geo.geoparquet;

import java.util.ArrayList;
import java.util.List;

import io.tileverse.parquetry.schema.ColumnPath;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.deser.std.StdDeserializer;

/**
 * Jackson deserializer that reads a GeoParquet {@code covering.bbox} path - a JSON array of strings - into a typed
 * {@link ColumnPath}. Registered by {@link GeoParquetModule}; not auto-discovered via SPI.
 */
final class ColumnPathDeserializer extends StdDeserializer<ColumnPath> {

    ColumnPathDeserializer() {
        super(ColumnPath.class);
    }

    @Override
    public ColumnPath deserialize(JsonParser p, DeserializationContext ctxt) throws JacksonException {
        JsonNode root = p.objectReadContext().readTree(p);
        List<String> parts = new ArrayList<>(root.size());
        for (JsonNode node : root) {
            parts.add(node.asString());
        }
        return new ColumnPath(parts);
    }
}
