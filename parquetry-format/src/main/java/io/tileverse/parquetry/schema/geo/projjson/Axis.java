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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import tools.jackson.databind.JsonNode;

/**
 * One axis of a {@link CoordinateSystem}: name, abbreviation, direction (e.g. {@code "north"}, {@code "east"}), and
 * unit. Order in the parent {@link CoordinateSystem#axes()} list is significant - it defines the order of coordinates
 * in stored geometries.
 *
 * <p>The PROJJSON schema permits {@code unit} to be either a plain string (e.g. {@code "degree"}, {@code "metre"}) or a
 * structured unit object. Both shapes are preserved as raw {@link JsonNode}; {@link #unitName()} returns the simple
 * string form when one is available.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Axis(
        Optional<String> name, Optional<String> abbreviation, Optional<String> direction, Optional<JsonNode> unit) {

    /**
     * Returns the unit as a string when {@link #unit()} is a JSON string, or when it is a JSON object with a
     * {@code "name"} property. Otherwise empty.
     */
    public Optional<String> unitName() {
        return unit.flatMap(node -> {
            if (node.isString()) {
                return Optional.of(node.asString());
            }
            JsonNode nameNode = node.get("name");
            if (nameNode != null && nameNode.isString()) {
                return Optional.of(nameNode.asString());
            }
            return Optional.empty();
        });
    }
}
