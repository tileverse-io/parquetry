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
package io.tileverse.parquetry.schema.geo.geoparquet;

import java.util.OptionalDouble;

import io.tileverse.parquetry.format.BoundingBox;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ValueDeserializer;

/**
 * Reads a GeoParquet {@code "bbox"} JSON array into the typed {@link BoundingBox} record.
 *
 * <p>GeoParquet writes bboxes in corner-pair order: {@code [xmin, ymin, xmax, ymax]} for XY-only, or {@code [xmin,
 * ymin, zmin, xmax, ymax, zmax]} when the column carries Z. This deserializer permutes the wire order into
 * {@link BoundingBox}'s flat-double order ({@code xmin, xmax, ymin, ymax, ...}) so consumers see one bbox shape
 * regardless of source.
 *
 * <p>Arrays of any other length surface as {@code null} (which {@code GeoColumn}'s canonical constructor maps to
 * {@link java.util.Optional#empty()}) - malformed bbox data is dropped silently rather than failing the whole
 * {@code "geo"} JSON parse.
 */
final class BboxDeserializer extends ValueDeserializer<BoundingBox> {

    @Override
    public BoundingBox deserialize(JsonParser parser, DeserializationContext context) throws JacksonException {
        JsonNode node = parser.readValueAsTree();
        if (node == null || !node.isArray()) {
            return null;
        }
        return switch (node.size()) {
            case 4 ->
                new BoundingBox(
                        node.get(0).asDouble(),
                        node.get(2).asDouble(),
                        node.get(1).asDouble(),
                        node.get(3).asDouble(),
                        OptionalDouble.empty(),
                        OptionalDouble.empty(),
                        OptionalDouble.empty(),
                        OptionalDouble.empty());
            case 6 ->
                new BoundingBox(
                        node.get(0).asDouble(),
                        node.get(3).asDouble(),
                        node.get(1).asDouble(),
                        node.get(4).asDouble(),
                        OptionalDouble.of(node.get(2).asDouble()),
                        OptionalDouble.of(node.get(5).asDouble()),
                        OptionalDouble.empty(),
                        OptionalDouble.empty());
            default -> null;
        };
    }
}
