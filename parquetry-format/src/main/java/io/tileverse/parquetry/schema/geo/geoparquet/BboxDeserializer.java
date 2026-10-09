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
 * <p>A bbox of another shape reads as {@code null}, mapped to an empty {@link java.util.Optional} by the canonical
 * constructor of {@code GeoColumn}: an array of another length, a non-numeric bound, and a box
 * {@linkplain BoundingBox#provesNothing() proving nothing}, such as the NaN bounds written for a column without
 * coordinates. Malformed bbox data is dropped rather than failing the whole {@code "geo"} JSON parse.
 */
final class BboxDeserializer extends ValueDeserializer<BoundingBox> {

    @Override
    public BoundingBox deserialize(JsonParser parser, DeserializationContext context) throws JacksonException {
        JsonNode node = parser.readValueAsTree();
        if (node == null || !isArrayOfNumbers(node)) {
            return null;
        }
        BoundingBox box = cornerPairs(node);
        if (box == null || box.provesNothing()) {
            return null;
        }
        return box;
    }

    /** A bound written as text, as {@code null} or as a nested value is no number, and makes no box. */
    private static boolean isArrayOfNumbers(JsonNode node) {
        if (!node.isArray()) {
            return false;
        }
        for (JsonNode bound : node) {
            if (!bound.isNumber()) {
                return false;
            }
        }
        return true;
    }

    /** The box of a corner-pair array of 4 or 6 numbers, {@code null} for another length. */
    private static BoundingBox cornerPairs(JsonNode bounds) {
        return switch (bounds.size()) {
            case 4 ->
                new BoundingBox(
                        bounds.get(0).asDouble(),
                        bounds.get(2).asDouble(),
                        bounds.get(1).asDouble(),
                        bounds.get(3).asDouble(),
                        OptionalDouble.empty(),
                        OptionalDouble.empty(),
                        OptionalDouble.empty(),
                        OptionalDouble.empty());
            case 6 ->
                new BoundingBox(
                        bounds.get(0).asDouble(),
                        bounds.get(3).asDouble(),
                        bounds.get(1).asDouble(),
                        bounds.get(4).asDouble(),
                        OptionalDouble.of(bounds.get(2).asDouble()),
                        OptionalDouble.of(bounds.get(5).asDouble()),
                        OptionalDouble.empty(),
                        OptionalDouble.empty());
            default -> null;
        };
    }
}
