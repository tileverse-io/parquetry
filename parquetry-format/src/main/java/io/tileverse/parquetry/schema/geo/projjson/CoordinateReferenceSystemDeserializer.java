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
package io.tileverse.parquetry.schema.geo.projjson;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.deser.std.StdDeserializer;

/**
 * Polymorphic deserializer for {@link CoordinateReferenceSystem}. Dispatches on the {@code "type"} JSON discriminator
 * to the corresponding record deserializer. Both an unmodeled discriminator and a missing {@code "type"} fall through
 * to {@link CoordinateReferenceSystem.Unknown} with the raw JSON node preserved; the two are distinguished by
 * {@link CoordinateReferenceSystem.Unknown#rawType()} (the modeled discriminator vs the empty string for a missing
 * one). A missing type is valid in a nested position - a PROJJSON {@code base_crs} may omit it - so it is not rejected
 * here; a top-level CRS that omits it is caught where the top-level context is known (see
 * {@code GeoParquetMetadata.parse}).
 */
final class CoordinateReferenceSystemDeserializer extends StdDeserializer<CoordinateReferenceSystem> {

    CoordinateReferenceSystemDeserializer() {
        super(CoordinateReferenceSystem.class);
    }

    @Override
    public CoordinateReferenceSystem deserialize(JsonParser p, DeserializationContext ctxt) throws JacksonException {
        ObjectReadContext rc = p.objectReadContext();
        JsonNode root = rc.readTree(p);
        JsonNode typeNode = root.get("type");
        if (typeNode == null || typeNode.isNull()) {
            return new CoordinateReferenceSystem.Unknown("", root);
        }
        String type = typeNode.asString();
        Class<? extends CoordinateReferenceSystem> target = targetFor(type);
        if (target == null) {
            return new CoordinateReferenceSystem.Unknown(type, root);
        }
        JsonParser treeParser = rc.treeAsTokens(root);
        treeParser.nextToken();
        return rc.readValue(treeParser, target);
    }

    private static Class<? extends CoordinateReferenceSystem> targetFor(String type) {
        return switch (type) {
            case "GeographicCRS" -> GeographicCRS.class;
            case "GeodeticCRS" -> GeodeticCRS.class;
            case "ProjectedCRS" -> ProjectedCRS.class;
            case "CompoundCRS" -> CompoundCRS.class;
            case "BoundCRS" -> BoundCRS.class;
            case "VerticalCRS" -> VerticalCRS.class;
            case "Transformation" -> Transformation.class;
            default -> null;
        };
    }
}
