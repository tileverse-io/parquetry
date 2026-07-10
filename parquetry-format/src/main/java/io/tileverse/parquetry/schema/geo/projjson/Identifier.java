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
package io.tileverse.parquetry.schema.geo.projjson;

import java.util.Optional;
import java.util.OptionalInt;

import tools.jackson.databind.annotation.JsonDeserialize;

/**
 * A PROJJSON identifier: authority + code that names a CRS, datum, ellipsoid, or operation in some external registry.
 *
 * <p>The most common case is an EPSG identifier, e.g. {@code {"authority": "EPSG", "code": 4326}}. The PROJJSON schema
 * permits {@code code} to be either an integer or a string; this record normalizes to {@link String} (the deserializer
 * coerces integers to their decimal representation) and exposes {@link #epsgCode()} for the common case.
 *
 * @param authority registry name (e.g. {@code "EPSG"}, {@code "OGC"}, {@code "PROJ"})
 * @param code identifier within the authority's namespace; stored as a string regardless of the wire form
 * @param version optional registry version
 * @param uri optional canonical URI
 */
@JsonDeserialize(using = IdentifierDeserializer.class)
public record Identifier(String authority, String code, Optional<String> version, Optional<String> uri) {

    /**
     * Returns this identifier's EPSG code as an {@link OptionalInt} when {@link #authority()} is {@code "EPSG"} and
     * {@link #code()} parses as a non-negative {@code int}; otherwise empty. JTS and downstream geo tooling use the
     * resulting integer as the SRID.
     */
    public OptionalInt epsgCode() {
        if (!"EPSG".equals(authority)) {
            return OptionalInt.empty();
        }
        try {
            int parsed = Integer.parseInt(code);
            if (parsed < 0) {
                return OptionalInt.empty();
            }
            return OptionalInt.of(parsed);
        } catch (NumberFormatException _) {
            return OptionalInt.empty();
        }
    }
}
