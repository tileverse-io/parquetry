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
 * The typed PROJJSON coordinate reference system ADT. One of:
 *
 * <ul>
 *   <li>{@link GeographicCRS} - lat/lon on a geodetic datum (the most common GeoParquet case, OGC:CRS84).
 *   <li>{@link GeodeticCRS} - 3D geocentric (X/Y/Z) on a geodetic datum.
 *   <li>{@link ProjectedCRS} - planar (easting/northing) derived from a base CRS via a {@link Conversion}.
 *   <li>{@link CompoundCRS} - horizontal + vertical (or other) components combined.
 *   <li>{@link BoundCRS} - a CRS paired with a transformation to a target CRS.
 *   <li>{@link VerticalCRS} - elevation / depth on a vertical datum.
 *   <li>{@link Transformation} - a top-level coordinate-operation document.
 *   <li>{@link Unknown} - forward-compat fallback when the PROJJSON {@code type} discriminator is one this package does
 *       not yet model.
 * </ul>
 *
 * <p>The common accessors {@link #name()} and {@link #id()} are declared on this interface. Use {@link #id()} together
 * with {@link Identifier#epsgCode()} to obtain the EPSG SRID where applicable.
 *
 * <p>Polymorphic deserialization is driven by the JSON {@code type} discriminator via
 * {@link CoordinateReferenceSystemDeserializer}, wired through {@link ProjJsonModule}. The deserializer is wired at the
 * module level rather than via {@code @JsonDeserialize} on this interface: the permitted records inherit interface
 * annotations, so an interface-level {@code @JsonDeserialize} would cause the polymorphic deserializer to recurse into
 * itself for every concrete subtype lookup. Module-level wiring binds the deserializer only at
 * {@code CoordinateReferenceSystem.class}, leaving the concrete records free to use Jackson's default record
 * deserializer.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public sealed interface CoordinateReferenceSystem
        permits GeographicCRS,
                GeodeticCRS,
                ProjectedCRS,
                CompoundCRS,
                BoundCRS,
                VerticalCRS,
                Transformation,
                CoordinateReferenceSystem.Unknown {

    Optional<String> name();

    Optional<Identifier> id();

    /**
     * Forward-compat fallback for PROJJSON {@code type} discriminators this package does not yet model. The raw JSON
     * tree is preserved so callers can inspect it if they recognize the type.
     */
    record Unknown(String rawType, JsonNode raw) implements CoordinateReferenceSystem {

        @Override
        public Optional<String> name() {
            return Optional.empty();
        }

        @Override
        public Optional<Identifier> id() {
            return Optional.empty();
        }
    }
}
