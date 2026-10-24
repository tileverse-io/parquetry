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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import tools.jackson.databind.JsonNode;

/**
 * The typed PROJJSON coordinate reference system ADT. One of:
 *
 * <ul>
 *   <li>{@link GeographicCRS} - angular coordinates on a geodetic frame, the most common GeoParquet case. The axis
 *       order is the one stated by the coordinate system: OGC:CRS84 is lon/lat, EPSG:4326 is lat/lon.
 *   <li>{@link GeodeticCRS} - 3D geocentric (X/Y/Z) on a geodetic datum.
 *   <li>{@link ProjectedCRS} - planar (easting/northing) derived from a base CRS via a {@link Conversion}.
 *   <li>{@link CompoundCRS} - horizontal + vertical (or other) components combined.
 *   <li>{@link BoundCRS} - a CRS paired with a transformation to a target CRS.
 *   <li>{@link VerticalCRS} - elevation / depth on a vertical datum.
 *   <li>{@link Transformation} - a coordinate operation rather than a CRS. The PROJJSON schema allows one as a
 *       top-level document, which is why it is modeled here alongside the CRS types.
 *   <li>{@link Unknown} - forward-compat fallback when the PROJJSON {@code type} discriminator is one this package does
 *       not yet model.
 * </ul>
 *
 * <p>The common accessors {@link #name()} and {@link #id()} are declared on this interface. Use {@link #id()} together
 * with {@link Identifier#epsgCode()} to obtain the EPSG SRID where applicable.
 *
 * <p>A CRS parsed from a document keeps that document in {@link #raw()}, and writing it back emits the kept tree
 * unchanged. The records model the properties read by parquetry; PROJJSON has many more, and several of them are
 * required. OGC:CRS84, EPSG:4326 and the CRSs derived from them define their frame with a {@code datum_ensemble}, not
 * modeled by any record here, and PROJ rejects a document that omits it. Writing the kept tree keeps such a document
 * valid, and keeps the unmodeled properties, among them {@code scope}, {@code area}, {@code bbox} and {@code remarks}.
 * A CRS built in code has no document and is written from its fields.
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
     * The source document of this CRS, empty for a CRS built in code. The serializer writes it unchanged, which keeps
     * the properties not modeled by the records.
     */
    Optional<JsonNode> raw();

    /**
     * Returns a copy of this CRS that remembers its source document. Called by the deserializer; a caller building a
     * CRS in code has no document to attach.
     */
    CoordinateReferenceSystem withRaw(JsonNode document);

    /**
     * Forward-compat fallback for a PROJJSON {@code type} discriminator not yet modeled by this package. The document
     * is preserved for callers that recognize the type and want to inspect it.
     */
    record Unknown(String rawType, JsonNode document) implements CoordinateReferenceSystem {

        @Override
        public Optional<String> name() {
            return Optional.empty();
        }

        @Override
        public Optional<Identifier> id() {
            return Optional.empty();
        }

        @Override
        public Optional<JsonNode> raw() {
            return Optional.ofNullable(document);
        }

        @Override
        public Unknown withRaw(JsonNode replacement) {
            return new Unknown(rawType, replacement);
        }
    }
}
