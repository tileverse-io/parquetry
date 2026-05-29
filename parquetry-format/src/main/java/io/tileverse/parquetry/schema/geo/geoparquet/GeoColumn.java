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

import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;

import lombok.Builder;
import tools.jackson.databind.annotation.JsonDeserialize;

/**
 * Per-column geometry metadata inside the GeoParquet {@code "geo"} key. Fields are independently optional across spec
 * versions:
 *
 * <ul>
 *   <li>1.0: {@code encoding}, {@code geometry_types}, and {@code crs} are present; {@code covering} is unsupported.
 *   <li>1.1+: adds {@code covering} and {@code orientation}; {@code crs} may be inline PROJJSON or {@code null}
 *       (meaning OGC:CRS84).
 *   <li>2.0: native GEOMETRY / GEOGRAPHY logical types carry the CRS and algorithm; the {@code "geo"} JSON still
 *       carries {@code geometry_types}, {@code bbox}, {@code covering} per column.
 * </ul>
 *
 * @param encoding wire encoding of the geometry payload: {@code "WKB"} (default; all versions), {@code "point"} /
 *     {@code "linestring"} / ... (2.0-dev native encoding kinds, deprecated path)
 * @param geometryTypes WKT-style type names this column carries (e.g. {@code ["Polygon", "MultiPolygon"]}); empty list
 *     when not declared
 * @param crs typed PROJJSON coordinate reference system; {@link Optional#empty()} means "use the spec default"
 *     (OGC:CRS84 for GeoParquet 1.x, the native annotation's default for 2.0)
 * @param edges {@code "planar"} or {@code "spherical"} (1.x); {@link Optional#empty()} for planar default
 * @param orientation winding order, e.g. {@code "counterclockwise"} (1.1+)
 * @param bbox file-level bounding box, parsed from the GeoParquet corner-pair JSON array (4 or 6 doubles) into the
 *     typed {@link BoundingBox} record; {@link Optional#empty()} when absent or malformed
 * @param epoch coordinate epoch as a decimal year (1.1+); used by time-varying CRSes
 * @param covering 1.1+ covering metadata pointing at sidecar bbox columns for spatial pruning
 */
// S2789: constructor null-tolerates Optional to support the @Builder pattern.
@SuppressWarnings("java:S2789")
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public record GeoColumn(
        Optional<String> encoding,
        @JsonProperty("geometry_types") List<String> geometryTypes,
        Optional<CoordinateReferenceSystem> crs,
        Optional<String> edges,
        Optional<String> orientation,

        @JsonDeserialize(contentUsing = BboxDeserializer.class)
        Optional<BoundingBox> bbox,

        OptionalDouble epoch,
        Optional<Covering> covering) {

    public GeoColumn {
        encoding = encoding == null ? Optional.empty() : encoding;
        geometryTypes = geometryTypes == null ? List.of() : List.copyOf(geometryTypes);
        crs = crs == null ? Optional.empty() : crs;
        edges = edges == null ? Optional.empty() : edges;
        orientation = orientation == null ? Optional.empty() : orientation;
        bbox = bbox == null ? Optional.empty() : bbox;
        epoch = epoch == null ? OptionalDouble.empty() : epoch;
        covering = covering == null ? Optional.empty() : covering;
    }
}
